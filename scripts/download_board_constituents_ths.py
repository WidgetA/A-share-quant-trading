#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
通过同花顺网页抓取概念板块成分股，保存到 data/board_constituents.json。

使用 Playwright 控制浏览器，注入 Cookie 复用同花顺登录态，
逐板块读取 https://q.10jqka.com.cn/thshy/detail/code/{板块代码} 的成分股表格。

比 iwencai NLP 查询精确得多——直接读板块详情页，无 NLP 歧义。

用法：
    uv run python scripts/download_board_constituents_ths.py
    uv run python scripts/download_board_constituents_ths.py --test AI应用
    uv run python scripts/download_board_constituents_ths.py --dry-run
    uv run python scripts/download_board_constituents_ths.py --proxy-key KEY --proxy-secret SECRET
"""

import argparse
import io
import json
import logging
import os
import random
import sys
import time
from pathlib import Path

import requests

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

PROJECT_ROOT = Path(__file__).parent.parent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

SECTORS_PATH = PROJECT_ROOT / "data" / "sectors.json"
OUTPUT_PATH = PROJECT_ROOT / "data" / "board_constituents.json"
# Progress file: set of board names successfully scraped (non-suspect).
# Separate from data file so we know what's THS-verified vs old iwencai junk.
PROGRESS_PATH = PROJECT_ROOT / "data" / ".ths_scrape_done.json"

PROXY_API_URL = "https://api.xiaoxiangdaili.com/ip/get"
# Rotate proxy every N boards
_PROXY_ROTATE_EVERY = 2


def _load_boards() -> list[dict]:
    """Load concept boards from sectors.json, return [{name, code}, ...]."""
    with open(SECTORS_PATH, encoding="utf-8") as f:
        sectors = json.load(f)

    boards = []
    for item in sectors.get("concept", []):
        code = item["code"].replace(".TI", "")
        boards.append({"name": item["name"], "code": code})

    logger.info(f"Loaded {len(boards)} concept boards from sectors.json")
    return boards


def _get_cookie_string() -> str:
    """Get THS cookie from env var or config/secrets.yaml."""
    cookie = os.environ.get("THS_COOKIE", "")
    if cookie:
        return cookie

    secrets_path = PROJECT_ROOT / "config" / "secrets.yaml"
    if secrets_path.exists():
        try:
            import yaml

            with open(secrets_path, encoding="utf-8") as f:
                secrets = yaml.safe_load(f)
            cookie = secrets.get("ths", {}).get("cookie", "")
            if cookie:
                return cookie
        except Exception:
            pass

    return ""


def _parse_cookie_string(cookie_str: str) -> list[dict]:
    """Parse cookie string into Playwright cookie dicts for 10jqka.com.cn."""
    cookies = []
    for part in cookie_str.split(";"):
        part = part.strip()
        if "=" not in part:
            continue
        name, value = part.split("=", 1)
        cookies.append(
            {
                "name": name.strip(),
                "value": value.strip(),
                "domain": ".10jqka.com.cn",
                "path": "/",
            }
        )
    return cookies


def _fetch_proxy_ip(app_key: str, app_secret: str) -> str | None:
    """Fetch a fresh proxy IP from xiaoxiangdaili API.

    Returns proxy URL like 'http://key:secret@ip:port' or None on failure.
    """
    try:
        resp = requests.get(
            PROXY_API_URL,
            params={"appKey": app_key, "appSecret": app_secret, "wt": "text", "cnt": 1},
            timeout=10,
        )
        ip_port = resp.text.strip()
        if not ip_port or ":" not in ip_port:
            logger.warning(f"Proxy API returned invalid response: {ip_port}")
            return None
        proxy_url = f"http://{app_key}:{app_secret}@{ip_port}"
        logger.info(f"New proxy IP: {ip_port}")
        return proxy_url
    except Exception as e:
        logger.warning(f"Failed to fetch proxy: {e}")
        return None


def _extract_page_stocks(page) -> list[list[str]]:
    """Extract stock codes and names from the currently loaded table page.

    Uses Playwright DOM API — the data table is the second table.m-table
    on the page (first is the empty template table).
    """
    stocks: list[list[str]] = []
    tables = page.locator("table.m-table")
    if tables.count() < 2:
        return stocks

    rows = tables.nth(1).locator("tbody tr")
    count = rows.count()
    for i in range(count):
        cells = rows.nth(i).locator("td")
        if cells.count() < 3:
            continue
        code = cells.nth(1).inner_text().strip()
        name = cells.nth(2).inner_text().strip()
        if len(code) == 6 and code.isdigit():
            stocks.append([code, name])

    return stocks


def _get_total_pages(page) -> int:
    """Extract total page count from pagination element."""
    info = page.locator(".page_info")
    if info.count() > 0:
        text = info.first.inner_text().strip()
        parts = text.split("/")
        if len(parts) == 2 and parts[1].isdigit():
            return int(parts[1])
    return 1


def _scrape_board(page, board_code: str, board_name: str) -> list[list[str]] | None:
    """Scrape all pages of a single board. Returns [[code, name], ...] or None."""
    url = f"https://q.10jqka.com.cn/thshy/detail/code/{board_code}"
    page.goto(url, wait_until="domcontentloaded", timeout=15000)

    # Wait for data rows to load via AJAX
    try:
        page.wait_for_selector("table.m-table tbody tr", timeout=15000)
    except Exception:
        # Might need more time or hit JS challenge
        page.wait_for_timeout(5000)
        try:
            page.wait_for_selector("table.m-table tbody tr", timeout=10000)
        except Exception:
            logger.warning(f"{board_name}: data rows not loaded, URL={page.url}")
            return None

    page.wait_for_timeout(500)

    all_stocks: list[list[str]] = []
    total_pages = _get_total_pages(page)

    # Page 1
    stocks = _extract_page_stocks(page)
    all_stocks.extend(stocks)

    # Remaining pages: click "下一页" button repeatedly
    for pg in range(2, total_pages + 1):
        try:
            # Click "下一页" link
            next_btn = page.locator("a.changePage", has_text="下一页")
            if next_btn.count() == 0:
                logger.debug(f"{board_name}: no '下一页' button at page {pg}")
                break
            next_btn.first.click()

            # Wait for AJAX to load new page data
            expected_info = f"{pg}/{total_pages}"
            for attempt in range(30):
                page.wait_for_timeout(200)
                info_el = page.locator(".page_info")
                if info_el.count() > 0:
                    cur = info_el.first.inner_text().strip()
                    if cur == expected_info:
                        break

            # Check actual page info
            cur_info = ""
            info_el = page.locator(".page_info")
            if info_el.count() > 0:
                cur_info = info_el.first.inner_text().strip()

            stocks = _extract_page_stocks(page)
            logger.debug(
                f"{board_name} pg {pg}: page_info={cur_info}, "
                f"tables={page.locator('table.m-table').count()}, "
                f"rows={stocks and len(stocks)}"
            )
            if not stocks:
                logger.warning(f"{board_name}: empty page {pg} (info={cur_info})")
                break
            all_stocks.extend(stocks)

        except Exception as e:
            logger.warning(f"{board_name} page {pg}/{total_pages}: {e}")
            break

    # Deduplicate
    seen = set()
    unique: list[list[str]] = []
    for code, name in all_stocks:
        if code not in seen:
            seen.add(code)
            unique.append([code, name])

    return unique


def _create_context(p, proxy_url: str | None, saved_cookies: list[dict] | None):
    """Create a new browser + context (with optional proxy), restore cookies."""
    launch_args: dict = {
        "headless": False,
        "args": ["--disable-blink-features=AutomationControlled"],
    }
    if proxy_url:
        launch_args["proxy"] = {"server": proxy_url}
    browser = p.chromium.launch(**launch_args)
    context = browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/145.0.0.0 Safari/537.36"
        ),
    )
    if saved_cookies:
        context.add_cookies(saved_cookies)
    page = context.new_page()
    return browser, context, page


def scrape_all(
    dry_run: bool = False,
    test_board: str | None = None,
    headless: bool = False,
    proxy: str | None = None,
    proxy_key: str | None = None,
    proxy_secret: str | None = None,
) -> None:
    from playwright.sync_api import sync_playwright

    boards = _load_boards()
    if not boards:
        logger.error("No concept boards found in sectors.json")
        return

    if dry_run:
        for i, b in enumerate(boards, 1):
            print(f"  {i:3d}. {b['code']}  {b['name']}")
        print(f"\nTotal: {len(boards)} boards (dry run)")
        return

    # Get cookie (optional if Chrome profile is available)
    cookie_str = _get_cookie_string()

    use_rotating_proxy = bool(proxy_key and proxy_secret)

    # Filter to single board for testing (uses separate output file)
    if test_board:
        boards = [b for b in boards if b["name"] == test_board]
        if not boards:
            logger.error(f"Board '{test_board}' not found in sectors.json")
            return
        logger.info(
            f"Test mode: only scraping '{test_board}' (print only, won't touch {OUTPUT_PATH.name})"
        )

    # Load existing data for incremental updates (skip already-scraped boards)
    existing: dict[str, list[list[str]]] = {}
    if OUTPUT_PATH.exists() and not test_board:
        with open(OUTPUT_PATH, encoding="utf-8") as f:
            existing = json.load(f)
        logger.info(f"Loaded {len(existing)} existing boards for incremental update")

    with sync_playwright() as p:
        # Initial proxy
        initial_proxy = proxy
        if use_rotating_proxy:
            initial_proxy = _fetch_proxy_ip(proxy_key, proxy_secret)
            if not initial_proxy:
                logger.error("Failed to get initial proxy IP. Aborting.")
                return

        browser, context, page = _create_context(p, initial_proxy, None)

        # Inject cookies if available
        if cookie_str:
            cookies = _parse_cookie_string(cookie_str)
            context.add_cookies(cookies)
            logger.info(f"Injected {len(cookies)} cookies")

        # Open THS login page — let user log in first
        logger.info("Opening 同花顺... please log in if needed.")
        page.goto("https://upass.10jqka.com.cn/login", timeout=30000)
        input(
            "\n============================================\n"
            "  请在弹出的浏览器窗口中登录同花顺\n"
            "  登录完成后回到这里按 Enter 继续\n"
            "============================================\n"
            ">>> "
        )

        # Verify login by loading a board page
        logger.info("Verifying login...")
        page.goto(
            "https://q.10jqka.com.cn/thshy/detail/code/886108",
            wait_until="domcontentloaded",
            timeout=30000,
        )
        try:
            page.wait_for_selector("table.m-table tbody tr", timeout=20000)
            stocks = _extract_page_stocks(page)
            logger.info(f"Login verified — test page loaded {len(stocks)} rows")
        except Exception:
            logger.error("Login failed — cannot load board data. Aborting.")
            context.close()
            browser.close()
            return

        # Save cookies from logged-in session (for proxy rotation)
        saved_cookies = context.cookies()

        # Load progress: boards already verified by THS scraper
        done: set[str] = set()
        if PROGRESS_PATH.exists() and not test_board:
            with open(PROGRESS_PATH, encoding="utf-8") as f:
                done = set(json.load(f))
            logger.info(
                f"Progress: {len(done)} boards already done, {len(boards) - len(done)} remaining"
            )

        result: dict[str, list[list[str]]] = dict(existing)
        failed: list[str] = []
        skipped = 0
        scraped_since_rotate = 0

        for i, board in enumerate(boards, 1):
            board_name = board["name"]
            board_code = board["code"]

            # Skip boards already verified by THS scraper
            if board_name in done:
                skipped += 1
                continue

            # Rotate proxy every N boards
            if use_rotating_proxy and scraped_since_rotate >= _PROXY_ROTATE_EVERY:
                new_proxy = _fetch_proxy_ip(proxy_key, proxy_secret)
                if new_proxy:
                    context.close()
                    browser.close()
                    browser, context, page = _create_context(p, new_proxy, saved_cookies)
                    scraped_since_rotate = 0
                    # Random pause after IP switch
                    pause = 3 + random.random() * 5
                    logger.info(f"Switched proxy, pausing {pause:.0f}s...")
                    time.sleep(pause)

            try:
                stocks = _scrape_board(page, board_code, board_name)

                if stocks is not None and len(stocks) > 0:
                    result[board_name] = stocks
                    done.add(board_name)
                    scraped_since_rotate += 1
                    logger.info(
                        f"[{i}/{len(boards)}] {board_name} ({board_code}): {len(stocks)} stocks"
                    )
                else:
                    failed.append(board_name)
                    logger.warning(f"[{i}/{len(boards)}] {board_name} ({board_code}): FAILED (0 stocks)")

            except Exception as e:
                err_msg = str(e)
                if "ERR_" in err_msg:
                    # Connection dead — wait 3 min, re-login, retry this board
                    logger.error(
                        f"[{i}/{len(boards)}] {board_name}: connection lost. Waiting 3 min..."
                    )
                    time.sleep(180)
                    try:
                        context.close()
                        browser.close()
                    except Exception:
                        pass
                    if use_rotating_proxy:
                        new_proxy = _fetch_proxy_ip(proxy_key, proxy_secret)
                    else:
                        new_proxy = proxy
                    browser, context, page = _create_context(p, new_proxy, saved_cookies)
                    scraped_since_rotate = 0
                    # Let user re-login
                    page.goto("https://upass.10jqka.com.cn/login", timeout=30000)
                    input(
                        "\n============================================\n"
                        "  连接断开，请重新登录同花顺\n"
                        "  登录完成后按 Enter 继续\n"
                        "============================================\n"
                        ">>> "
                    )
                    saved_cookies = context.cookies()
                    # Retry this board
                    try:
                        stocks = _scrape_board(page, board_code, board_name)
                        if stocks is not None and len(stocks) > 0:
                            result[board_name] = stocks
                            done.add(board_name)
                            scraped_since_rotate += 1
                            logger.info(
                                f"[{i}/{len(boards)}] {board_name} ({board_code}): "
                                f"{len(stocks)} stocks (retry ok)"
                            )
                        else:
                            failed.append(board_name)
                            logger.warning(
                                f"[{i}/{len(boards)}] {board_name}: "
                                f"retry got 0 stocks, marked FAILED"
                            )
                    except Exception as e2:
                        failed.append(board_name)
                        logger.error(f"[{i}/{len(boards)}] {board_name}: retry failed: {e2}")
                else:
                    failed.append(board_name)
                    logger.error(f"[{i}/{len(boards)}] {board_name} ({board_code}): ERROR {e}")

            # Rate limit — random pause
            if i < len(boards):
                time.sleep(3 + random.random() * 5)

            # Save data + progress after every board
            if not test_board:
                OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
                with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
                    json.dump(result, f, ensure_ascii=False, indent=2)
                with open(PROGRESS_PATH, "w", encoding="utf-8") as f:
                    json.dump(sorted(done), f, ensure_ascii=False)

        context.close()
        browser.close()

    if test_board:
        # Test mode: print results only, never touch main data file
        for bname, stocks in result.items():
            if bname not in existing:
                print(f"\n{bname}: {len(stocks)} stocks")
                for code, name in stocks[:20]:
                    print(f"  {code} {name}")
                if len(stocks) > 20:
                    print(f"  ... ({len(stocks) - 20} more)")
    else:
        # Final save
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

    total_stocks = sum(len(v) for v in result.values())
    logger.info(
        f"Done: {len(result)} boards ({total_stocks} stocks), "
        f"{skipped} skipped, {len(failed)} failed"
    )
    if not test_board:
        logger.info(f"Saved to {OUTPUT_PATH}")
        logger.info(f"Progress: {len(done)}/{len(boards)} verified")
    if failed:
        logger.warning(f"Failed boards ({len(failed)}): {failed}")


def main() -> None:
    parser = argparse.ArgumentParser(description="通过同花顺网页下载概念板块成分股")
    parser.add_argument("--dry-run", action="store_true", help="只打印板块列表")
    parser.add_argument("--test", type=str, help="测试单个板块，传板块名称")
    parser.add_argument("--headless", action="store_true", help="无头模式（不显示浏览器窗口）")
    parser.add_argument("--proxy", type=str, help="固定代理地址，如 http://127.0.0.1:7890")
    parser.add_argument("--proxy-key", type=str, help="小象代理 appKey")
    parser.add_argument("--proxy-secret", type=str, help="小象代理 appSecret")
    args = parser.parse_args()
    scrape_all(
        dry_run=args.dry_run,
        test_board=args.test,
        headless=args.headless,
        proxy=args.proxy,
        proxy_key=args.proxy_key,
        proxy_secret=args.proxy_secret,
    )


if __name__ == "__main__":
    main()
