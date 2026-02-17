#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
漏斗层级收益分析。

对选股漏斗的每一层做收益回测：从热门板块全部成分股开始，逐层过滤，
每层假设9:40买入（每只1手），次日开盘卖出，扣除手续费后计算净收益。
与区间回测使用相同的卖出价（T+1开盘）和费用模型。
如果收益逐层递增，说明漏斗有效；如果某层收益反降，说明该层在筛掉好股票。

层级定义：
  L0: 热门板块全部成分股（主板，有价格数据）
  L1: L0 + 9:40涨幅 >= 0.56%
  L2: L1 + PE(TTM) 在板块中位数 ±30%
  L3: L2 + 高开低走过滤
  L4: 最终推荐（单只）

用法：
    uv run python scripts/analyze_funnel.py -s 2026-02-10 -e 2026-02-14
    uv run python scripts/analyze_funnel.py -s 2026-02-10 -e 2026-02-10 --no-fade-filter
"""

import argparse
import asyncio
import io
import logging
import sys
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from statistics import median

# Fix Windows console encoding for Chinese characters
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

# Add project root and scripts dir to path
PROJECT_ROOT = Path(__file__).parent.parent
SCRIPTS_DIR = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(SCRIPTS_DIR))

from backtest_momentum import fetch_main_board_prices_for_date  # noqa: E402

from src.data.clients.ifind_http_client import IFinDHttpClient, IFinDHttpError  # noqa: E402
from src.data.database.fundamentals_db import create_fundamentals_db_from_config  # noqa: E402
from src.data.sources.concept_mapper import ConceptMapper  # noqa: E402
from src.strategy.filters.gap_fade_filter import GapFadeConfig, GapFadeFilter  # noqa: E402
from src.strategy.filters.stock_filter import create_main_board_only_filter  # noqa: E402
from src.strategy.strategies.momentum_sector_scanner import (  # noqa: E402
    MomentumSectorScanner,
    SelectedStock,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# === DATA MODELS ===


@dataclass
class StockReturn:
    """A stock with its next-day return (after transaction costs)."""

    stock_code: str
    stock_name: str
    board_name: str
    buy_price: float  # 9:40 price
    sell_price: float  # T+1 open
    return_pct: float  # net return after costs


@dataclass
class LayerResult:
    """Results for a single funnel layer on a single date."""

    stock_codes: set[str] = field(default_factory=set)
    returns: list[StockReturn] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.stock_codes)

    @property
    def avg_return(self) -> float:
        if not self.returns:
            return 0.0
        return sum(r.return_pct for r in self.returns) / len(self.returns)

    @property
    def win_rate(self) -> float:
        if not self.returns:
            return 0.0
        wins = sum(1 for r in self.returns if r.return_pct > 0)
        return wins / len(self.returns) * 100

    @property
    def median_return(self) -> float:
        if not self.returns:
            return 0.0
        return median(r.return_pct for r in self.returns)


@dataclass
class DayResult:
    """All layer results for a single trading day."""

    trade_date: date
    layers: dict[str, LayerResult] = field(default_factory=dict)


LAYER_NAMES = ["L0: 全部成分股", "L1: 涨幅>0.56%", "L2: PE过滤", "L3: 高开低走过滤", "L4: 最终推荐"]


# === CORE ANALYSIS ===


def calc_net_return_pct(buy_price: float, sell_price: float) -> float:
    """Calculate net return percentage after transaction costs (assuming 1 lot = 100 shares).

    Uses the same cost model as interval backtest:
    - Buy commission: max(0.3%, ¥5)
    - Sell commission: max(0.3%, ¥5)
    - Stamp tax (sell): 0.05%
    - Transfer fee: 0.001% each way
    """
    shares = 100  # 1 lot
    buy_amount = shares * buy_price
    buy_commission = max(buy_amount * 0.003, 5.0)
    buy_transfer = buy_amount * 0.00001
    total_buy_cost = buy_amount + buy_commission + buy_transfer

    sell_amount = shares * sell_price
    sell_commission = max(sell_amount * 0.003, 5.0)
    sell_transfer = sell_amount * 0.00001
    sell_stamp = sell_amount * 0.0005
    net_sell = sell_amount - sell_commission - sell_transfer - sell_stamp

    return (net_sell - total_buy_cost) / total_buy_cost * 100 if total_buy_cost > 0 else 0.0


async def fetch_next_day_open(
    client: IFinDHttpClient,
    stock_codes: list[str],
    next_trade_date: date,
) -> dict[str, float]:
    """Fetch open prices on the next trading day for return calculation (次日开盘卖)."""
    result: dict[str, float] = {}
    batch_size = 50
    date_str = next_trade_date.strftime("%Y-%m-%d")

    for i in range(0, len(stock_codes), batch_size):
        batch = stock_codes[i : i + batch_size]
        codes_str = ",".join(f"{c}.SH" if c.startswith("6") else f"{c}.SZ" for c in batch)

        try:
            # iFinD returns empty tables for single-indicator queries, include preClose
            data = await client.history_quotes(
                codes=codes_str,
                indicators="open,preClose",
                start_date=date_str,
                end_date=date_str,
            )

            for table_entry in data.get("tables", []):
                thscode = table_entry.get("thscode", "")
                bare = thscode.split(".")[0] if thscode else ""
                if not bare:
                    continue

                tbl = table_entry.get("table", {})
                open_vals = tbl.get("open", [])
                if open_vals and open_vals[0] is not None:
                    result[bare] = float(open_vals[0])

        except IFinDHttpError as e:
            logger.warning(f"Failed to fetch T+1 open for batch: {e}")

    return result


def _dedup_stocks(stocks: list[SelectedStock]) -> list[SelectedStock]:
    """Deduplicate by stock_code, keeping highest open_gain_pct."""
    seen: dict[str, SelectedStock] = {}
    for s in stocks:
        existing = seen.get(s.stock_code)
        if existing is None or s.open_gain_pct > existing.open_gain_pct:
            seen[s.stock_code] = s
    return list(seen.values())


async def run_single_date(
    trade_date: date,
    next_trade_date: date,
    ifind_client: IFinDHttpClient,
    fundamentals_db,
    concept_mapper: ConceptMapper,
    fade_filter_enabled: bool = True,
    pe_filter_enabled: bool = True,
) -> DayResult | None:
    """Run funnel analysis for a single trading day."""
    logger.info(f"=== Analyzing {trade_date} (T+1={next_trade_date}) ===")
    day_result = DayResult(trade_date=trade_date)

    stock_filter = create_main_board_only_filter()
    gap_fade_config = GapFadeConfig(enabled=fade_filter_enabled)
    scanner = MomentumSectorScanner(
        ifind_client=ifind_client,
        fundamentals_db=fundamentals_db,
        concept_mapper=concept_mapper,
        stock_filter=stock_filter,
        gap_fade_config=gap_fade_config,
    )

    # Fetch price data for the date
    price_snapshots = await fetch_main_board_prices_for_date(ifind_client, trade_date)
    if not price_snapshots:
        logger.warning(f"No price data for {trade_date}, skipping")
        return None

    # Steps 1-3: find hot boards (reuse scanner internals)
    scanner._trade_date = trade_date
    gainers = await scanner._step1_filter_gainers(price_snapshots)
    if not gainers:
        logger.info(f"{trade_date}: No gainers found")
        return None

    stock_boards = await scanner._step2_reverse_lookup(list(gainers.keys()))
    hot_boards = scanner._step3_find_hot_boards(stock_boards)
    if not hot_boards:
        logger.info(f"{trade_date}: No hot boards found")
        return None

    # Step 4: get all constituents
    board_constituents = await scanner._step4_get_constituents(list(hot_boards.keys()))

    # === Layer capture: replicate Step 5 with per-layer tracking ===

    # Main board filter on constituents
    all_constituent_codes: set[str] = set()
    filtered_board_constituents: dict[str, list[tuple[str, str]]] = {}
    for board_name, stocks in board_constituents.items():
        allowed = [(code, name) for code, name in stocks if stock_filter.is_allowed(code)]
        filtered_board_constituents[board_name] = allowed
        for code, _ in allowed:
            all_constituent_codes.add(code)

    # Fetch PE data
    pe_data = await fundamentals_db.batch_get_pe(list(all_constituent_codes))

    # Fetch prices for missing constituents
    missing_codes = [c for c in all_constituent_codes if c not in price_snapshots]
    if missing_codes:
        extra_prices = await scanner._fetch_constituent_prices(missing_codes)
        price_snapshots = {**price_snapshots, **extra_prices}

    # Per-layer stock lists (as SelectedStock for compatibility with gap-fade filter)
    layer0_stocks: list[SelectedStock] = []  # all with valid price
    layer1_stocks: list[SelectedStock] = []  # + gain filter
    layer2_stocks: list[SelectedStock] = []  # + PE filter

    for board_name, stocks in filtered_board_constituents.items():
        # Calculate board median PE
        board_pe_values = [pe_data[c] for c, _ in stocks if pe_data.get(c) and pe_data[c] > 0]
        if board_pe_values:
            sorted_pe = sorted(board_pe_values)
            n = len(sorted_pe)
            board_median_pe = (
                sorted_pe[n // 2] if n % 2 else (sorted_pe[n // 2 - 1] + sorted_pe[n // 2]) / 2
            )
            pe_lower = board_median_pe * (1 - MomentumSectorScanner.PE_TOLERANCE)
            pe_upper = board_median_pe * (1 + MomentumSectorScanner.PE_TOLERANCE)
        else:
            board_median_pe = 0.0
            pe_lower = pe_upper = 0.0

        for code, name in stocks:
            snap = price_snapshots.get(code)
            if not snap:
                continue

            pe = pe_data.get(code)

            # L0: has valid price data (main board already filtered)
            layer0_stocks.append(
                SelectedStock(
                    stock_code=code,
                    stock_name=name,
                    board_name=board_name,
                    open_gain_pct=snap.open_gain_pct,
                    pe_ttm=pe if pe and pe > 0 else 0.0,
                    board_avg_pe=board_median_pe,
                )
            )

            # L1: + gain filter
            if snap.gain_from_open_pct >= MomentumSectorScanner.GAIN_FROM_OPEN_THRESHOLD:
                layer1_stocks.append(
                    SelectedStock(
                        stock_code=code,
                        stock_name=name,
                        board_name=board_name,
                        open_gain_pct=snap.open_gain_pct,
                        pe_ttm=pe if pe and pe > 0 else 0.0,
                        board_avg_pe=board_median_pe,
                    )
                )

                # L2: + PE filter (need valid PE and within range)
                # When PE filter disabled, L2 passes through all L1 stocks
                if pe_filter_enabled:
                    pe_pass = pe and pe > 0 and pe_lower > 0 and pe_lower <= pe <= pe_upper
                else:
                    pe_pass = True

                if pe_pass:
                    layer2_stocks.append(
                        SelectedStock(
                            stock_code=code,
                            stock_name=name,
                            board_name=board_name,
                            open_gain_pct=snap.open_gain_pct,
                            pe_ttm=pe if pe and pe > 0 else 0.0,
                            board_avg_pe=board_median_pe,
                        )
                    )

    # Deduplicate each layer
    layer0_stocks = _dedup_stocks(layer0_stocks)
    layer1_stocks = _dedup_stocks(layer1_stocks)
    layer2_stocks = _dedup_stocks(layer2_stocks)

    # L3: gap-fade filter applied to L2
    gap_fade_filter = GapFadeFilter(ifind_client, gap_fade_config)
    if fade_filter_enabled and layer2_stocks:
        layer3_stocks, _ = await gap_fade_filter.filter_stocks(
            layer2_stocks, price_snapshots, trade_date
        )
    else:
        layer3_stocks = list(layer2_stocks)

    # L4: recommendation (single stock)
    layer4_stocks: list[SelectedStock] = []
    if layer3_stocks:
        rec = await scanner._step6_recommend(layer3_stocks, price_snapshots)
        if rec:
            layer4_stocks = [
                SelectedStock(
                    stock_code=rec.stock_code,
                    stock_name=rec.stock_name,
                    board_name=rec.board_name,
                    open_gain_pct=rec.open_gain_pct,
                    pe_ttm=rec.pe_ttm,
                    board_avg_pe=rec.board_avg_pe,
                )
            ]

    all_layers = [layer0_stocks, layer1_stocks, layer2_stocks, layer3_stocks, layer4_stocks]

    # Collect all unique stock codes across all layers for T+1 fetch
    all_codes: set[str] = set()
    for layer_stocks in all_layers:
        for s in layer_stocks:
            all_codes.add(s.stock_code)

    if not all_codes:
        logger.info(f"{trade_date}: No stocks in any layer")
        return None

    # Fetch T+1 open prices (consistent with interval backtest: 次日开盘卖)
    next_open_prices = await fetch_next_day_open(ifind_client, list(all_codes), next_trade_date)
    logger.info(f"Fetched T+1 open for {len(next_open_prices)}/{len(all_codes)} stocks")

    # Calculate returns for each layer (with transaction costs, consistent with backtest)
    for layer_name, layer_stocks in zip(LAYER_NAMES, all_layers):
        layer_result = LayerResult(stock_codes={s.stock_code for s in layer_stocks})

        for s in layer_stocks:
            snap = price_snapshots.get(s.stock_code)
            sell_price = next_open_prices.get(s.stock_code)

            if not snap or not sell_price or snap.latest_price <= 0:
                continue

            ret_pct = calc_net_return_pct(snap.latest_price, sell_price)
            layer_result.returns.append(
                StockReturn(
                    stock_code=s.stock_code,
                    stock_name=s.stock_name,
                    board_name=s.board_name,
                    buy_price=snap.latest_price,
                    sell_price=sell_price,
                    return_pct=ret_pct,
                )
            )

        day_result.layers[layer_name] = layer_result

    return day_result


# === OUTPUT ===


def print_day_detail(day: DayResult) -> None:
    """Print per-day layer breakdown."""
    print(f"\n  {day.trade_date}:")
    print(f"  {'层级':<18}  {'股数':>6}  {'平均收益':>10}  {'胜率':>8}  {'中位收益':>10}")
    for name in LAYER_NAMES:
        lr = day.layers.get(name)
        if lr and lr.returns:
            print(
                f"  {name:<18}  {lr.count:>6}  "
                f"{lr.avg_return:>+9.2f}%  {lr.win_rate:>6.1f}%  "
                f"{lr.median_return:>+9.2f}%"
            )
        elif lr:
            print(f"  {name:<18}  {lr.count:>6}  {'N/A':>10}  {'N/A':>8}  {'N/A':>10}")
        else:
            print(f"  {name:<18}  {'0':>6}  {'N/A':>10}  {'N/A':>8}  {'N/A':>10}")


def print_summary(all_days: list[DayResult]) -> None:
    """Print aggregate summary across all trading days."""
    n_days = len(all_days)

    print(f"\n{'=' * 76}")
    print(
        f"  漏斗层级收益分析汇总"
        f" ({all_days[0].trade_date} ~ {all_days[-1].trade_date},"
        f" 共{n_days}个交易日)"
    )
    print(f"{'=' * 76}")

    # Aggregate metrics per layer
    print(f"\n  {'层级':<18}  {'平均股数':>8}  {'平均次日收益':>12}  {'胜率':>8}  {'中位收益':>10}")
    print(f"  {'─' * 70}")

    prev_avg_return: float | None = None
    for name in LAYER_NAMES:
        all_returns: list[float] = []
        total_stocks = 0
        days_with_data = 0

        for day in all_days:
            lr = day.layers.get(name)
            if lr:
                total_stocks += lr.count
                days_with_data += 1
                for r in lr.returns:
                    all_returns.append(r.return_pct)

        if not all_returns:
            print(f"  {name:<18}  {'0':>8}  {'N/A':>12}  {'N/A':>8}  {'N/A':>10}")
            prev_avg_return = None
            continue

        avg_count = total_stocks / max(days_with_data, 1)
        avg_ret = sum(all_returns) / len(all_returns)
        win = sum(1 for r in all_returns if r > 0) / len(all_returns) * 100
        med_ret = median(all_returns)

        # Arrow indicator for change from previous layer
        arrow = ""
        if prev_avg_return is not None:
            diff = avg_ret - prev_avg_return
            if diff > 0.05:
                arrow = " ^"
            elif diff < -0.05:
                arrow = " !!"

        print(
            f"  {name:<18}  {avg_count:>8.1f}  "
            f"{avg_ret:>+10.2f}%{arrow:<2}  {win:>6.1f}%  "
            f"{med_ret:>+9.2f}%"
        )
        prev_avg_return = avg_ret

    # Filtered-out best stocks per layer transition
    print(f"\n  {'─' * 70}")
    _print_filtered_out_best(all_days)

    # Conclusion
    print(f"\n  {'─' * 70}")
    _print_conclusions(all_days)
    print()


def _print_filtered_out_best(all_days: list[DayResult]) -> None:
    """Show best-performing stocks that were filtered out at each layer."""
    print("  被误杀的高收益股票 (每层筛掉的票中收益最好的):")

    transitions = [
        (LAYER_NAMES[0], LAYER_NAMES[1], "L0→L1 涨幅筛选"),
        (LAYER_NAMES[1], LAYER_NAMES[2], "L1→L2 PE过滤"),
        (LAYER_NAMES[2], LAYER_NAMES[3], "L2→L3 高开低走"),
        (LAYER_NAMES[3], LAYER_NAMES[4], "L3→L4 最终推荐"),
    ]

    for prev_name, curr_name, label in transitions:
        # Collect filtered-out stock returns across all days
        filtered_returns: list[tuple[date, StockReturn]] = []

        for day in all_days:
            prev_lr = day.layers.get(prev_name)
            curr_lr = day.layers.get(curr_name)
            if not prev_lr or not curr_lr:
                continue

            dropped_codes = prev_lr.stock_codes - curr_lr.stock_codes
            for r in prev_lr.returns:
                if r.stock_code in dropped_codes:
                    filtered_returns.append((day.trade_date, r))

        if not filtered_returns:
            print(f"\n    {label}: 无筛除")
            continue

        # Stats
        all_rets = [r.return_pct for _, r in filtered_returns]
        avg_ret = sum(all_rets) / len(all_rets)
        positive = sum(1 for x in all_rets if x > 0)

        # Top 3 best filtered-out stocks
        top3 = sorted(filtered_returns, key=lambda x: x[1].return_pct, reverse=True)[:3]

        print(
            f"\n    {label}: 共筛掉{len(filtered_returns)}只,"
            f" 平均收益{avg_ret:+.2f}%,"
            f" 其中{positive}只盈利"
        )
        for dt, r in top3:
            print(
                f"      {dt} {r.stock_code} {r.stock_name:<6}"
                f"  {r.board_name:<10}"
                f"  次日收益 {r.return_pct:+.2f}%"
            )


def _print_conclusions(all_days: list[DayResult]) -> None:
    """Print automated conclusions about each filter layer."""
    layer_avg_returns: dict[str, float] = {}
    for name in LAYER_NAMES:
        all_returns = []
        for day in all_days:
            lr = day.layers.get(name)
            if lr:
                all_returns.extend(r.return_pct for r in lr.returns)
        if all_returns:
            layer_avg_returns[name] = sum(all_returns) / len(all_returns)

    print("  结论:")
    pairs = [
        (LAYER_NAMES[0], LAYER_NAMES[1], "涨幅筛选"),
        (LAYER_NAMES[1], LAYER_NAMES[2], "PE过滤"),
        (LAYER_NAMES[2], LAYER_NAMES[3], "高开低走过滤"),
        (LAYER_NAMES[3], LAYER_NAMES[4], "最终推荐"),
    ]
    for prev_name, curr_name, filter_label in pairs:
        prev_ret = layer_avg_returns.get(prev_name)
        curr_ret = layer_avg_returns.get(curr_name)
        if prev_ret is not None and curr_ret is not None:
            diff = curr_ret - prev_ret
            ret_str = f"{prev_ret:+.2f}% -> {curr_ret:+.2f}% ({diff:+.2f}%)"
            if diff > 0.05:
                print(f"    {filter_label}: 收益 {ret_str}, 有效过滤")
            elif diff < -0.05:
                print(f"    {filter_label}: 收益 {ret_str}, 疑似负面过滤!")
            else:
                print(f"    {filter_label}: 收益 {ret_str}, 影响不大")


# === MAIN ===


async def run_analysis(
    start_date: date,
    end_date: date,
    fade_filter: bool = True,
    pe_filter: bool = True,
) -> None:
    """Run funnel analysis across a date range."""
    ifind_client = IFinDHttpClient()
    fundamentals_db = create_fundamentals_db_from_config()

    try:
        await ifind_client.start()
        await fundamentals_db.connect()

        # Get trading dates in range (+ a few extra days for T+1)
        from datetime import timedelta

        extended_end = end_date + timedelta(days=10)
        trade_dates_raw = await ifind_client.get_trade_dates(
            market_code="212001",  # SSE
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=extended_end.strftime("%Y-%m-%d"),
        )

        if not trade_dates_raw:
            logger.error("Failed to get trading dates from iFinD, falling back to calendar dates")
            # Fallback: use calendar dates, skip weekends
            trade_dates_raw = []
            d = start_date
            while d <= extended_end:
                if d.weekday() < 5:  # Mon-Fri
                    trade_dates_raw.append(d.strftime("%Y-%m-%d"))
                d += timedelta(days=1)

        # Parse to date objects
        trade_dates = [date.fromisoformat(d) for d in trade_dates_raw]
        # Filter to requested range
        dates_in_range = [d for d in trade_dates if start_date <= d <= end_date]
        # Build T+1 mapping
        next_day_map: dict[date, date] = {}
        for i, d in enumerate(trade_dates):
            if d > end_date:
                break
            if d >= start_date and i + 1 < len(trade_dates):
                next_day_map[d] = trade_dates[i + 1]

        if not dates_in_range:
            logger.error(f"No trading dates in range {start_date} ~ {end_date}")
            return

        logger.info(
            f"Analyzing {len(dates_in_range)} trading days:"
            f" {dates_in_range[0]} ~ {dates_in_range[-1]}"
        )

        concept_mapper = ConceptMapper(ifind_client)
        all_days: list[DayResult] = []

        for trade_date in dates_in_range:
            next_trade_date = next_day_map.get(trade_date)
            if not next_trade_date:
                logger.warning(f"{trade_date}: No next trading day available, skipping")
                continue

            # Clear concept mapper cache for each new date
            # (board membership may vary across dates in iwencai)
            concept_mapper.clear_cache()

            day_result = await run_single_date(
                trade_date=trade_date,
                next_trade_date=next_trade_date,
                ifind_client=ifind_client,
                fundamentals_db=fundamentals_db,
                concept_mapper=concept_mapper,
                fade_filter_enabled=fade_filter,
                pe_filter_enabled=pe_filter,
            )

            if day_result:
                all_days.append(day_result)
                print_day_detail(day_result)

        if not all_days:
            print("\n  没有可分析的交易日数据")
            return

        print_summary(all_days)

    finally:
        await fundamentals_db.close()
        await ifind_client.stop()


def main():
    parser = argparse.ArgumentParser(description="漏斗层级收益分析")
    parser.add_argument(
        "--start-date",
        "-s",
        type=str,
        required=True,
        help="开始日期 (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        "-e",
        type=str,
        required=True,
        help="结束日期 (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--no-fade-filter",
        action="store_true",
        help="禁用高开低走过滤器 (L3 = L2)",
    )
    parser.add_argument(
        "--no-pe-filter",
        action="store_true",
        help="禁用PE过滤 (L2 = L1)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="显示调试信息",
    )

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    s = date.fromisoformat(args.start_date)
    e = date.fromisoformat(args.end_date)

    asyncio.run(
        run_analysis(s, e, fade_filter=not args.no_fade_filter, pe_filter=not args.no_pe_filter)
    )


if __name__ == "__main__":
    main()
