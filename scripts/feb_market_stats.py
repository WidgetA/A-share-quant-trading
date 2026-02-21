"""
Query February 2026 trading days: count up/down/flat stocks for A-share main board (沪深主板).
Uses akshare for data.
"""

import akshare as ak
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed


def get_stock_hist(code: str) -> pd.DataFrame:
    """Get one stock's Feb daily data."""
    try:
        df = ak.stock_zh_a_hist(
            symbol=code, period="daily",
            start_date="20260201", end_date="20260218", adjust="",
        )
        if df is not None and not df.empty:
            df = df[["日期", "涨跌幅"]].copy()
            df["code"] = code
            return df
    except Exception:
        pass
    return pd.DataFrame()


def main():
    # Step 1: Get all A-share stock codes
    print("Fetching A-share stock list...")
    stock_list = ak.stock_info_a_code_name()
    print(f"Total A-share stocks: {len(stock_list)}")

    # Filter main board only:
    #   Shanghai main board: 60xxxx
    #   Shenzhen main board: 000xxx, 001xxx, 002xxx, 003xxx (中小板2021年并入主板)
    mask = stock_list["code"].str.match(r"^(60\d{4}|00[0-3]\d{3})$")
    main_board = stock_list[mask]
    codes = main_board["code"].tolist()
    print(f"Main board stocks: {len(codes)}")

    # Step 2: Concurrent fetch
    print("Fetching daily data (this may take a minute)...")
    results = []
    done = 0
    total = len(codes)

    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = {executor.submit(get_stock_hist, c): c for c in codes}
        for future in as_completed(futures):
            done += 1
            if done % 200 == 0:
                print(f"  Progress: {done}/{total}")
            df = future.result()
            if not df.empty:
                results.append(df)

    if not results:
        print("No data fetched!")
        return

    all_data = pd.concat(results, ignore_index=True)
    print(f"\nTotal records: {len(all_data)}")
    print()

    # Step 3: Aggregate by date
    print("=" * 70)
    print(f"{'日期':<14} {'总数':>6} {'上涨':>6} {'下跌':>6} {'平盘':>6}  {'涨/跌比'}")
    print("=" * 70)

    for date_val, group in sorted(all_data.groupby("日期")):
        total_n = len(group)
        up = (group["涨跌幅"] > 0).sum()
        down = (group["涨跌幅"] < 0).sum()
        flat = (group["涨跌幅"] == 0).sum()
        ratio = f"{up}:{down}"
        print(
            f"{date_val!s:<14} {total_n:>6} "
            f"{up:>6} ({up/total_n*100:4.1f}%) "
            f"{down:>6} ({down/total_n*100:4.1f}%) "
            f"{flat:>6} ({flat/total_n*100:4.1f}%)  "
            f"{ratio}"
        )

    print("=" * 70)


if __name__ == "__main__":
    main()
