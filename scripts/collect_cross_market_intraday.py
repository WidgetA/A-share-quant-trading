"""One complete source-retained minute backfill/update cycle, or a continuous loop."""

import argparse
import asyncio
import json
import math
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.data.cross_market_intraday_ingest import CrossMarketIntradayIngestor
from src.data.cross_market_store import CrossMarketStore


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fc-endpoint", default=os.environ.get("CROSS_MARKET_FC_ENDPOINT"))
    parser.add_argument(
        "--fc-function",
        default=os.environ.get("CROSS_MARKET_FC_FUNCTION", "ashare_yahoo_indices_v15"),
    )
    parser.add_argument(
        "--greptime-url",
        default=os.environ.get("CROSS_MARKET_GREPTIME_URL", "http://localhost:4000"),
    )
    root = PROJECT_ROOT / "src/data/reference/cross_market"
    parser.add_argument("--reference", type=Path, default=root / "industry_indices.json")
    parser.add_argument("--base-reference", type=Path, default=root / "industry_boards.json")
    parser.add_argument("--capability-reference", type=Path, default=root / "intraday_indices.json")
    parser.add_argument(
        "--state-dir",
        type=Path,
        default=Path(
            os.environ.get(
                "CROSS_MARKET_INTRADAY_STATE_DIR", str(PROJECT_ROOT / "data/cross_market_intraday")
            )
        ),
    )
    parser.add_argument(
        "--concurrency", type=int, default=int(os.environ.get("CROSS_MARKET_CONCURRENCY", "2"))
    )
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument(
        "--loop-seconds",
        type=float,
        default=float(os.environ.get("CROSS_MARKET_LOOP_SECONDS", "300")),
    )
    args = parser.parse_args()
    if not args.fc_endpoint:
        parser.error("--fc-endpoint is required")
    if not math.isfinite(args.loop_seconds) or args.loop_seconds < 0:
        parser.error("--loop-seconds must be zero or positive")
    return args


async def run(args):
    yahoo = store = None
    try:
        from src.data.fc_intraday_indices import FCYahooIntradayIndexClient

        yahoo = FCYahooIntradayIndexClient(
            args.fc_endpoint,
            args.fc_function,
            region=os.environ.get("CROSS_MARKET_FC_REGION", "us-west-1"),
        )
        store = CrossMarketStore(args.greptime_url, batch_size=args.batch_size)
        producer = CrossMarketIntradayIngestor(
            reference_path=args.reference,
            base_reference_path=args.base_reference,
            capability_path=args.capability_reference,
            state_dir=args.state_dir,
            yahoo=yahoo,
            store=store,
            concurrency=args.concurrency,
        )
        while True:
            try:
                result = await producer.run_once()
            except Exception as exc:
                result = {"status": "failed", "error_type": type(exc).__name__}
            print(json.dumps(result, ensure_ascii=False), flush=True)
            if not args.loop_seconds:
                return int(result["status"] in {"failed", "partial_failure"})
            await asyncio.sleep(args.loop_seconds)
    except Exception as exc:
        print(json.dumps({"status": "failed", "error_type": type(exc).__name__}), flush=True)
        return 1
    finally:
        if yahoo is not None:
            await yahoo.aclose()
        if store is not None:
            await store.aclose()


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(run(arguments())))
    except KeyboardInterrupt:
        raise SystemExit(130) from None
