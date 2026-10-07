"""One collection cycle, or a configurable loop, without changing production scheduling."""

import argparse
import asyncio
import json
import math
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.data.cross_market_ingest import CrossMarketIngestor
from src.data.cross_market_store import CrossMarketStore
from src.data.yahoo_indices import YahooIndexClient


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--proxy", default=os.environ.get("CROSS_MARKET_YAHOO_PROXY") or None)
    parser.add_argument("--fc-endpoint", default=os.environ.get("CROSS_MARKET_FC_ENDPOINT") or None)
    parser.add_argument("--fc-function", default=os.environ.get("CROSS_MARKET_FC_FUNCTION") or None)
    parser.add_argument(
        "--greptime-url",
        default=os.environ.get("CROSS_MARKET_GREPTIME_URL", "http://localhost:4000"),
    )
    parser.add_argument(
        "--reference",
        type=Path,
        default=Path(
            os.environ.get(
                "CROSS_MARKET_REFERENCE",
                str(PROJECT_ROOT / "src/data/reference/cross_market/industry_indices.json"),
            )
        ),
    )
    parser.add_argument(
        "--base-reference",
        type=Path,
        default=PROJECT_ROOT / "src/data/reference/cross_market/industry_boards.json",
    )
    parser.add_argument(
        "--state-dir",
        type=Path,
        default=Path(
            os.environ.get("CROSS_MARKET_STATE_DIR", str(PROJECT_ROOT / "data/cross_market"))
        ),
    )
    parser.add_argument(
        "--loop-seconds",
        type=float,
        default=float(os.environ.get("CROSS_MARKET_LOOP_SECONDS", "0")),
    )
    parser.add_argument(
        "--concurrency", type=int, default=int(os.environ.get("CROSS_MARKET_CONCURRENCY", "4"))
    )
    parser.add_argument("--batch-size", type=int, default=100)
    args = parser.parse_args()
    if not math.isfinite(args.loop_seconds) or args.loop_seconds < 0:
        parser.error("--loop-seconds must be zero or positive")
    if args.fc_function and not args.fc_endpoint:
        parser.error("--fc-function requires --fc-endpoint")
    if args.fc_endpoint and args.proxy:
        parser.error("FC collection and the local Yahoo proxy cannot be combined")
    return args


async def run(args):
    yahoo = None
    store = None
    try:
        endpoint = getattr(args, "fc_endpoint", None)
        function = getattr(args, "fc_function", None)
        if endpoint or function:
            if not endpoint or args.proxy:
                raise ValueError("FC collection needs an endpoint and cannot use a Yahoo proxy")
            from src.data.fc_yahoo_indices import FC_FUNCTION_NAME, FC_REGION, FCYahooIndexClient

            yahoo = FCYahooIndexClient(
                endpoint,
                function or FC_FUNCTION_NAME,
                region=os.environ.get("CROSS_MARKET_FC_REGION") or FC_REGION,
            )
        else:
            yahoo = YahooIndexClient(proxy=args.proxy)
        store = CrossMarketStore(args.greptime_url, batch_size=args.batch_size, timeout=120)
        producer = CrossMarketIngestor(
            reference_path=args.reference,
            base_reference_path=args.base_reference,
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
            failed = result["status"] in {"failed", "partial_failure"}
            if not args.loop_seconds:
                return 1 if failed else 0
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
