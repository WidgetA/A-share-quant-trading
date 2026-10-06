"""Backfill reviewed original US indices, with one persistent five-per-minute queue."""

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.data.cross_market_massive_ingest import CrossMarketMassiveIngestor
from src.data.cross_market_store import CrossMarketStore
from src.data.massive_indices import MassiveIndexClient


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--key-file", type=Path, required=True)
    parser.add_argument("--key-variable", default="MASSIVE_API_KEY")
    parser.add_argument("--proxy", default=os.environ.get("CROSS_MARKET_MASSIVE_PROXY") or None)
    parser.add_argument(
        "--greptime-url",
        default=os.environ.get("CROSS_MARKET_GREPTIME_URL", "http://8.133.23.9:8000"),
    )
    parser.add_argument(
        "--reference",
        type=Path,
        default=PROJECT_ROOT / "src/data/reference/cross_market/massive_indices.json",
    )
    parser.add_argument(
        "--industry-reference",
        type=Path,
        default=PROJECT_ROOT / "src/data/reference/cross_market/industry_indices.json",
    )
    parser.add_argument(
        "--state-dir", type=Path, default=PROJECT_ROOT / "data/cross_market/state-massive"
    )
    parser.add_argument("--start-date", default="2023-01-01")
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--batch-size", type=int, default=1000)
    args = parser.parse_args()
    if not args.key_file.is_file():
        parser.error("--key-file must be an existing dotenv")
    if args.batch_size <= 0:
        parser.error("--batch-size must be positive")
    return args


async def run(args):
    massive = store = None
    try:
        # Validate the reference before opening a source connection or mutating DB.
        from src.data.cross_market_massive_ingest import load_massive_reference

        load_massive_reference(args.reference, args.industry_reference)
        massive = MassiveIndexClient(
            key_file=args.key_file,
            key_variable=args.key_variable,
            proxy=args.proxy,
            rate_state_path=args.state_dir / "rate.json",
        )
        store = CrossMarketStore(args.greptime_url, batch_size=args.batch_size, timeout=120)
        producer = CrossMarketMassiveIngestor(
            reference_path=args.reference,
            industry_reference_path=args.industry_reference,
            state_dir=args.state_dir,
            massive=massive,
            store=store,
            progress=lambda event: print(json.dumps(event, ensure_ascii=False), flush=True),
        )
        result = await producer.run_once(
            start_date=args.start_date,
            end_date=args.end_date or datetime.now(ZoneInfo("America/New_York")).date().isoformat(),
        )
        print(json.dumps(result, ensure_ascii=False), flush=True)
        return 0 if result["status"] == "verified" else 1
    except Exception as exc:
        # Never print source/vendor exception text: it may contain authentication.
        print(
            json.dumps(
                {
                    "status": "failed",
                    "error_type": type(exc).__name__,
                    "classification": getattr(exc, "classification", None),
                }
            ),
            flush=True,
        )
        return 1
    finally:
        if massive is not None:
            await massive.aclose()
        if store is not None:
            await store.aclose()


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(run(arguments())))
    except KeyboardInterrupt:
        raise SystemExit(130)
