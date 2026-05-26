# === MODULE PURPOSE ===
# One-off cleanup for the trade_notes "ghost row" bug:
#
# Before the fix in 4c19b2f, editing a broker-imported note with the
# minute-precision time input would soft-delete the original row at its
# sub-minute ts and insert a new row at the truncated ts. The next broker
# order poll (every 30s) then resurrected the soft-deleted shadow because
# `upsert_broker_event_by_order_id` did `ORDER BY ts DESC LIMIT 1` without
# filtering `deleted` — so a single event_id ended up with two live rows.
#
# This script finds every (code, event_id) group with more than one live
# row and soft-deletes the shadows, keeping the row with non-empty content
# (tiebreak: earliest ts — shadows always sit at the older `submit_time`
# ts since minute-truncation moves the live row backwards).
#
# Run inside the trading-service container so it picks up the same
# GreptimeDB config the app uses:
#
#   docker-compose exec trading-service uv run python \
#     scripts/cleanup_duplicate_note_shadows.py            # dry-run
#   docker-compose exec trading-service uv run python \
#     scripts/cleanup_duplicate_note_shadows.py --apply    # commit

from __future__ import annotations

import argparse
import asyncio
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.data.clients.greptime_storage import create_storage_from_config  # noqa: E402
from src.notes.note_store import TradeNoteStore  # noqa: E402


def _row_ts_ms(row) -> int:
    ts_val = row["ts"]
    if isinstance(ts_val, datetime):
        if ts_val.tzinfo is None:
            ts_val = ts_val.replace(tzinfo=timezone.utc)
        return int(ts_val.timestamp() * 1000)
    return int(ts_val)


def _content_len(row) -> int:
    return len(row["content"] or "") + len(row["content_external"] or "")


async def main(apply: bool) -> int:
    storage = create_storage_from_config()
    await storage.start()
    db = storage.db
    store = TradeNoteStore(storage)

    rows = await db.fetch(
        "SELECT ts, code, event_id, event_type, event_source, title, "
        "       price, qty, side, content, content_external, author "
        "FROM trade_notes "
        "WHERE (deleted = false OR deleted IS NULL)"
    )

    groups: dict[tuple[str, str], list] = defaultdict(list)
    for r in rows:
        groups[(r["code"], r["event_id"])].append(r)

    dup_groups = {k: v for k, v in groups.items() if len(v) > 1}
    print(f"Live rows total: {len(rows)}")
    print(f"Duplicate (code, event_id) groups: {len(dup_groups)}")
    if not dup_groups:
        print("Nothing to clean.")
        return 0

    shadows_to_delete = []
    for (code, event_id), grp in sorted(dup_groups.items()):
        # Keep the row with the most content; tie-break with earlier ts
        # (shadow rows from the bug always sit at the later, seconds-precision
        # broker submit_time, while the user's edited row sits at an earlier
        # minute-truncated ts).
        ranked = sorted(
            grp,
            key=lambda r: (-_content_len(r), _row_ts_ms(r)),
        )
        keeper = ranked[0]
        shadows = ranked[1:]
        print(
            f"\n  {code}/{event_id}: {len(grp)} live rows"
            f"\n    KEEP  ts={keeper['ts']!s} content_len={_content_len(keeper)} "
            f"title={keeper['title']!r}"
        )
        for s in shadows:
            print(f"    DROP  ts={s['ts']!s} content_len={_content_len(s)} title={s['title']!r}")
        shadows_to_delete.extend(shadows)

    print(f"\nTotal shadow rows to soft-delete: {len(shadows_to_delete)}")
    if not apply:
        print("Dry-run — pass --apply to commit.")
        return 0

    for r in shadows_to_delete:
        await store._raw_insert(
            ts_ms=_row_ts_ms(r),
            code=r["code"],
            event_id=r["event_id"],
            event_type=r["event_type"] or "",
            source=r["event_source"] or "",
            title=r["title"] or "",
            price=r["price"],
            qty=r["qty"],
            side=r["side"],
            content=r["content"] or "",
            content_external=r["content_external"] or "",
            author=r["author"] or "",
            deleted=True,
        )
    print(f"Soft-deleted {len(shadows_to_delete)} shadow rows.")
    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument(
        "--apply",
        action="store_true",
        help="Commit changes (omit for dry-run)",
    )
    args = p.parse_args()
    sys.exit(asyncio.run(main(apply=args.apply)))
