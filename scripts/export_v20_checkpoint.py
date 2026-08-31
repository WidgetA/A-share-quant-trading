"""Export an accepted V20 forward-shadow cut as an immutable checkpoint."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
import tempfile
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.data.database.v20_repository import canonical_json, create_v20_repository_from_config


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export a V20 v2 production bootstrap checkpoint from PostgreSQL."
    )
    parser.add_argument(
        "--database-config",
        type=Path,
        default=Path("config/database-config.yaml"),
    )
    parser.add_argument("--source-stream", required=True)
    parser.add_argument("--source-lineage", required=True)
    parser.add_argument("--target-stream", required=True)
    parser.add_argument("--target-lineage", required=True)
    parser.add_argument("--as-of", required=True, type=date.fromisoformat)
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def _write_immutable(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() == content:
            return
        raise FileExistsError(f"refusing to overwrite a different checkpoint: {path}")
    temporary_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
            temporary_name = handle.name
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary_name, path)
        except FileExistsError:
            if path.read_bytes() != content:
                raise FileExistsError(
                    f"another process created a different checkpoint: {path}"
                ) from None
    finally:
        if temporary_name is not None:
            Path(temporary_name).unlink(missing_ok=True)


async def _run(args: argparse.Namespace) -> tuple[Path, str, int]:
    repository = create_v20_repository_from_config(args.database_config)
    # Export must remain a read-only ledger operation. Production migrations
    # are an explicit deployment step, not a side effect of checkpointing.
    await repository.connect(migrate=False)
    try:
        checkpoint = await repository.export_bootstrap_checkpoint(
            source_official_stream_id=args.source_stream,
            source_lineage_id=args.source_lineage,
            target_official_stream_id=args.target_stream,
            target_lineage_id=args.target_lineage,
            as_of_trade_date=args.as_of,
        )
    finally:
        await repository.close()
    content = (canonical_json(checkpoint) + "\n").encode("utf-8")
    _write_immutable(args.output, content)
    return (
        args.output.resolve(),
        hashlib.sha256(content).hexdigest(),
        len(checkpoint["state_shadow_batches"]),
    )


def main() -> None:
    output, digest, batch_count = asyncio.run(_run(_arguments()))
    print(
        json.dumps(
            {
                "checkpoint_path": str(output),
                "checkpoint_sha256": digest,
                "state_shadow_batch_count": batch_count,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
