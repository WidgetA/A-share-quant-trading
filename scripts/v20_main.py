#!/usr/bin/env python3
"""Run the dedicated V20 ASGI process without loading platform trading code."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Sequence

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))


def _default_port() -> int:
    raw = os.getenv("V20_PORT", "8000")
    try:
        port = int(raw)
    except ValueError as exc:
        raise ValueError("V20_PORT must be an integer") from exc
    if not 1 <= port <= 65535:
        raise ValueError("V20_PORT must be between 1 and 65535")
    return port


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Dedicated V20 decision-notification host")
    parser.add_argument("--host", default=os.getenv("V20_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=_default_port())
    args = parser.parse_args(argv)
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")

    import uvicorn

    uvicorn.run("src.web.v20_app:app", host=args.host, port=args.port)


if __name__ == "__main__":
    main()
