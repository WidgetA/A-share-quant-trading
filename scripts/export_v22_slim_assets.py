"""One-time, offline publication of the user-approved frozen V22 assets."""

import argparse
import hashlib
import json
import subprocess
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--research-workspace", type=Path, required=True)
    args = parser.parse_args()
    source = args.research_workspace.resolve()
    target = Path(__file__).resolve().parents[1] / "models" / "v22_slim"
    target.mkdir(parents=True, exist_ok=True)
    sources = {
        "lgbrank_latest.txt": (source / "dev-tools/models/lgbrank_latest.txt").read_bytes(),
        "feature_list.json": (source / "dev-tools/models/feature_list.json").read_bytes(),
        "board_constituents.json": subprocess.run(
            [
                "git",
                "-C",
                str(source),
                "show",
                "11b9d0fc20fd059d644a54b1aed1a6374f244012:data/board_constituents.json",
            ],
            check=True,
            capture_output=True,
        ).stdout,
    }
    if (
        hashlib.sha256(sources["lgbrank_latest.txt"]).hexdigest()
        != "55b6c1eb6afe9b642893fcdad2d073cb8851e73914592ee7d95946e06da82525"
    ):
        raise ValueError("ranking model differs from the frozen research model")
    for name, content in sources.items():
        (target / name).write_bytes(content)
    manifest = {
        "schema": "v22-slim-assets/v1",
        "version": "V22-slim",
        "frozen_date": "2026-09-07",
        "board_effective_from": "2026-07-10",
        "sha256": {name: hashlib.sha256(content).hexdigest() for name, content in sources.items()},
    }
    (target / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(manifest))


if __name__ == "__main__":
    main()
