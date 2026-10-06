"""Select infrastructure releases from the complete GitHub push's Git range."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
from pathlib import Path

ZERO_SHA = "0" * 40
CROSS_MARKET_FILES = frozenset(
    {
        ".github/workflows/ci.yml",
        ".dockerignore",
        "Dockerfile",
        "pyproject.toml",
        "uv.lock",
        "scripts/cross_market_release_changes.py",
        "scripts/collect_cross_market_indices.py",
        "scripts/collect_cross_market_intraday.py",
        "src/__init__.py",
        "src/data/__init__.py",
        "src/data/yahoo_indices.py",
        "src/data/yahoo_intraday_indices.py",
        "src/data/fc_yahoo_worker.py",
        "src/data/fc_yahoo_indices.py",
        "src/data/fc_intraday_indices.py",
        "src/data/cross_market_store.py",
        "src/data/cross_market_ingest.py",
        "src/data/cross_market_intraday_ingest.py",
        "src/data/massive_indices.py",
        "src/data/cross_market_massive_ingest.py",
        "scripts/backfill_cross_market_massive.py",
    }
)
CROSS_MARKET_DIRECTORIES = (
    "deploy/cross-market/",
    "serverless/yahoo_indices/",
    "src/data/reference/cross_market/",
)


def classify_paths(paths: list[str]) -> dict[str, bool]:
    """Documentation does not change either deployed runtime."""
    runtime_paths = [path for path in paths if not path.lower().endswith((".md", ".rst"))]
    return {
        "cross_market": any(
            path in CROSS_MARKET_FILES or path.startswith(CROSS_MARKET_DIRECTORIES)
            for path in runtime_paths
        ),
        "training": any(
            path.startswith("serverless/") and not path.startswith("serverless/yahoo_indices/")
            for path in runtime_paths
        ),
    }


def _git(repository: Path, *arguments: str, check: bool = True) -> subprocess.CompletedProcess:
    result = subprocess.run(
        ["git", "-C", str(repository), *arguments],
        capture_output=True,
        check=False,
    )
    if check and result.returncode:
        # Do not print remote URLs, Git credential diagnostics or arbitrary stderr.
        raise RuntimeError("Cannot determine the complete push range")
    return result


def _ensure_commit(repository: Path, revision: str) -> None:
    if _git(repository, "cat-file", "-e", revision + "^{commit}", check=False).returncode:
        # A force-push's prior head may no longer be reachable in checkout's refs.
        _git(repository, "fetch", "--no-tags", "origin", revision)
        _git(repository, "cat-file", "-e", revision + "^{commit}")


def detect_release_changes(before: str, after: str, *, repository: Path = Path(".")) -> dict:
    if (
        any(not re.fullmatch("[0-9a-fA-F]{40}", sha) for sha in (before, after))
        or after == ZERO_SHA
    ):
        raise ValueError("Push revisions must be complete Git commit SHA1 values")
    before, after = before.lower(), after.lower()
    _ensure_commit(repository, after)
    first_push = before == ZERO_SHA
    if first_push:
        raw = _git(repository, "ls-tree", "-r", "--name-only", "-z", after).stdout
    else:
        _ensure_commit(repository, before)
        raw = _git(
            repository, "diff", "--no-ext-diff", "--no-renames", "--name-only", "-z", before, after
        ).stdout
    paths = [os.fsdecode(path) for path in raw.split(b"\0") if path]
    flags = classify_paths(paths)
    return {
        "before": before,
        "after": after,
        "first_push": first_push,
        "changed_count": len(paths),
        **flags,
        "cross_market": first_push or flags["cross_market"],
    }


def latest_branch_head(repository: Path, branch: str) -> str:
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9._/-]*", branch) or ".." in branch:
        raise ValueError("Invalid publication branch")
    ref = "refs/heads/" + branch
    raw = _git(repository, "ls-remote", "--exit-code", "origin", ref).stdout.decode()
    lines = [line.split() for line in raw.splitlines() if line.strip()]
    if len(lines) != 1 or len(lines[0]) != 2 or lines[0][1] != ref:
        raise RuntimeError("Cannot establish the current publication branch")
    revision = lines[0][0].lower()
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise RuntimeError("Cannot establish the current publication branch")
    return revision


def mutable_publication(repository: Path, revision: str, branch: str) -> dict:
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("Publication requires a complete commit SHA")
    head = latest_branch_head(repository, branch)
    return {"revision": revision, "branch_head": head, "publish_v15": revision == head}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before")
    parser.add_argument("--after")
    parser.add_argument("--publish-if-current")
    parser.add_argument("--branch", default="refactor/cleanup-v15-only")
    parser.add_argument("--repository", type=Path, default=Path("."))
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args(argv)
    if args.publish_if_current:
        if args.before or args.after:
            parser.error("Publication and push detection are separate operations")
        result = mutable_publication(args.repository, args.publish_if_current, args.branch)
        names = ("publish_v15",)
    else:
        if not args.before or not args.after:
            parser.error("Push detection requires before and after revisions")
        result = detect_release_changes(args.before, args.after, repository=args.repository)
        names = ("cross_market", "training")
    if args.github_output:
        with args.github_output.open("a", encoding="utf-8") as output:
            for name in names:
                output.write(name + "=" + str(result[name]).lower() + "\n")
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
