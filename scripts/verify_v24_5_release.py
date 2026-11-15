"""Read-only deployment evidence; never triggers selection or sends a message.

HTTP mode performs one GET /api/status. A saved manual-task response may be
assessed separately; service health and closed-day skip are not trading-day
acceptance. No credentials or unrestricted service payload are written.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import urlopen


def assess_status(payload: dict, expected_commit: str, expected_strategy: str) -> dict:
    if not re.fullmatch(r"[0-9a-f]{40}", expected_commit):
        raise ValueError("expected commit must be a complete lowercase Git SHA")
    runtime = payload.get("v20") or {}
    checks = {
        "http_status_ok": payload.get("status") == "ok",
        "exact_commit": payload.get("git_commit") == expected_commit,
        "exact_strategy": runtime.get("strategy_version") == expected_strategy,
        "configured": runtime.get("configured") is True,
        "enabled": runtime.get("enabled") is True,
        "started": runtime.get("started") is True,
        "healthy": runtime.get("healthy") is True,
        "running": runtime.get("running") is True,
        "not_recovering": runtime.get("retrying") is False,
        "no_start_error": not runtime.get("start_error_code")
        and not runtime.get("start_error_type"),
    }
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "observed": {
            "git_commit": payload.get("git_commit"),
            "git_branch": payload.get("git_branch"),
            "build_time": payload.get("build_time"),
            "strategy_version": runtime.get("strategy_version"),
            "mode": runtime.get("mode"),
            "startup_stage": runtime.get("startup_stage"),
        },
        "proves": "deployed version and runtime health only",
    }


def assess_task(payload: dict, expected_date: str, expected_strategy: str) -> dict:
    date.fromisoformat(expected_date)
    common = {
        "exact_current_date": payload.get("trade_date") == expected_date,
        "exact_strategy": payload.get("selection_version") == expected_strategy,
        "notification_only": payload.get("orders_changed") is False,
    }
    if payload.get("cycle_result") == "NON_TRADING_DAY":
        checks = {
            **common,
            "no_calculation": payload.get("calculation_result") == "NOT_RUN",
            "no_delivery": payload.get("delivery_status") == "NOT_REQUIRED",
            "no_tickets": payload.get("symbols") == [] and payload.get("reference_symbols") == [],
            "no_state_advance": payload.get("official_state_changed") is False,
            "not_reported_as_trading_success": payload.get("task_success") is False,
        }
        return {
            "kind": "non_trading_day_skip",
            "passed": all(checks.values()),
            "checks": checks,
            "trading_day_acceptance": False,
        }
    checks = {
        **common,
        "calculated": payload.get("calculation_result") == "SUCCESS",
        "persisted_event_identity": bool(payload.get("entry_event_id")),
        "current_not_retrospective": payload.get("retrospective_expired") is False,
        "shared_message": payload.get("exact_automatic_message") is True,
        "selection_delivered": payload.get("delivery_status") == "SENT",
        "timing_event_identity": bool(payload.get("entry_timing_alert_event_id")),
        "timing_delivered": payload.get("entry_timing_alert_delivery_status") == "SENT",
        "receipt_confirmed": payload.get("feishu_delivery_confirmed") is True,
        "task_success": payload.get("task_success") is True,
    }
    return {
        "kind": "trading_day_task_response",
        "passed": all(checks.values()),
        "checks": checks,
        "trading_day_acceptance": all(checks.values()),
        "limitation": "Bind event IDs to persisted result/outbox and actual deployed commit too.",
    }


def _load(path: Path) -> tuple[dict, str]:
    raw = path.read_bytes()
    return json.loads(raw.decode("utf-8-sig")), hashlib.sha256(raw).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--status-file", type=Path)
    source.add_argument("--status-url")
    parser.add_argument("--expected-commit", required=True)
    parser.add_argument("--expected-strategy", default="V24.5")
    parser.add_argument("--task-file", type=Path)
    parser.add_argument("--expected-date")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.task_file and not args.expected_date:
        parser.error("--task-file requires the actual --expected-date")
    if args.status_file:
        status, status_sha = _load(args.status_file)
    else:
        url = urlsplit(args.status_url)
        if (
            url.scheme not in {"http", "https"}
            or url.username
            or url.password
            or url.path != "/api/status"
            or url.query
            or url.fragment
        ):
            parser.error("status URL must be a credential-free GET /api/status")
        with urlopen(args.status_url, timeout=15) as response:
            raw = response.read()
        status = json.loads(raw)
        status_sha = hashlib.sha256(raw).hexdigest()
    report = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "read_only": True,
        "status_sha256": status_sha,
        "status": assess_status(status, args.expected_commit, args.expected_strategy),
        "task": None,
        "trading_day_acceptance": False,
    }
    if args.task_file:
        task, task_sha = _load(args.task_file)
        report["task_sha256"] = task_sha
        report["task"] = assess_task(task, args.expected_date, args.expected_strategy)
        report["trading_day_acceptance"] = (
            report["status"]["passed"] and report["task"]["trading_day_acceptance"]
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return (
        0 if report["status"]["passed"] and (not report["task"] or report["task"]["passed"]) else 1
    )


if __name__ == "__main__":
    raise SystemExit(main())
