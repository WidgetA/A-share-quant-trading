"""Prepare and audit offline V16 theme-normalization proposals.

The experiment never invokes an LLM implicitly.  ``prepare`` creates the same strict
protocol with a hash-bound Codex, Kimi, or human identity; the explicit ``invoke``
command runs Codex or Kimi in an isolated temporary directory and captures stdout.
``validate`` and ``audit`` reject anything outside the protocol.  This keeps
credentials out of artifacts and prevents unvalidated free text from entering a
trading runtime.

Examples::

    uv run python scripts/experiment_v16_theme_semantics.py probe
    uv run python scripts/experiment_v16_theme_semantics.py prepare \
        --boards boards.json --evidence evidence.json \
        --taxonomy config/v16_theme_taxonomy.example.json \
        --provider codex --model gpt-5 \
        --request-out /tmp/v16-theme-request.json \
        --prompt-out /tmp/v16-theme-prompt.txt
    uv run python scripts/experiment_v16_theme_semantics.py validate \
        --request /tmp/v16-theme-request.json \
        --response /tmp/codex-response.json
    uv run python scripts/experiment_v16_theme_semantics.py audit \
        --request /tmp/v16-theme-request.json \
        --left /tmp/codex-response.json --right /tmp/kimi-response.json \
        --out /tmp/v16-theme-audit.json

Input files are JSON.  ``boards.json`` is either a string array or
``{"raw_boards": [...]}``.  The optional evidence document has exactly these keys::

    {
      "stocks": {"000001": ["board A", "board B"]},
      "cooccurrences": [
        {"board_a": "board A", "board_b": "board B", "count": 3}
      ]
    }
"""

from __future__ import annotations

import argparse
import hashlib
import re
import shutil
import subprocess
import sys
from collections.abc import Mapping
from itertools import combinations
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from src.strategy.v16_theme_semantics import (  # noqa: E402
    DISAGREEMENT_AUDIT_SCHEMA,
    NORMALIZATION_REQUEST_SCHEMA,
    NORMALIZATION_RESPONSE_SCHEMA,
    PROTOCOL_HASH,
    PROTOCOL_VERSION,
    ThemeSemanticsValidationError,
    audit_normalization_responses,
    build_consensus_candidate_taxonomy,
    build_normalization_prompt,
    build_normalization_request,
    canonical_json,
    canonicalize_normalization_response,
    normalization_response_sha256,
    parse_json_strict,
    prompt_sha256,
    validate_normalization_request,
    validate_normalization_response,
    validate_taxonomy,
)

_SAFE_VERSION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 ._+/-]{0,119}$")


def _read_json(path: Path) -> Any:
    try:
        return parse_json_strict(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ThemeSemanticsValidationError(f"cannot read {path}: {exc.strerror}") from exc


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", newline="\n")


def _emit_json(value: Any, path: Path | None = None) -> None:
    text = f"{canonical_json(value)}\n"
    if path is None:
        sys.stdout.write(text)
    else:
        _write_text(path, text)


def _load_raw_boards(path: Path) -> list[str]:
    value = _read_json(path)
    if isinstance(value, list):
        return value
    if isinstance(value, Mapping) and set(value) == {"raw_boards"}:
        boards = value["raw_boards"]
        if isinstance(boards, list):
            return boards
    raise ThemeSemanticsValidationError(
        "boards input must be a JSON string array or exactly {'raw_boards': [...]}"
    )


def _load_evidence(path: Path | None) -> tuple[dict[str, list[str]], list[tuple[str, str, int]]]:
    if path is None:
        return {}, []
    value = _read_json(path)
    if not isinstance(value, Mapping) or set(value) != {"stocks", "cooccurrences"}:
        raise ThemeSemanticsValidationError(
            "evidence must contain exactly 'stocks' and 'cooccurrences'"
        )
    stocks = value["stocks"]
    if not isinstance(stocks, Mapping):
        raise ThemeSemanticsValidationError("evidence.stocks must be an object")
    stock_boards: dict[str, list[str]] = {}
    for code, boards in stocks.items():
        if not isinstance(code, str) or not isinstance(boards, list):
            raise ThemeSemanticsValidationError(
                "evidence.stocks must map string codes to board-name arrays"
            )
        stock_boards[code] = boards

    rows = value["cooccurrences"]
    if not isinstance(rows, list):
        raise ThemeSemanticsValidationError("evidence.cooccurrences must be an array")
    cooccurrences: list[tuple[str, str, int]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping) or set(row) != {"board_a", "board_b", "count"}:
            raise ThemeSemanticsValidationError(
                f"evidence.cooccurrences[{index}] has an invalid shape"
            )
        cooccurrences.append((row["board_a"], row["board_b"], row["count"]))
    return stock_boards, cooccurrences


def _probe_one(name: str) -> dict[str, Any]:
    executable = shutil.which(f"{name}.cmd") or shutil.which(name)
    if executable is None:
        return {"name": name, "available": False, "version": None}
    command = [executable, "--version"]
    if Path(executable).suffix.lower() == ".ps1":
        powershell = shutil.which("pwsh") or shutil.which("powershell")
        if powershell is None:
            return {"name": name, "available": True, "version": None}
        command = [powershell, "-NoProfile", "-File", executable, "--version"]
    try:
        completed = subprocess.run(
            command,
            check=False,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return {"name": name, "available": True, "version": None}
    lines = [line.strip() for line in (completed.stdout or completed.stderr).splitlines()]
    safe_line = next((line for line in lines if _SAFE_VERSION_RE.fullmatch(line)), None)
    return {"name": name, "available": True, "version": safe_line}


def _provider_executable(provider: str) -> list[str]:
    """Resolve an npm CLI without routing the prompt through a command shell."""

    executable = shutil.which(f"{provider}.cmd") or shutil.which(provider)
    if executable is None:
        raise ThemeSemanticsValidationError(f"{provider} CLI is not available")
    if sys.platform != "win32":
        return [executable]

    npm_root = Path(executable).resolve().parent
    node = npm_root / "node.exe"
    if not node.exists():
        node_path = shutil.which("node")
        if node_path is None:
            raise ThemeSemanticsValidationError("node executable for npm CLI was not found")
        node = Path(node_path)
    package_entry = {
        "codex": npm_root / "node_modules" / "@openai" / "codex" / "bin" / "codex.js",
        "kimi": (npm_root / "node_modules" / "@moonshot-ai" / "kimi-code" / "dist" / "main.mjs"),
    }[provider]
    if not package_entry.exists():
        raise ThemeSemanticsValidationError(f"{provider} npm entry point was not found")
    return [str(node), str(package_entry)]


def _command_probe(_args: argparse.Namespace) -> None:
    # Only --version is executed.  No prompt, environment value, credential, or CLI path is emitted.
    _emit_json({"tools": [_probe_one("codex"), _probe_one("kimi")]})


def _command_schema(_args: argparse.Namespace) -> None:
    _emit_json(
        {
            "schema_version": PROTOCOL_VERSION,
            "protocol_hash": PROTOCOL_HASH,
            "request_schema": NORMALIZATION_REQUEST_SCHEMA,
            "response_schema": NORMALIZATION_RESPONSE_SCHEMA,
            "audit_schema": DISAGREEMENT_AUDIT_SCHEMA,
        }
    )


def _command_invoke(args: argparse.Namespace) -> None:
    """Opt-in isolated CLI invocation; it never grants either Agent repository access."""

    try:
        prompt = args.prompt.read_text(encoding="utf-8")
    except OSError as exc:
        raise ThemeSemanticsValidationError(f"cannot read {args.prompt}: {exc.strerror}") from exc
    prompt_match = re.search(r"^PROMPT_PAYLOAD_SHA256=([0-9a-f]{64})$", prompt, re.MULTILINE)
    if prompt_match is None:
        raise ThemeSemanticsValidationError("prompt has no declared payload hash")

    command = _provider_executable(args.provider)
    stdin_text: str | None = None
    if args.provider == "codex":
        command.extend(
            [
                "exec",
                "--ephemeral",
                "--ignore-user-config",
                "--ignore-rules",
                "--skip-git-repo-check",
                "--sandbox",
                "read-only",
                "-m",
                args.model,
                "--color",
                "never",
                "-",
            ]
        )
        stdin_text = prompt
    else:
        command.extend(
            [
                "--model",
                args.model,
                "--prompt",
                prompt,
                "--output-format",
                "text",
            ]
        )

    stdout = ""
    stderr = ""
    exit_code: int | None = None
    timed_out = False
    with TemporaryDirectory(prefix=f"v16-theme-{args.provider}-") as isolated:
        try:
            completed = subprocess.run(
                command,
                cwd=isolated,
                input=stdin_text,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=args.timeout,
                check=False,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            stdout = completed.stdout
            stderr = completed.stderr
            exit_code = completed.returncode
        except subprocess.TimeoutExpired as exc:
            timed_out = True
            if isinstance(exc.stdout, bytes):
                stdout = exc.stdout.decode("utf-8", errors="replace")
            else:
                stdout = exc.stdout or ""
            if isinstance(exc.stderr, bytes):
                stderr = exc.stderr.decode("utf-8", errors="replace")
            else:
                stderr = exc.stderr or ""

    _write_text(args.raw_out, stdout)
    report = {
        "provider": args.provider,
        "model": args.model,
        "declared_prompt_hash": prompt_match.group(1),
        "prompt_file_hash": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
        "exit_code": exit_code,
        "timed_out": timed_out,
        "stdout_bytes": len(stdout.encode("utf-8")),
        "stdout_hash": hashlib.sha256(stdout.encode("utf-8")).hexdigest(),
        "stderr_bytes": len(stderr.encode("utf-8")),
        "stderr_hash": hashlib.sha256(stderr.encode("utf-8")).hexdigest(),
        "raw_out": str(args.raw_out),
    }
    _emit_json(report, args.report_out)
    _emit_json(report)
    if timed_out or exit_code != 0:
        raise ThemeSemanticsValidationError(
            f"{args.provider} invocation failed; inspect the sanitized report and raw stdout"
        )


def _command_prepare(args: argparse.Namespace) -> None:
    raw_boards = _load_raw_boards(args.boards)
    stock_boards, cooccurrences = _load_evidence(args.evidence)
    taxonomy: Mapping[str, Any] | None = None
    if args.taxonomy is not None:
        taxonomy_value = _read_json(args.taxonomy)
        validate_taxonomy(taxonomy_value)
        taxonomy = taxonomy_value
    request = build_normalization_request(
        raw_boards,
        stock_boards=stock_boards,
        cooccurrences=cooccurrences,
        taxonomy=taxonomy,
        allow_new_themes=args.allow_new_themes,
    )
    prompt = build_normalization_prompt(request, provider=args.provider, model=args.model)
    _emit_json(request, args.request_out)
    _write_text(args.prompt_out, prompt)
    _emit_json(
        {
            "schema_version": PROTOCOL_VERSION,
            "protocol_hash": PROTOCOL_HASH,
            "request_hash": request["request_hash"],
            "taxonomy_hash": request["taxonomy_hash"],
            "provider": args.provider,
            "model": args.model,
            "prompt_hash": prompt_sha256(request, provider=args.provider, model=args.model),
            "request_out": str(args.request_out),
            "prompt_out": str(args.prompt_out),
        }
    )


def _command_validate(args: argparse.Namespace) -> None:
    request = _read_json(args.request)
    response = _read_json(args.response)
    validate_normalization_request(request)
    validate_normalization_response(response, request)
    report = {
        "valid": True,
        "candidate_status": "validated_not_approved",
        "provider": response["provider"],
        "model": response["model"],
        "protocol_hash": PROTOCOL_HASH,
        "request_hash": request["request_hash"],
        "prompt_hash": response["prompt_hash"],
        "response_hash": normalization_response_sha256(response, request),
    }
    _emit_json(report, args.out)
    if args.out is not None:
        _emit_json(report)


def _command_extract(args: argparse.Namespace) -> None:
    """Remove only a known CLI display frame; preserve the raw stdout separately."""

    try:
        raw_text = args.raw_response.read_text(encoding="utf-8")
    except OSError as exc:
        raise ThemeSemanticsValidationError(
            f"cannot read {args.raw_response}: {exc.strerror}"
        ) from exc
    stripped = raw_text.strip()
    framing = "none"
    if args.provider == "kimi" and stripped.startswith("• "):
        stripped = stripped[2:].lstrip()
        framing = "kimi_text_bullet"
    value = parse_json_strict(stripped)
    if not isinstance(value, Mapping):
        raise ThemeSemanticsValidationError("extracted provider response must be one JSON object")
    _emit_json(value, args.out)
    _emit_json(
        {
            "candidate_status": "extracted_not_validated",
            "provider": args.provider,
            "framing_removed": framing,
            "raw_stdout_hash": hashlib.sha256(raw_text.encode("utf-8")).hexdigest(),
            "extracted_json_hash": canonical_json_hash(value),
            "out": str(args.out),
        },
        args.report_out,
    )


def _command_canonicalize(args: argparse.Namespace) -> None:
    request = _read_json(args.request)
    raw_response = _read_json(args.response)
    candidate = canonicalize_normalization_response(raw_response, request)
    _emit_json(candidate, args.out)
    _emit_json(
        {
            "candidate_status": "validated_not_approved",
            "raw_response_hash": canonical_json_hash(raw_response),
            "canonical_response_hash": normalization_response_sha256(candidate, request),
            "out": str(args.out),
        }
    )


def canonical_json_hash(value: Any) -> str:
    """Hash a raw JSON value without implying that it passed protocol validation."""

    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _command_audit(args: argparse.Namespace) -> None:
    request = _read_json(args.request)
    left = _read_json(args.left)
    right = _read_json(args.right)
    audit = audit_normalization_responses(left, right, request)
    _emit_json(audit, args.out)
    if args.out is not None:
        _emit_json(
            {
                "audit_hash": audit["audit_hash"],
                "semantic_agreement": audit["semantic_agreement"],
                "exact_agreement": audit["exact_agreement"],
                "disagreement_count": len(audit["disagreements"]),
                "out": str(args.out),
            }
        )


def _command_consensus(args: argparse.Namespace) -> None:
    request = _read_json(args.request)
    left = _read_json(args.left)
    right = _read_json(args.right)
    taxonomy, manifest = build_consensus_candidate_taxonomy(left, right, request)
    _emit_json(taxonomy, args.taxonomy_out)
    _emit_json(manifest, args.manifest_out)
    _emit_json(
        {
            "artifact_status": manifest["artifact_status"],
            "runtime_load_allowed": manifest["runtime_load_allowed"],
            "taxonomy_hash": manifest["taxonomy_hash"],
            "manifest_hash": manifest["manifest_hash"],
            "consensus_alias_count": len(manifest["consensus_aliases"]),
            "manual_review_count": len(manifest["manual_review"]),
            "taxonomy_out": str(args.taxonomy_out),
            "manifest_out": str(args.manifest_out),
        }
    )


def _theme_by_board(response: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {alias: theme for theme in response["themes"] for alias in theme["aliases"]}


def _same_cluster_pairs(response: Mapping[str, Any]) -> set[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    for theme in response["themes"]:
        pairs.update(combinations(theme["aliases"], 2))
    return pairs


def _load_focus_groups(path: Path | None) -> list[dict[str, Any]]:
    if path is None:
        return []
    value = _read_json(path)
    if not isinstance(value, Mapping) or set(value) != {"groups"}:
        raise ThemeSemanticsValidationError("focus groups must contain exactly 'groups'")
    groups = value["groups"]
    if not isinstance(groups, list):
        raise ThemeSemanticsValidationError("focus groups.groups must be an array")
    parsed: list[dict[str, Any]] = []
    for index, group in enumerate(groups):
        if not isinstance(group, Mapping) or set(group) != {"name", "raw_boards"}:
            raise ThemeSemanticsValidationError(f"focus groups.groups[{index}] has invalid keys")
        if not isinstance(group["name"], str) or not isinstance(group["raw_boards"], list):
            raise ThemeSemanticsValidationError(f"focus groups.groups[{index}] has invalid types")
        if not all(isinstance(board, str) for board in group["raw_boards"]):
            raise ThemeSemanticsValidationError(
                f"focus groups.groups[{index}].raw_boards must contain strings"
            )
        parsed.append({"name": group["name"], "raw_boards": group["raw_boards"]})
    return parsed


def _command_compare(args: argparse.Namespace) -> None:
    request = _read_json(args.request)
    left = _read_json(args.left)
    right = _read_json(args.right)
    validate_normalization_response(left, request)
    validate_normalization_response(right, request)
    left_map = _theme_by_board(left)
    right_map = _theme_by_board(right)
    boards = request["raw_boards"]
    board_set = set(boards)
    universe_pairs = set(combinations(boards, 2))
    left_pairs = _same_cluster_pairs(left)
    right_pairs = _same_cluster_pairs(right)
    intersection = left_pairs & right_pairs
    union = left_pairs | right_pairs
    symmetric_difference = left_pairs ^ right_pairs
    label_agreements = [
        board for board in boards if left_map[board]["label"] == right_map[board]["label"]
    ]
    label_disagreements = [
        {
            "raw_board": board,
            "left_label": left_map[board]["label"],
            "right_label": right_map[board]["label"],
        }
        for board in boards
        if left_map[board]["label"] != right_map[board]["label"]
    ]

    focus_groups = _load_focus_groups(args.focus_groups)
    focus_board_names = sorted({board for group in focus_groups for board in group["raw_boards"]})
    focus_boards: list[dict[str, Any]] = []
    for board in focus_board_names:
        if board not in board_set:
            focus_boards.append({"raw_board": board, "in_request": False})
            continue
        left_theme = left_map[board]
        right_theme = right_map[board]
        focus_boards.append(
            {
                "raw_board": board,
                "in_request": True,
                "label_agreement": left_theme["label"] == right_theme["label"],
                "exact_alias_partition_agreement": left_theme["aliases"] == right_theme["aliases"],
                "left": {
                    "canonical_theme_id": left_theme["canonical_theme_id"],
                    "label": left_theme["label"],
                    "aliases": left_theme["aliases"],
                },
                "right": {
                    "canonical_theme_id": right_theme["canonical_theme_id"],
                    "label": right_theme["label"],
                    "aliases": right_theme["aliases"],
                },
            }
        )

    group_results: list[dict[str, Any]] = []
    for group in focus_groups:
        requested = group["raw_boards"]
        covered = [board for board in requested if board in board_set]
        missing = [board for board in requested if board not in board_set]
        covered_pairs = set(combinations(sorted(covered), 2))
        relation_agreements = covered_pairs - symmetric_difference
        group_results.append(
            {
                "name": group["name"],
                "requested_boards": requested,
                "covered_boards": covered,
                "missing_boards": missing,
                "covered_pair_count": len(covered_pairs),
                "same_cluster_relation_agreement_count": len(relation_agreements),
                "same_cluster_relation_agreement_rate": (
                    len(relation_agreements) / len(covered_pairs) if covered_pairs else None
                ),
                "left_all_covered_boards_same_cluster": (
                    covered_pairs.issubset(left_pairs) if covered_pairs else None
                ),
                "right_all_covered_boards_same_cluster": (
                    covered_pairs.issubset(right_pairs) if covered_pairs else None
                ),
                "left_assignments": {
                    board: left_map[board]["canonical_theme_id"] for board in covered
                },
                "right_assignments": {
                    board: right_map[board]["canonical_theme_id"] for board in covered
                },
            }
        )

    summary_payload = {
        "schema_version": "v16-theme-semantics-comparison/1.0",
        "artifact_status": "candidate/not_approved",
        "protocol_hash": PROTOCOL_HASH,
        "request_hash": request["request_hash"],
        "left": {
            "provider": left["provider"],
            "model": left["model"],
            "response_hash": normalization_response_sha256(left, request),
        },
        "right": {
            "provider": right["provider"],
            "model": right["model"],
            "response_hash": normalization_response_sha256(right, request),
        },
        "board_count": len(boards),
        "label_agreement_count": len(label_agreements),
        "label_agreement_rate": len(label_agreements) / len(boards),
        "label_agreements": label_agreements,
        "label_disagreements": label_disagreements,
        "same_cluster_pairs": {
            "universe_pair_count": len(universe_pairs),
            "left_pair_count": len(left_pairs),
            "right_pair_count": len(right_pairs),
            "intersection_count": len(intersection),
            "union_count": len(union),
            "jaccard": len(intersection) / len(union) if union else 1.0,
            "partition_relation_agreement_count": len(universe_pairs) - len(symmetric_difference),
            "partition_relation_agreement_rate": (
                (len(universe_pairs) - len(symmetric_difference)) / len(universe_pairs)
                if universe_pairs
                else 1.0
            ),
            "left_only_pairs": [list(pair) for pair in sorted(left_pairs - right_pairs)],
            "right_only_pairs": [list(pair) for pair in sorted(right_pairs - left_pairs)],
        },
        "focus_boards": focus_boards,
        "focus_groups": group_results,
    }
    summary = {**summary_payload, "summary_hash": canonical_json_hash(summary_payload)}
    _emit_json(summary, args.out)
    _emit_json(
        {
            "label_agreement_count": summary["label_agreement_count"],
            "same_cluster_pair_jaccard": summary["same_cluster_pairs"]["jaccard"],
            "partition_relation_agreement_rate": summary["same_cluster_pairs"][
                "partition_relation_agreement_rate"
            ],
            "summary_hash": summary["summary_hash"],
            "out": str(args.out),
        }
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    probe = subparsers.add_parser("probe", help="check local CLI availability using --version only")
    probe.set_defaults(func=_command_probe)

    schema = subparsers.add_parser("schema", help="print protocol hashes and strict JSON schemas")
    schema.set_defaults(func=_command_schema)

    invoke = subparsers.add_parser(
        "invoke",
        help="run one provider in an isolated temporary directory and capture stdout",
    )
    invoke.add_argument("--provider", choices=("codex", "kimi"), required=True)
    invoke.add_argument("--model", required=True)
    invoke.add_argument("--prompt", type=Path, required=True)
    invoke.add_argument("--raw-out", type=Path, required=True)
    invoke.add_argument("--report-out", type=Path, required=True)
    invoke.add_argument("--timeout", type=int, default=300)
    invoke.set_defaults(func=_command_invoke)

    prepare = subparsers.add_parser("prepare", help="build a hashed request and neutral prompt")
    prepare.add_argument("--boards", type=Path, required=True)
    prepare.add_argument("--evidence", type=Path)
    prepare.add_argument("--taxonomy", type=Path)
    prepare.add_argument("--provider", choices=("codex", "kimi", "human"), required=True)
    prepare.add_argument(
        "--model", required=True, help="stable CLI model name or human reviewer role"
    )
    prepare.add_argument(
        "--allow-new-themes",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="allow canonical IDs not already present in the supplied taxonomy",
    )
    prepare.add_argument("--request-out", type=Path, required=True)
    prepare.add_argument("--prompt-out", type=Path, required=True)
    prepare.set_defaults(func=_command_prepare)

    validate = subparsers.add_parser("validate", help="strictly validate one proposal")
    validate.add_argument("--request", type=Path, required=True)
    validate.add_argument("--response", type=Path, required=True)
    validate.add_argument("--out", type=Path)
    validate.set_defaults(func=_command_validate)

    extract = subparsers.add_parser(
        "extract",
        help="remove only a known CLI stdout frame and emit the untouched JSON value",
    )
    extract.add_argument("--provider", choices=("codex", "kimi"), required=True)
    extract.add_argument("--raw-response", type=Path, required=True)
    extract.add_argument("--out", type=Path, required=True)
    extract.add_argument("--report-out", type=Path, required=True)
    extract.set_defaults(func=_command_extract)

    canonicalize = subparsers.add_parser(
        "canonicalize",
        help="mechanically sort a raw proposal and emit a validated candidate",
    )
    canonicalize.add_argument("--request", type=Path, required=True)
    canonicalize.add_argument("--response", type=Path, required=True)
    canonicalize.add_argument("--out", type=Path, required=True)
    canonicalize.set_defaults(func=_command_canonicalize)

    audit = subparsers.add_parser("audit", help="audit two independently validated proposals")
    audit.add_argument("--request", type=Path, required=True)
    audit.add_argument("--left", type=Path, required=True)
    audit.add_argument("--right", type=Path, required=True)
    audit.add_argument("--out", type=Path)
    audit.set_defaults(func=_command_audit)

    consensus = subparsers.add_parser(
        "consensus",
        help="emit a not-approved taxonomy containing exact semantic agreement only",
    )
    consensus.add_argument("--request", type=Path, required=True)
    consensus.add_argument("--left", type=Path, required=True)
    consensus.add_argument("--right", type=Path, required=True)
    consensus.add_argument("--taxonomy-out", type=Path, required=True)
    consensus.add_argument("--manifest-out", type=Path, required=True)
    consensus.set_defaults(func=_command_consensus)

    compare = subparsers.add_parser(
        "compare",
        help="compare labels and alias partitions without relying on canonical ID spelling",
    )
    compare.add_argument("--request", type=Path, required=True)
    compare.add_argument("--left", type=Path, required=True)
    compare.add_argument("--right", type=Path, required=True)
    compare.add_argument("--focus-groups", type=Path)
    compare.add_argument("--out", type=Path, required=True)
    compare.set_defaults(func=_command_compare)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        args.func(args)
    except ThemeSemanticsValidationError as exc:
        print(f"validation error: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
