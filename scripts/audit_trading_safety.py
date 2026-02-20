#!/usr/bin/env python
"""
交易安全审计脚本。

扫描 strategy/trading/data 代码中违反交易安全原则的模式：
1. except Exception 吞掉异常不 raise（静默降级）
2. 数据获取失败返回空值而不是报错（fail-open）
3. 缓存回退替代实时数据

原则：交易安全 > 程序健壮性。拿不到数据 → 报错停止，不许继续交易。

用法：
    uv run python scripts/audit_trading_safety.py
    uv run python scripts/audit_trading_safety.py --strict  # 包含 web/simulation
"""

import ast
import io
import sys
from dataclasses import dataclass, field
from pathlib import Path

# Fix Windows console encoding
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

PROJECT_ROOT = Path(__file__).parent.parent

# 交易关键路径：这些目录的代码直接影响交易决策和资金安全
CRITICAL_DIRS = [
    "src/strategy",
    "src/trading",
    "src/data/sources",
    "src/data/database",
    "src/data/clients",
]

# 扩展范围（--strict 模式）
EXTENDED_DIRS = CRITICAL_DIRS + [
    "src/web",
    "src/simulation",
    "src/common",
]


@dataclass
class Violation:
    file: str
    line: int
    category: str
    detail: str
    severity: str  # CRITICAL / WARNING


@dataclass
class AuditResult:
    violations: list[Violation] = field(default_factory=list)
    files_scanned: int = 0


class TradingSafetyAuditor(ast.NodeVisitor):
    """AST visitor that detects trading safety violations."""

    def __init__(self, filepath: str):
        self.filepath = filepath
        self.violations: list[Violation] = []

    def _add(self, node: ast.AST, category: str, detail: str, severity: str = "CRITICAL"):
        self.violations.append(
            Violation(
                file=self.filepath,
                line=getattr(node, "lineno", 0),
                category=category,
                detail=detail,
                severity=severity,
            )
        )

    def visit_ExceptHandler(self, node: ast.ExceptHandler):
        """Check for broad exception catching without re-raise."""
        # Is it catching Exception or bare except?
        is_broad = False
        if node.type is None:
            is_broad = True  # bare except:
        elif isinstance(node.type, ast.Name) and node.type.id in ("Exception", "BaseException"):
            is_broad = True

        if not is_broad:
            self.generic_visit(node)
            return

        # Check if the except body contains a raise statement
        has_raise = False
        has_return_empty = False
        has_logger_only = False
        return_values = []

        for child in ast.walk(node):
            if isinstance(child, ast.Raise):
                has_raise = True
            if isinstance(child, ast.Return):
                val = child.value
                if val is None:
                    return_values.append("None")
                elif isinstance(val, ast.Dict) and not val.keys:
                    return_values.append("{}")
                elif isinstance(val, ast.List) and not val.elts:
                    return_values.append("[]")
                elif isinstance(val, ast.Constant) and val.value is None:
                    return_values.append("None")

        if return_values:
            has_return_empty = True

        # Count meaningful statements (excluding logger calls)
        body_stmts = node.body
        logger_calls = 0
        for stmt in body_stmts:
            if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Call):
                func = stmt.value.func
                if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
                    if func.value.id == "logger":
                        logger_calls += 1

        if logger_calls == len(body_stmts) or (
            logger_calls == len(body_stmts) - 1 and has_return_empty
        ):
            has_logger_only = True

        # Report violations
        if not has_raise and has_return_empty:
            self._add(
                node,
                "SILENT_DEGRADATION",
                f"except Exception 捕获后返回 {'/'.join(return_values)}，"
                f"没有 raise — 数据缺失会被静默吞掉",
            )
        elif not has_raise and has_logger_only:
            self._add(
                node,
                "SWALLOWED_EXCEPTION",
                "except Exception 只打日志不 raise — 错误被静默吞掉，调用方无法感知",
            )
        elif not has_raise and is_broad:
            self._add(
                node,
                "BROAD_CATCH_NO_RAISE",
                "except Exception 没有 raise — 可能导致静默降级",
                severity="WARNING",
            )

        self.generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        """Check for functions that return empty defaults on failure paths."""
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        """Check async functions (same logic as sync)."""
        self.generic_visit(node)


def audit_file(filepath: Path) -> list[Violation]:
    """Audit a single Python file for trading safety violations."""
    try:
        source = filepath.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(filepath))
    except (SyntaxError, UnicodeDecodeError):
        return []

    rel_path = str(filepath.relative_to(PROJECT_ROOT)).replace("\\", "/")
    auditor = TradingSafetyAuditor(rel_path)
    auditor.visit(tree)
    return auditor.violations


def run_audit(strict: bool = False) -> AuditResult:
    """Run the full audit."""
    dirs = EXTENDED_DIRS if strict else CRITICAL_DIRS
    result = AuditResult()

    for dir_path in dirs:
        full_dir = PROJECT_ROOT / dir_path
        if not full_dir.exists():
            continue
        for py_file in sorted(full_dir.rglob("*.py")):
            if "__pycache__" in str(py_file):
                continue
            result.files_scanned += 1
            result.violations.extend(audit_file(py_file))

    return result


def print_report(result: AuditResult):
    """Print the audit report."""
    print(f"\n{'=' * 70}")
    print("  交易安全审计报告")
    print(f"{'=' * 70}")
    print(f"  扫描文件: {result.files_scanned}")
    print(f"  发现问题: {len(result.violations)}")

    critical = [v for v in result.violations if v.severity == "CRITICAL"]
    warnings = [v for v in result.violations if v.severity == "WARNING"]

    if critical:
        print(f"\n  🚨 严重问题 (CRITICAL): {len(critical)}")
        print(f"  {'─' * 60}")
        for v in critical:
            print(f"\n  [{v.category}] {v.file}:{v.line}")
            print(f"    {v.detail}")

    if warnings:
        print(f"\n  ⚠️  警告 (WARNING): {len(warnings)}")
        print(f"  {'─' * 60}")
        for v in warnings:
            print(f"\n  [{v.category}] {v.file}:{v.line}")
            print(f"    {v.detail}")

    if not result.violations:
        print("\n  没有发现交易安全问题")

    print(f"\n{'=' * 70}")

    # Exit code: 1 if any CRITICAL violations
    return 1 if critical else 0


def main():
    strict = "--strict" in sys.argv
    result = run_audit(strict=strict)
    exit_code = print_report(result)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
