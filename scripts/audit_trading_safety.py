#!/usr/bin/env python
"""
交易安全审计脚本。

扫描 strategy/trading/data 代码中违反交易安全原则的模式：

== Level 1: 语法级别 ==
1. except Exception 吞掉异常不 raise（静默降级）
2. 数据获取失败返回空值而不是报错（fail-open）

== Level 2: 业务逻辑级别 ==
3. 价格/金额变量用 .get(key, 0.0) 提供默认值 — 缺数据时静默用零价格
4. latest_price = open_price 回退 — API 拿不到实时价就用开盘价顶替
5. 三元表达式中价格变量回退到 0.0 或其他价格变量
6. PriceSnapshot 构造时用 open_price 作为 latest_price 的默认值

原则：交易安全 > 程序健壮性。拿不到数据 → 报错停止，不许继续交易。

用法：
    uv run python scripts/audit_trading_safety.py
    uv run python scripts/audit_trading_safety.py --strict  # 包含 web/simulation
"""

import ast
import io
import re
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

# 价格/金额相关的变量名关键词 — 这些变量不允许用默认值回退
FINANCIAL_VAR_KEYWORDS = {
    "price",
    "latest",
    "close",
    "open_price",
    "prev_close",
    "high",
    "low",
    "volume",
    "amount",
    "sell_price",
    "buy_price",
    "cash",
    "balance",
    "pnl",
    "cost",
    "turnover",
    "market_cap",
    "cum_vol",
    "max_high",
    "min_low",
}

# .get() 调用中不允许用 0.0 默认值的 key 关键词
FINANCIAL_KEY_KEYWORDS = {
    "price",
    "close",
    "open",
    "high",
    "low",
    "volume",
    "amount",
    "balance",
    "cash",
    "cost",
    "pnl",
    "sell",
    "buy",
    "latest",
    "turnover",
    "market_cap",
    "prev",
}


@dataclass
class Violation:
    file: str
    line: int
    category: str
    detail: str
    severity: str  # CRITICAL / WARNING
    source_line: str = ""  # 违规代码行的原文


@dataclass
class AuditResult:
    violations: list[Violation] = field(default_factory=list)
    files_scanned: int = 0


def _is_financial_name(name: str) -> bool:
    """Check if a variable name looks like a financial/price variable."""
    name_lower = name.lower()
    return any(kw in name_lower for kw in FINANCIAL_VAR_KEYWORDS)


def _is_financial_key(key_str: str) -> bool:
    """Check if a dict key string looks like a financial field."""
    key_lower = key_str.lower()
    return any(kw in key_lower for kw in FINANCIAL_KEY_KEYWORDS)


def _is_zero_constant(node: ast.AST) -> bool:
    """Check if node is a zero-ish constant (0, 0.0, None)."""
    if isinstance(node, ast.Constant):
        return node.value in (0, 0.0, None)
    return False


def _get_source_line(source_lines: list[str], lineno: int) -> str:
    """Get source line by 1-based line number."""
    if 1 <= lineno <= len(source_lines):
        return source_lines[lineno - 1].strip()
    return ""


# ============================================================
# Level 1: Exception handling auditor (original)
# ============================================================


class ExceptionAuditor(ast.NodeVisitor):
    """AST visitor that detects exception handling safety violations."""

    def __init__(self, filepath: str, source_lines: list[str]):
        self.filepath = filepath
        self.source_lines = source_lines
        self.violations: list[Violation] = []

    def _add(self, node: ast.AST, category: str, detail: str, severity: str = "CRITICAL"):
        lineno = getattr(node, "lineno", 0)
        # Allow suppression via inline comment: # safety: ignore
        if "# safety: ignore" in _get_source_line(self.source_lines, lineno):
            return
        self.violations.append(
            Violation(
                file=self.filepath,
                line=lineno,
                category=category,
                detail=detail,
                severity=severity,
                source_line=_get_source_line(self.source_lines, lineno),
            )
        )

    def visit_ExceptHandler(self, node: ast.ExceptHandler):
        """Check for broad exception catching without re-raise."""
        is_broad = False
        if node.type is None:
            is_broad = True
        elif isinstance(node.type, ast.Name) and node.type.id in ("Exception", "BaseException"):
            is_broad = True

        if not is_broad:
            self.generic_visit(node)
            return

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
                "except Exception 只打日志不 raise — 错误被静默吞掉",
            )
        elif not has_raise and is_broad:
            self._add(
                node,
                "BROAD_CATCH_NO_RAISE",
                "except Exception 没有 raise — 可能导致静默降级",
                severity="WARNING",
            )

        self.generic_visit(node)


# ============================================================
# Level 2: Business logic fallback auditor
# ============================================================


class BusinessLogicAuditor(ast.NodeVisitor):
    """AST visitor that detects business-logic-level silent degradation.

    Catches patterns like:
    - sell_price = prices.get(code, 0.0)   # 缺数据用零价格
    - latest = val if val else open_price  # 回退到开盘价
    - PriceSnapshot(latest_price=open_price)  # 用开盘价充当实时价
    """

    def __init__(self, filepath: str, source_lines: list[str]):
        self.filepath = filepath
        self.source_lines = source_lines
        self.violations: list[Violation] = []

    def _add(self, node: ast.AST, category: str, detail: str, severity: str = "CRITICAL"):
        lineno = getattr(node, "lineno", 0)
        self.violations.append(
            Violation(
                file=self.filepath,
                line=lineno,
                category=category,
                detail=detail,
                severity=severity,
                source_line=_get_source_line(self.source_lines, lineno),
            )
        )

    def visit_Assign(self, node: ast.Assign):
        """Check assignments for fallback patterns."""
        # Pattern: var = dict.get(key, 0.0) where var is financial
        if len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            target_name = node.targets[0].id
            if _is_financial_name(target_name):
                self._check_dict_get_fallback(node, target_name, node.value)
                self._check_ternary_fallback(node, target_name, node.value)

        self.generic_visit(node)

    def _check_dict_get_fallback(self, node: ast.AST, target: str, value: ast.AST):
        """Detect: price = d.get(key, 0.0)"""
        if not isinstance(value, ast.Call):
            return
        func = value.func
        if not (isinstance(func, ast.Attribute) and func.attr == "get"):
            return
        if len(value.args) < 2:
            return

        default_arg = value.args[1]
        if _is_zero_constant(default_arg):
            # Check if the key is also financial
            key_arg = value.args[0]
            key_str = ""
            if isinstance(key_arg, ast.Constant) and isinstance(key_arg.value, str):
                key_str = key_arg.value
            elif isinstance(key_arg, ast.Attribute):
                key_str = key_arg.attr

            detail = f"`{target} = ...get(..., 0.0)` — 缺数据时静默用 0.0 替代，应该报错或跳过"
            if key_str and _is_financial_key(key_str):
                self._add(node, "PRICE_FALLBACK_ZERO", detail)
            elif _is_financial_name(target):
                self._add(node, "PRICE_FALLBACK_ZERO", detail, severity="WARNING")

    def _check_ternary_fallback(self, node: ast.AST, target: str, value: ast.AST):
        """Detect: latest = x if x else open_price / 0.0"""
        if not isinstance(value, ast.IfExp):
            return

        orelse = value.orelse

        # Case 1: fallback to 0.0
        if _is_zero_constant(orelse):
            self._add(
                node,
                "TERNARY_FALLBACK_ZERO",
                f"`{target} = ... if ... else 0.0` — 缺数据时静默用 0.0，应该报错或跳过",
                severity="WARNING",
            )
            return

        # Case 2: fallback to another price variable (e.g., open_price)
        if isinstance(orelse, ast.Name) and _is_financial_name(orelse.id):
            # latest_price = x if x else open_price -> CRITICAL
            self._add(
                node,
                "PRICE_FALLBACK_SUBSTITUTE",
                f"`{target} = ... if ... else {orelse.id}` — "
                f"数据缺失时用 {orelse.id} 替代 {target}，"
                f"会导致涨幅计算为 0%，静默掩盖数据缺失",
            )

    def visit_Call(self, node: ast.Call):
        """Check function calls for PriceSnapshot(latest_price=open_price)."""
        func = node.func
        func_name = ""
        if isinstance(func, ast.Name):
            func_name = func.id
        elif isinstance(func, ast.Attribute):
            func_name = func.attr

        # Detect PriceSnapshot(latest_price=open_price) pattern
        if func_name == "PriceSnapshot":
            for kw in node.keywords:
                if kw.arg == "latest_price" and isinstance(kw.value, ast.Name):
                    val_name = kw.value.id
                    if val_name == "open_price":
                        self._add(
                            node,
                            "SNAPSHOT_PRICE_FALLBACK",
                            "PriceSnapshot(latest_price=open_price) — "
                            "用开盘价充当实时价，gain_from_open=0%，"
                            "静默掩盖 9:40 数据缺失",
                        )

        self.generic_visit(node)


# ============================================================
# Level 3: Regex-based line scanning for patterns hard to catch via AST
# ============================================================

# Each rule: (pattern, category, detail_template, severity)
# detail_template can use {match} for the matched text
LINE_SCAN_RULES: list[tuple[re.Pattern, str, str, str]] = [
    # sell_prices.get(xxx, 0.0) or buy_prices.get(xxx, 0.0)
    (
        re.compile(r"(sell_price|buy_price)\w*\s*=\s*\w+\.get\(.+?,\s*0\.0\s*\)"),
        "PRICE_FALLBACK_ZERO",
        "卖出/买入价用 .get(key, 0.0) — 缺数据时以零价格成交，导致 P&L 严重失真",
        "CRITICAL",
    ),
    # cash_balance = xxx.get("cash_balance", 0.0) or similar
    (
        re.compile(r"cash_balance\s*=\s*.*\.get\(.*,\s*0\.0\s*\)"),
        "BALANCE_FALLBACK_ZERO",
        "现金余额用 .get(key, 0.0) — 状态丢失时静默归零，隐藏数据损坏",
        "CRITICAL",
    ),
    # latest_price = open_price (direct assignment, not in if/else)
    (
        re.compile(r"latest_price\s*=\s*open_price\s*(?:#.*)?$"),
        "PRICE_SUBSTITUTION",
        "latest_price = open_price — 用开盘价顶替实时价，涨幅被静默归零",
        "CRITICAL",
    ),
    # max_high = 0.0 or min_low = 0.0 as initialization before conditional fill
    (
        re.compile(r"(?:max_high|min_low)\s*=\s*0\.0\s*(?:#.*)?$"),
        "HIGHLOW_INIT_ZERO",
        "max_high/min_low 初始化为 0.0 — 如果后续条件分支未填充，0.0 会被当作真实极值参与计算",
        "WARNING",
    ),
]


def scan_lines(filepath: str, source_lines: list[str]) -> list[Violation]:
    """Scan source lines with regex rules."""
    violations = []
    for lineno_0, line in enumerate(source_lines):
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        for pattern, category, detail, severity in LINE_SCAN_RULES:
            if pattern.search(stripped):
                violations.append(
                    Violation(
                        file=filepath,
                        line=lineno_0 + 1,
                        category=category,
                        detail=detail,
                        severity=severity,
                        source_line=stripped,
                    )
                )
    return violations


# ============================================================
# Orchestration
# ============================================================


def audit_file(filepath: Path) -> list[Violation]:
    """Audit a single Python file for trading safety violations."""
    try:
        source = filepath.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(filepath))
    except (SyntaxError, UnicodeDecodeError):
        return []

    rel_path = str(filepath.relative_to(PROJECT_ROOT)).replace("\\", "/")
    source_lines = source.splitlines()

    # Level 1: Exception handling
    exc_auditor = ExceptionAuditor(rel_path, source_lines)
    exc_auditor.visit(tree)

    # Level 2: Business logic fallbacks
    biz_auditor = BusinessLogicAuditor(rel_path, source_lines)
    biz_auditor.visit(tree)

    # Level 3: Line-scan regex rules
    line_violations = scan_lines(rel_path, source_lines)

    return exc_auditor.violations + biz_auditor.violations + line_violations


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
            if v.source_line:
                print(f"    >>> {v.source_line}")

    if warnings:
        print(f"\n  ⚠️  警告 (WARNING): {len(warnings)}")
        print(f"  {'─' * 60}")
        for v in warnings:
            print(f"\n  [{v.category}] {v.file}:{v.line}")
            print(f"    {v.detail}")
            if v.source_line:
                print(f"    >>> {v.source_line}")

    if not result.violations:
        print("\n  ✅ 没有发现交易安全问题")

    print(f"\n{'=' * 70}")

    return 1 if critical else 0


def main():
    strict = "--strict" in sys.argv
    result = run_audit(strict=strict)
    exit_code = print_report(result)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
