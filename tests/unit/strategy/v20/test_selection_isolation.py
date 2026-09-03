from __future__ import annotations

import ast
import hashlib
from pathlib import Path

from src.strategy.strategies.v16_scanner import V16Scanner as ProductionV16Scanner
from src.strategy.v20.selection_scanner import V16Scanner as V20OwnedScanner

PROJECT_ROOT = Path(__file__).resolve().parents[4]


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
    return modules


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_v20_selection_implementation_is_not_the_production_v16_class() -> None:
    assert V20OwnedScanner is not ProductionV16Scanner
    assert Path(V20OwnedScanner.__module__.replace(".", "/")) == Path(
        "src/strategy/v20/selection_scanner"
    )


def test_v20_selection_code_cannot_import_v16_scanner_or_scorer() -> None:
    targets = (
        PROJECT_ROOT / "src" / "strategy" / "v20",
        PROJECT_ROOT / "src" / "web",
    )
    checked = [
        path
        for target in targets
        for path in target.rglob("*.py")
        if target.name == "v20" or path.name.startswith("v20_")
    ]
    banned = {
        "src.strategy.strategies.v16_scanner",
        "src.strategy.lgbrank_scorer",
        "src.web.v15_scan_service",
    }
    offenders = {
        str(path.relative_to(PROJECT_ROOT)): sorted(_imports(path).intersection(banned))
        for path in checked
        if _imports(path).intersection(banned)
    }
    assert offenders == {}


def test_production_v16_cannot_import_v20() -> None:
    production_v16_files = (
        PROJECT_ROOT / "src" / "web" / "v15_scan_service.py",
        PROJECT_ROOT / "src" / "web" / "iquant_routes.py",
        PROJECT_ROOT / "src" / "strategy" / "strategies" / "v16_scanner.py",
        PROJECT_ROOT / "src" / "strategy" / "lgbrank_scorer.py",
    )
    offenders = {
        str(path.relative_to(PROJECT_ROOT)): sorted(
            module for module in _imports(path) if module.startswith("src.strategy.v20")
        )
        for path in production_v16_files
        if any(module.startswith("src.strategy.v20") for module in _imports(path))
    }
    assert offenders == {}


def test_v20_models_are_owned_copies_initially_aligned_with_v16() -> None:
    pairs = (
        (
            PROJECT_ROOT / "models" / "lgbrank_latest.txt",
            PROJECT_ROOT / "models" / "v20" / "lgbrank_latest.txt",
        ),
        (
            PROJECT_ROOT / "models" / "feature_list.json",
            PROJECT_ROOT / "models" / "v20" / "feature_list.json",
        ),
    )
    for v16_path, v20_path in pairs:
        assert v16_path != v20_path
        assert _sha256(v20_path) == _sha256(v16_path)
