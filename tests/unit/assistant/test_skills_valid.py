# === MODULE PURPOSE ===
# CI GATE for the kimi skill library (AST-001): every skill under kimi-skills/
# must parse — frontmatter present, name + description non-empty, names unique —
# so a broken skill can never ship in the image (push → CI red, not a silently
# dead command in production). Validation uses kimi-cli's OWN frontmatter
# parser, so "valid here" means "valid to the kimi that will load it".

from __future__ import annotations

from pathlib import Path

from kimi_cli.utils.frontmatter import parse_frontmatter

from src.common.config import PROJECT_ROOT

SKILLS_DIR = PROJECT_ROOT / "kimi-skills"


def _skill_files() -> list[Path]:
    return sorted(SKILLS_DIR.glob("*/SKILL.md"))


def test_skills_dir_exists_with_at_least_check_holdings():
    assert SKILLS_DIR.is_dir(), "kimi-skills/ missing from the repo"
    names = {p.parent.name for p in _skill_files()}
    assert "check-holdings" in names, "M1 requires the check-holdings (/持仓) skill"


def test_every_skill_parses_with_kimi_own_parser():
    problems: list[str] = []
    seen: dict[str, str] = {}
    for skill_md in _skill_files():
        rel = skill_md.relative_to(PROJECT_ROOT)
        fm = parse_frontmatter(skill_md.read_text(encoding="utf-8")) or {}
        name = (fm.get("name") or "").strip()
        description = (fm.get("description") or "").strip()
        if not name:
            problems.append(f"{rel}: frontmatter 缺 name")
            continue
        if not description:
            problems.append(f"{rel}: frontmatter 缺 description(kimi 靠它匹配技能)")
        if name.casefold() in seen:
            problems.append(f"{rel}: 技能名 {name!r} 与 {seen[name.casefold()]} 重复")
        seen[name.casefold()] = str(rel)
        if name != skill_md.parent.name:
            problems.append(
                f"{rel}: frontmatter name {name!r} 与目录名 {skill_md.parent.name!r} 不一致"
                "(派单器按目录名指定技能,必须一致)"
            )
    assert not problems, "\n".join(problems)


def test_dispatcher_commands_map_to_existing_skills():
    """Every slash command must point at a real skill directory."""
    from src.assistant.dispatcher import SLASH_COMMANDS

    existing = {p.parent.name for p in _skill_files()}
    for command, spec in SLASH_COMMANDS.items():
        assert spec["skill"] in existing, f"{command} 指向不存在的技能 {spec['skill']!r}"
        assert spec["ack"].strip(), f"{command} 缺秒回文案"
        assert spec["desc"].strip(), f"{command} 缺能力说明(/帮助 列表用)"
