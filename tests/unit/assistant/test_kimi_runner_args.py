# === MODULE PURPOSE ===
# AST-001: assistant kimi spawns default to --no-thinking (skills are scripted,
# latency matters in chat), with KIMI_ASSISTANT_THINKING=1 as the quality
# escape hatch. Path B / AI journal are NOT affected — their spawn sites keep
# thinking on (fallback-reasoning lesson).

from __future__ import annotations

import pytest

from src.assistant.kimi_runner import thinking_args


def test_no_thinking_by_default(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("KIMI_ASSISTANT_THINKING", raising=False)
    assert thinking_args() == ["--no-thinking"]


def test_env_flag_restores_thinking(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("KIMI_ASSISTANT_THINKING", "1")
    assert thinking_args() == []
    monkeypatch.setenv("KIMI_ASSISTANT_THINKING", "true")
    assert thinking_args() == []


def test_garbage_env_value_keeps_no_thinking(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("KIMI_ASSISTANT_THINKING", "0")
    assert thinking_args() == ["--no-thinking"]


def test_free_form_tasks_enable_thinking(monkeypatch: pytest.MonkeyPatch):
    """自由提问显式开 thinking(现场选表/选工具/失败绕道要推理)。"""
    monkeypatch.delenv("KIMI_ASSISTANT_THINKING", raising=False)
    assert thinking_args(enable_thinking=True) == []
