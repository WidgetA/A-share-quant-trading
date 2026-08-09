from pathlib import Path

_TEMPLATE = Path(__file__).parents[3] / "src" / "web" / "templates" / "index.html"


def test_margin_risk_curve_uses_direct_plot_gestures_without_range_buttons() -> None:
    html = _TEMPLATE.read_text(encoding="utf-8")

    assert 'class="risk-tool-button"' not in html
    assert "data-risk-range" not in html
    assert "overlay.addEventListener('wheel'" in html
    assert "overlay.addEventListener('pointerdown'" in html
    assert "overlay.addEventListener('dblclick'" in html
    assert "touch-action: none" in html


def test_margin_risk_metric_guide_explains_every_chart_metric() -> None:
    html = _TEMPLATE.read_text(encoding="utf-8")

    assert 'id="riskMetricGuideButton"' in html
    assert 'role="dialog"' in html
    assert html.count('class="risk-explainer-item"') == 14
    assert html.count("<dt>是什么</dt>") == 14
    assert html.count("<dt>表明什么</dt>") == 14
    assert html.count("<dt>导向结果</dt>") == 14
