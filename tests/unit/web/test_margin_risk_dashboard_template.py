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
    assert "从融资扩张的正反馈寻找下行拐点" in html
    assert "风险预算增加 → 融资扩张 → 主动购买力增强 → 趋势资金加仓" in html
    assert "扩张减速 → 个股融资动能广泛转弱 → 净偿还扩散 → 去杠杆" in html
    assert "融资余额高就会下跌" in html
    assert "不代表市场全部风险预算和资金来源" in html
    assert "把指标连起来读" in html
    assert html.count('class="risk-explainer-item"') == 14
    assert html.count("<dt>是什么</dt>") == 14
    assert html.count("<dt>表明什么</dt>") == 14
    assert html.count("<dt>导向结果</dt>") == 14
