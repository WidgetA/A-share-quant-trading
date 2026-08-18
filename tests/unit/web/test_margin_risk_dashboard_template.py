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


def test_updated_at_labels_render_beijing_time_not_browser_local_time() -> None:
    """A股口径统一按北京时间;页面在任何时区打开都必须显示同一个读数。"""

    html = _TEMPLATE.read_text(encoding="utf-8")

    assert "timeZone: 'Asia/Shanghai'" in html
    assert "hourCycle: 'h23'" in html
    # 旧写法跟着浏览器本地时区走,不能再出现。
    assert "toTimeString()" not in html
    assert html.count("'更新于 ' + beijingHM() + ' (北京)'") == 2
