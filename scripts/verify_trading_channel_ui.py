"""Offline browser verification of actual templates; all HTTP is intercepted.

Run: uv run python scripts/verify_trading_channel_ui.py
Requires Playwright Chromium. Screenshots go under ignored data/channel-ui/.
"""

import json
from pathlib import Path
from urllib.parse import urlsplit

from jinja2 import Environment, FileSystemLoader, select_autoescape
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
WEB = ROOT / "src" / "web"
OUT = ROOT / "data" / "channel-ui"
OUT.mkdir(parents=True, exist_ok=True)
env = Environment(loader=FileSystemLoader(WEB / "templates"), autoescape=select_autoescape())
context = {
    key: {}
    for key in ("broker_status", "scheduler", "model_scheduler", "daily_scan", "pre_market_report")
}
context.update(today="2026-09-19", recommendations_enabled=False)
state = {"backend": "miniqmt", "requests": []}


def intercept(route):
    req = route.request
    path = urlsplit(req.url).path
    if path in ("/", "/settings"):
        route.fulfill(
            content_type="text/html",
            body=env.get_template("settings.html" if path == "/settings" else "index.html").render(
                **context
            ),
        )
        return
    if path.startswith("/static/"):
        local = WEB / path.lstrip("/")
        if local.is_file():
            route.fulfill(path=local)
        else:
            route.fulfill(status=404, body="")
        return
    payload = {}
    if path == "/api/settings/trading-channel":
        if req.method == "POST":
            state["backend"] = req.post_data_json["backend"]
            payload = {"success": True}
        else:
            payload = {
                "backend": state["backend"],
                "channel": state["backend"] + "-test",
                "profiles": {
                    "miniqmt": {"configured": True},
                    "qmt": {
                        "configured": True,
                        "url": "https://qmt.example:18443",
                        "instance_id": "demo",
                        "key_id": "ingress",
                        "ca_file": "",
                    },
                },
            }
    elif path == "/api/settings/trading-channel/qmt":
        state["requests"].append(req.post_data_json)
        payload = {"success": True, "message": "QMT 配置已保存，选择 QMT 后生效"}
    elif path == "/api/stock/status":
        payload = {
            "broker_configured": True,
            "broker_healthy": True,
            "available_cash": 10000,
            "holdings_count": 0,
        }
    elif path == "/api/trading/orders":
        payload = {"orders": [], "channel": state["backend"] + "-test"}
    elif path == "/api/trading/holdings":
        payload = {"holdings": [], "channel": state["backend"] + "-test"}
    elif path == "/api/trading/equity-curve":
        payload = {"snapshots": [], "weekly": [], "current": {}}
    elif path == "/api/trading/recommendations":
        payload = {"recommendations": []}
    elif path == "/api/trading/buy":
        state["requests"].append(req.post_data_json)
        payload = {"success": True, "status": "RECEIVED", "order_id": "demo-order"}
    route.fulfill(content_type="application/json", body=json.dumps(payload))


with sync_playwright() as playwright:
    browser = playwright.chromium.launch()
    page = browser.new_page(viewport={"width": 1440, "height": 1050})
    errors = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    page.route("**/*", intercept)
    page.goto("http://channel.local/settings")
    page.wait_for_function(
        "document.getElementById('tradingChannelCurrent').textContent.includes('miniQMT')"
    )
    assert page.locator("#qmt_secret").input_value() == ""
    page.locator("#qmt_secret").fill("offline-demo-secret")
    page.get_by_role("button", name="保存 QMT 配置").click()
    page.wait_for_function(
        "document.getElementById('qmtConfigMessage').textContent.includes('已保存')"
    )
    assert page.locator("#qmt_secret").input_value() == ""
    page.locator("#tradingChannelSelect").select_option("qmt")
    page.locator("#tradingChannelApply").click()
    page.wait_for_function(
        "window.tradingChannel.backend === 'qmt' && document.getElementById('tradingChannelCurrent').textContent === '当前使用：QMT'"
    )
    page.screenshot(path=OUT / "settings.png", full_page=False)
    page.goto("http://channel.local/")
    page.wait_for_function("window.tradingChannel.backend === 'qmt'")
    page.locator("#brokerCard").scroll_into_view_if_needed()
    page.locator("#brokerCard").screenshot(path=OUT / "dashboard-channel.png")
    assert "每只提交一次" in page.locator("#channelBatchHint").inner_text()
    page.evaluate("""async () => {
        const a = await channelOrderPayload({stock_code:'601988',quantity:100,price:6.5}, 'probe');
        const b = await channelOrderPayload({stock_code:'601988',quantity:100,price:6.5}, 'probe');
        if (a.request_id !== b.request_id || a.channel !== 'qmt-test') throw Error('identity lost');
        const button = document.createElement('button'); button.dataset.quantity = '100';
        document.body.appendChild(button);
        await doBuy('601988', '演示股票', 6.5, 'probe', button);
    }""")
    assert state["requests"][-1]["channel"] == "qmt-test"
    assert len(state["requests"][-1]["request_id"]) == 32
    page.set_viewport_size({"width": 390, "height": 844})
    page.goto("http://channel.local/settings")
    page.wait_for_function("window.tradingChannel.backend === 'qmt'")
    assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
    page.screenshot(path=OUT / "settings-mobile.png", full_page=False)
    browser.close()
    assert not errors, errors
print("PASS: settings, channel switch, dashboard, request identity, desktop/mobile layout")
