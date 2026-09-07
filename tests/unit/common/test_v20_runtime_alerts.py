import json

import httpx
import pytest

from src.common.v20_runtime_alerts import V20RuntimeAlerts


async def test_direct_feishu_transport_uses_no_database_or_relay(monkeypatch):
    requests = []

    def handler(request):
        requests.append(request)
        assert request.url.host == "open.feishu.cn"
        body = json.loads(request.content)
        if request.url.path.endswith("/internal"):
            assert body == {"app_id": "app", "app_secret": "private"}
            return httpx.Response(200, json={"code": 0, "tenant_access_token": "tenant"})
        assert request.headers["Authorization"] == "Bearer tenant"
        assert body["receive_id"] == "chat"
        assert body["uuid"] == "incident-notice-id"
        assert json.loads(body["content"])["text"] == "V20 运行中断"
        return httpx.Response(200, json={"code": 0, "data": {"message_id": "message"}})

    client = httpx.AsyncClient
    monkeypatch.setattr(
        "src.common.v20_runtime_alerts.httpx.AsyncClient",
        lambda **kwargs: client(transport=httpx.MockTransport(handler), **kwargs),
    )
    alerts = V20RuntimeAlerts("app", "private", "chat")
    await alerts.send("V20 运行中断", "incident-notice-id")
    assert len(requests) == 2
    assert "private" not in repr(alerts)


@pytest.mark.parametrize("bad_token", [True, False])
async def test_rejected_feishu_receipt_is_not_reported_as_sent(monkeypatch, bad_token):
    def handler(request):
        if request.url.path.endswith("/internal") and not bad_token:
            return httpx.Response(200, json={"code": 0, "tenant_access_token": "tenant"})
        return httpx.Response(200, json={"code": 100, "msg": "private response"})

    client = httpx.AsyncClient
    monkeypatch.setattr(
        "src.common.v20_runtime_alerts.httpx.AsyncClient",
        lambda **kwargs: client(transport=httpx.MockTransport(handler), **kwargs),
    )
    with pytest.raises(RuntimeError) as error:
        await V20RuntimeAlerts("app", "private", "chat").send("incident", "uuid")
    assert "private" not in str(error.value)
