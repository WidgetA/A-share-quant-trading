import base64
import re
import ssl
from pathlib import Path

import certifi
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.trading.channel_store import default_store
from src.web.broker_channel_routes import create_broker_channel_router

SETTINGS = {
    "url": "https://qmt.example:18443",
    "instance_id": "test-instance",
    "key_id": "test-key",
    "secret": "test-signing-secret",
}
BASE = "/api/settings/trading-channel"


@pytest.fixture
def client(monkeypatch, tmp_path):
    monkeypatch.setattr("src.common.config.PROJECT_ROOT", tmp_path)
    monkeypatch.setattr("src.common.config.get_trading_api_key", lambda: "web-test-key")
    app = FastAPI()
    app.include_router(create_broker_channel_router())
    with TestClient(app, headers={"X-API-Key": "web-test-key"}) as client:
        yield client


@pytest.fixture
def certificates():
    return re.findall(
        rb"-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----",
        Path(certifi.where()).read_bytes(),
        re.DOTALL,
    )[:2]


def upload(client, contents, name="ca.crt"):
    return client.post(BASE + "/qmt/ca", files={"file": (name, contents)})


def test_uploaded_certificate_is_used_and_survives_configuration_reload(client, certificates):
    response = upload(client, certificates[0], "../../outside.pem")
    assert response.status_code == 200
    certificate_id = response.json()["certificate_id"]
    assert re.fullmatch(r"[0-9a-f]{64}", certificate_id)
    assert (
        client.post(
            BASE + "/qmt", json={**SETTINGS, "ca_certificate_id": certificate_id}
        ).status_code
        == 200
    )
    store = default_store()
    original_route = store.preference("qmt")
    saved = store.profile(original_route)
    path = Path(saved["ca_file"])
    assert path.parent == store.path.parent / "qmt-certificates"
    assert path.name == certificate_id + ".pem"
    assert certificates[0] in path.read_bytes()
    ssl.create_default_context(cafile=path)
    public = client.get(BASE).json()["profiles"]["qmt"]
    assert public["ca_configured"] is True
    assert "ca_file" not in public
    assert "secret" not in public
    # A subsequent save without selecting another file keeps the saved certificate and key.
    assert client.post(BASE + "/qmt", json={**SETTINGS, "secret": ""}).status_code == 200
    assert default_store().preference("qmt") == original_route
    assert client.get(BASE).json()["backend"] == "miniqmt"


def test_der_certificate_upload(client, certificates):
    der = base64.b64decode(certificates[0].split(b"-----")[2])
    response = upload(client, der, "ca.cer")
    assert response.status_code == 200
    assert (
        client.post(
            BASE + "/qmt",
            json={**SETTINGS, "ca_certificate_id": response.json()["certificate_id"]},
        ).status_code
        == 200
    )


@pytest.mark.parametrize(
    "contents", [b"", b"not a certificate", b"x" * 65537], ids=["empty", "invalid", "too_large"]
)
def test_invalid_upload_does_not_create_certificate_files(client, contents):
    response = upload(client, contents)
    assert response.status_code in (400, 413)
    assert not list(default_store().path.parent.glob("qmt-certificates/*"))
    assert default_store().preference("qmt") is None


def test_certificate_must_not_contain_a_private_key(client, certificates):
    response = upload(client, certificates[0] + b"\n-----BEGIN PRIVATE KEY-----\nsecret")
    assert response.status_code == 400
    assert not list(default_store().path.parent.glob("qmt-certificates/*"))


def test_upload_requires_existing_web_authorization(client, certificates):
    response = client.post(
        BASE + "/qmt/ca",
        headers={"X-API-Key": "wrong-key"},
        files={"file": ("ca.pem", certificates[0])},
    )
    assert response.status_code == 401


def test_replacing_or_clearing_certificate_preserves_old_channel_certificate(client, certificates):
    ids = [upload(client, cert).json()["certificate_id"] for cert in certificates]
    for certificate_id in ids:
        assert (
            client.post(
                BASE + "/qmt", json={**SETTINGS, "ca_certificate_id": certificate_id}
            ).status_code
            == 200
        )
    store = default_store()
    paths = [store.path.parent / "qmt-certificates" / (key + ".pem") for key in ids]
    assert all(path.is_file() for path in paths)
    assert client.post(BASE + "/qmt", json={**SETTINGS, "use_system_ca": True}).status_code == 200
    assert client.get(BASE).json()["profiles"]["qmt"]["ca_configured"] is False
    assert all(path.is_file() for path in paths)


@pytest.mark.parametrize("certificate_id", ["a" * 64, "../../outside.pem"])
def test_unknown_certificate_cannot_silently_use_system_trust(client, certificate_id):
    response = client.post(BASE + "/qmt", json={**SETTINGS, "ca_certificate_id": certificate_id})
    assert response.status_code == 400
    assert default_store().preference("qmt") is None


def test_connection_test_uses_upload_without_changing_saved_profile(
    client, certificates, monkeypatch
):
    assert client.post(BASE + "/qmt", json=SETTINGS).status_code == 200
    original_route = default_store().preference("qmt")
    certificate_id = upload(client, certificates[0]).json()["certificate_id"]
    observed = []

    async def snapshot(broker):
        observed.append(Path(broker.spec["ca_file"]).read_bytes())
        return {}

    async def is_ready(broker):
        return False

    monkeypatch.setattr("src.trading.qmt_http_client.QmtHttpClient.snapshot", snapshot)
    monkeypatch.setattr("src.trading.qmt_http_client.QmtHttpClient.is_ready", is_ready)
    response = client.post(
        BASE + "/qmt/test", json={**SETTINGS, "ca_certificate_id": certificate_id}
    )
    assert response.json()["success"] is True
    assert response.json()["trade_ready"] is False
    assert certificates[0] in observed[0]
    assert default_store().preference("qmt") == original_route
