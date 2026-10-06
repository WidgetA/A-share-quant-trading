"""Native stream, cloud byte proof and failure semantics without any cloud call."""

import base64
import importlib.util
import io
import json
import sys
from types import SimpleNamespace

import pytest

from tests.unit.test_cross_market_release import DEPLOY_DIR, RELEASE, make_release_inputs

SPEC = importlib.util.spec_from_file_location("deploy_fc", DEPLOY_DIR / "deploy_fc.py")
DEPLOY = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = DEPLOY
SPEC.loader.exec_module(DEPLOY)


class ShortStream(io.BytesIO):
    def read(self, size=-1):
        return super().read(min(size, 7))


def fake_response(body, *, status=200, headers=None):
    return SimpleNamespace(status_code=status, headers=headers or {}, body=body)


class FakeCloud:
    def __init__(self, raw_zip):
        self.raw_zip = raw_zip
        self.calls, self.requests, self.streams = [], [], []
        self.function = SimpleNamespace(
            function_name=DEPLOY.FUNCTION,
            environment_variables={"EXISTING": "preserved"},
            **DEPLOY.FUNCTION_SETTINGS,
        )
        self.mutate_function = lambda body: None
        self.mutate_envelope = lambda body: None
        self.invoke_headers, self.invoke_status = {}, 200

    def get_function(self, name, request):
        self.calls.append(("get", name))
        return fake_response(self.function)

    def update_function(self, name, request):
        self.calls.append(("update", name))
        self.updated = request.body
        assert base64.b64decode(request.body.code.zip_file) == self.raw_zip
        self.function = SimpleNamespace(
            function_name=name,
            environment_variables=request.body.environment_variables,
            **{key: getattr(request.body, key) for key in DEPLOY.FUNCTION_SETTINGS},
        )
        self.mutate_function(self.function)
        return fake_response(self.function)

    def get_function_code(self, name, request):
        self.calls.append(("code", name))
        return fake_response(
            SimpleNamespace(url="https://code.invalid/file?Signature=DO_NOT_PRINT")
        )

    def invoke_function_with_options(self, name, request, headers, runtime):
        assert headers.x_fc_invocation_type == "Sync"
        payload = json.loads(request.body.read())
        self.requests.append(payload)
        self.calls.append(("invoke", name))
        meta = {
            "symbol": payload["symbol"],
            "instrumentType": "INDEX",
            "currency": "USD" if payload["market"] == "US" else "KRW",
            "exchangeTimezoneName": "America/New_York"
            if payload["market"] == "US"
            else "Asia/Seoul",
            "dataGranularity": payload.get("interval", "1d"),
        }
        raw = json.dumps({"chart": {"error": None, "result": [{"meta": meta}]}})
        envelope = {
            **payload,
            "fetched_at": 1_791_300_000_000,
            "raw_json": raw,
            "payload_sha256": RELEASE.digest(raw.encode()),
            "request_url": "https://query1.finance.yahoo.com/v8/finance/chart/example",
            "runtime": {"region": DEPLOY.REGION, "fc_request_id": "actual-native-request"},
        }
        self.mutate_envelope(envelope)
        stream = ShortStream(json.dumps(envelope).encode())
        self.streams.append(stream)
        return fake_response(stream, status=self.invoke_status, headers=self.invoke_headers)


@pytest.fixture
def prepared(tmp_path):
    inputs = make_release_inputs(tmp_path)
    manifest = RELEASE.build_release(**inputs)
    cloud = FakeCloud((inputs["output_dir"] / "worker.zip").read_bytes())
    models = SimpleNamespace(
        **{
            name: SimpleNamespace
            for name in (
                "GetFunctionRequest",
                "UpdateFunctionRequest",
                "UpdateFunctionInput",
                "InputCodeLocation",
                "GetFunctionCodeRequest",
                "InvokeFunctionRequest",
                "InvokeFunctionHeaders",
            )
        }
    )
    parsed = []

    def parse(source, request, fetched_at):
        parsed.append((source, request, fetched_at))
        kind = "minute_bar" if request["capability"] == "minute_history" else "daily_bar"
        if request.get("interval") == "1h":
            kind = "hour_bar"
        return {}, [{"data_kind": kind}]

    kwargs = {
        "release_dir": inputs["output_dir"],
        "client": cloud,
        "endpoint": "123456789.us-west-1.fc.aliyuncs.com",
        "models_module": models,
        "runtime_options": object(),
        "download": lambda url: cloud.raw_zip,
        "parse_source": parse,
        "now": 1_791_300_000,
    }
    return SimpleNamespace(
        inputs=inputs,
        manifest=manifest,
        cloud=cloud,
        kwargs=kwargs,
        parsed=parsed,
    )


def test_only_yahoo_function_updated_then_exact_cloud_bytes_and_all_smokes_verified(prepared):
    result = DEPLOY.deploy_release(**prepared.kwargs)
    assert result["revision"] == prepared.manifest["revision"]
    assert result["code_zip_sha256"] == result["cloud_zip_sha256"]
    assert result["cloud_zip_sha256"] == prepared.manifest["worker_sha256"]
    assert result["uploaded_zip_exact_cloud_readback"] is True and result["verified"] is True
    assert (
        prepared.cloud.updated.environment_variables[DEPLOY.SOURCE_REVISION_ENV]
        == result["revision"]
    )
    assert prepared.cloud.updated.environment_variables["EXISTING"] == "preserved"
    assert all(name == DEPLOY.FUNCTION for _, name in prepared.cloud.calls)
    assert len(prepared.cloud.requests) == 5
    assert {(item["market"], item["capability"]) for item in prepared.cloud.requests} == {
        ("US", "daily_history"),
        ("KR", "snapshot_only"),
        ("US", "minute_history"),
        ("KR", "minute_history"),
        ("KR", "hour_history"),
    }
    assert {item.get("interval") for item in prepared.cloud.requests} == {None, "1m", "5m", "1h"}
    assert len(prepared.parsed) == 5
    assert result["native_smoke"][-1]["hour_bars"] == 1
    assert result["native_smoke"][-1]["minute_bars"] == 0
    assert all(stream.closed for stream in prepared.cloud.streams)
    assert json.loads((prepared.inputs["output_dir"] / "fc-deployment.json").read_text()) == result


def test_cloud_zip_mismatch_blocks_smoke_and_verified_receipt(prepared):
    prepared.kwargs["download"] = lambda url: b"cloud package from another revision"
    with pytest.raises(DEPLOY.FCDeploymentError, match="Cloud ZIP"):
        DEPLOY.deploy_release(**prepared.kwargs)
    assert prepared.cloud.requests == []
    assert not (prepared.inputs["output_dir"] / "fc-deployment.json").exists()


def test_fc_omitted_concurrency_is_recorded_without_claiming_readback(prepared):
    # Actual us-west-1 GetFunction omits instanceConcurrency for this function.
    prepared.cloud.mutate_function = lambda body: setattr(body, "instance_concurrency", None)
    result = DEPLOY.deploy_release(**prepared.kwargs)
    assert result["verified"] is True
    assert result["instance_concurrency_requested"] == 1
    assert result["instance_concurrency_readback"] is None
    assert result["instance_concurrency_confirmed"] is False
    assert len(prepared.cloud.requests) == 5


def test_fc_contradictory_concurrency_readback_still_fails(prepared):
    prepared.cloud.mutate_function = lambda body: setattr(body, "instance_concurrency", 2)
    with pytest.raises(DEPLOY.FCDeploymentError, match="settings or source revision"):
        DEPLOY.deploy_release(**prepared.kwargs)
    assert prepared.cloud.requests == []


def test_actual_getfunction_revision_mismatch_blocks_download_and_smoke(prepared):
    prepared.cloud.mutate_function = lambda body: body.environment_variables.update(
        {
            DEPLOY.SOURCE_REVISION_ENV: "b" * 40,
        }
    )
    with pytest.raises(DEPLOY.FCDeploymentError, match="revision differ"):
        DEPLOY.deploy_release(**prepared.kwargs)
    assert all(call != "code" and call != "invoke" for call, _ in prepared.cloud.calls)


@pytest.mark.parametrize("header", ["X-Fc-Error", "X-Fc-Error-Type", "X-Fc-Function-Error"])
def test_http200_function_error_cannot_pass_even_with_valid_success_envelope(prepared, header):
    prepared.cloud.invoke_headers = {header: "UnhandledInvocationError"}
    with pytest.raises(DEPLOY.FCDeploymentError, match="function failed"):
        DEPLOY.deploy_release(**prepared.kwargs)
    assert prepared.cloud.streams[0].closed
    assert not (prepared.inputs["output_dir"] / "fc-deployment.json").exists()


@pytest.mark.parametrize(
    "field,value",
    [
        ("symbol", "^WRONG"),
        ("start", 17),
        ("request_id", "unrelated-request"),
        ("payload_sha256", "0" * 64),
        ("raw_json", "not a chart"),
        ("runtime", {"region": "cn-hangzhou", "fc_request_id": "wrong region"}),
    ],
)
def test_wrong_native_identity_or_source_sha_is_not_deployment_success(prepared, field, value):
    prepared.cloud.mutate_envelope = lambda body: body.update({field: value})
    with pytest.raises(DEPLOY.FCDeploymentError):
        DEPLOY.deploy_release(**prepared.kwargs)
    assert not (prepared.inputs["output_dir"] / "fc-deployment.json").exists()


def test_native_http200_runtime_error_body_is_not_success(prepared):
    def error_body(body):
        body.clear()
        body.update(errorType="RuntimeError", errorMessage="private provider detail")

    prepared.cloud.mutate_envelope = error_body
    with pytest.raises(DEPLOY.FCDeploymentError, match="identity"):
        DEPLOY.deploy_release(**prepared.kwargs)


@pytest.mark.parametrize(
    "field,value",
    [
        ("symbol", "^WRONG"),
        ("instrumentType", "ETF"),
        ("currency", "KRW"),
        ("exchangeTimezoneName", "Asia/Seoul"),
    ],
)
def test_bad_raw_instrument_identity_with_correct_sha_is_rejected(prepared, field, value):
    def mutate(body):
        source = json.loads(body["raw_json"])
        source["chart"]["result"][0]["meta"][field] = value
        body["raw_json"] = json.dumps(source)
        body["payload_sha256"] = RELEASE.digest(body["raw_json"].encode())

    prepared.cloud.mutate_envelope = mutate
    with pytest.raises(DEPLOY.FCDeploymentError, match="local validation"):
        DEPLOY.deploy_release(**prepared.kwargs)


def test_local_artifact_corruption_fails_before_any_remote_call(prepared):
    (prepared.inputs["output_dir"] / "worker.zip").write_bytes(b"tampered")
    with pytest.raises(RELEASE.ReleaseError):
        DEPLOY.deploy_release(**prepared.kwargs)
    assert prepared.cloud.calls == []


def test_parser_from_other_revision_cannot_validate_or_update_release(prepared):
    prepared.kwargs.pop("parse_source")
    with pytest.raises(DEPLOY.FCDeploymentError, match="parser differs"):
        DEPLOY.deploy_release(**prepared.kwargs)
    assert prepared.cloud.calls == []


def test_hour_source_cannot_pass_as_minute_bars(prepared):
    def parse(source, request, fetched_at):
        return {}, [{"data_kind": "minute_bar" if request.get("interval") else "daily_bar"}]

    prepared.kwargs["parse_source"] = parse
    with pytest.raises(DEPLOY.FCDeploymentError, match="local validation"):
        DEPLOY.deploy_release(**prepared.kwargs)
    assert len(prepared.cloud.requests) == 5
    assert not (prepared.inputs["output_dir"] / "fc-deployment.json").exists()


def test_requests_match_installed_official_sdk_models(prepared):
    models = pytest.importorskip("alibabacloud_fc20230330.models")
    prepared.kwargs["models_module"] = models
    result = DEPLOY.deploy_release(**prepared.kwargs)
    serialized = prepared.cloud.updated.to_map()
    assert serialized["environmentVariables"][DEPLOY.SOURCE_REVISION_ENV] == result["revision"]
    assert serialized["runtime"] == "python3.12"
    assert serialized["diskSize"] == 512 and serialized["instanceConcurrency"] == 1
    assert serialized["code"]["zipFile"] == base64.b64encode(prepared.cloud.raw_zip).decode()


def test_environment_credentials_and_sts_account_build_official_https_client(monkeypatch):
    fc_module = pytest.importorskip("alibabacloud_fc20230330.client")
    sts_module = pytest.importorskip("alibabacloud_sts20150401.client")
    configs = []

    class STS:
        def __init__(self, config):
            configs.append(config)

        def get_caller_identity(self):
            return fake_response(SimpleNamespace(account_id="123456789"))

    class FC:
        def __init__(self, config):
            configs.append(config)

    monkeypatch.setattr(sts_module, "Client", STS)
    monkeypatch.setattr(fc_module, "Client", FC)
    monkeypatch.setenv("ALIYUN_ACCESS_KEY_ID", "DUMMY_ACCESS")
    monkeypatch.setenv("ALIYUN_ACCESS_KEY_SECRET", "DUMMY_SECRET")
    monkeypatch.setenv("ALIYUN_SECURITY_TOKEN", "DUMMY_TEMP_TOKEN")
    client, endpoint = DEPLOY._sdk_client(DEPLOY.REGION)
    assert isinstance(client, FC) and endpoint == "123456789.us-west-1.fc.aliyuncs.com"
    assert [config.endpoint for config in configs] == ["sts.aliyuncs.com", endpoint]
    assert all(config.protocol == "HTTPS" for config in configs)
    assert all(config.security_token == "DUMMY_TEMP_TOKEN" for config in configs)
    assert configs[1].access_key_id == "DUMMY_ACCESS"
    assert configs[1].retry_options.retryable is False
    assert configs[1].retry_options.max_attempts == 1


def test_training_target_cannot_be_updated(prepared):
    with pytest.raises(DEPLOY.FCDeploymentError, match="dedicated"):
        DEPLOY.deploy_release(**prepared.kwargs, function_name="model-training-existing")
    assert prepared.cloud.calls == []


def test_failure_removes_old_verified_receipt_instead_of_leaving_false_success(prepared):
    receipt = prepared.inputs["output_dir"] / "fc-deployment.json"
    receipt.write_text('{"verified":true}')
    prepared.kwargs["download"] = lambda url: b"wrong cloud package"
    with pytest.raises(DEPLOY.FCDeploymentError):
        DEPLOY.deploy_release(**prepared.kwargs)
    assert not receipt.exists()


def test_cli_error_never_prints_secret_endpoint_signed_url_or_sdk_message(monkeypatch, capsys):
    class SDKFailure(Exception):
        status_code = 503
        code = "InternalServerError"

    def fail(**kwargs):
        assert __import__("os").environ["DEBUG"] == ""
        raise SDKFailure(
            "PRIVATE_ACCESS_SECRET https://123456.us-west-1.fc.aliyuncs.com?Signed=secret"
        )

    monkeypatch.setattr(DEPLOY, "deploy_release", fail)
    monkeypatch.setenv("DEBUG", "sdk-debug-enabled")
    assert DEPLOY.main(["--release-dir", "."]) == 1
    output = capsys.readouterr().out
    assert json.loads(output) == {
        "error_type": "SDKFailure",
        "code": "InternalServerError",
        "status_code": 503,
    }
    assert "PRIVATE" not in output and "Signed" not in output and "aliyuncs" not in output


def test_cli_success_prints_no_account_or_endpoint(monkeypatch, capsys):
    monkeypatch.setattr(
        DEPLOY,
        "deploy_release",
        lambda **kwargs: {
            "revision": "a" * 40,
            "function_name": DEPLOY.FUNCTION,
            "region": DEPLOY.REGION,
            "verified": True,
            "endpoint": "123456.us-west-1.fc.aliyuncs.com",
        },
    )
    assert DEPLOY.main(["--release-dir", "."]) == 0
    assert "123456" not in capsys.readouterr().out
