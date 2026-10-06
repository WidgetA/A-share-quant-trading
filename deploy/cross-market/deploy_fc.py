"""Update only the v15 Yahoo US function, with exact package and native smoke proof.

Credentials are read from ALIYUN_ACCESS_KEY_ID / ALIYUN_ACCESS_KEY_SECRET and
optional ALIYUN_SECURITY_TOKEN. Account endpoints and signed download URLs are
never printed. No production SSH, database write or training deployment occurs.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import importlib
import io
import json
import os
import re
import sys
import time
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

from build_release import _atomic_write, digest, verify_release

FUNCTION = "ashare_yahoo_indices_v15"
REGION = "us-west-1"
SOURCE_REVISION_ENV = "CROSS_MARKET_SOURCE_REVISION"
FUNCTION_SETTINGS = {
    "runtime": "python3.12",
    "handler": "handler.handler",
    "cpu": 0.35,
    "memory_size": 512,
    "disk_size": 512,
    "timeout": 360,
    "instance_concurrency": 1,
    "internet_access": True,
}


class FCDeploymentError(ValueError):
    """A fixed verification step failed; provider exception text stays private."""

    code = "FCReleaseVerificationFailed"


def _field(value, name):
    return value.get(name) if isinstance(value, Mapping) else getattr(value, name, None)


def _successful(response):
    if _field(response, "status_code") != 200:
        raise FCDeploymentError("FC management request failed")
    return _field(response, "body")


def _sdk_client(region):
    from alibabacloud_fc20230330.client import Client as FCClient
    from alibabacloud_sts20150401.client import Client as STSClient
    from alibabacloud_tea_openapi.utils_models import Config
    from darabonba.policy.retry import RetryOptions

    key = os.environ.get("ALIYUN_ACCESS_KEY_ID")
    secret = os.environ.get("ALIYUN_ACCESS_KEY_SECRET")
    if not key or not secret:
        raise FCDeploymentError("FC deployment credentials are absent")
    credentials = {
        "access_key_id": key,
        "access_key_secret": secret,
        "security_token": os.environ.get("ALIYUN_SECURITY_TOKEN") or None,
        "protocol": "HTTPS",
        "connect_timeout": 15_000,
        "read_timeout": 390_000,
        "retry_options": RetryOptions(retryable=False, max_attempts=1),
    }
    account = _field(
        _successful(
            STSClient(
                Config(
                    **credentials,
                    endpoint="sts.aliyuncs.com",
                )
            ).get_caller_identity()
        ),
        "account_id",
    )
    if not isinstance(account, str) or not account.isdigit():
        raise FCDeploymentError("STS did not establish a valid account")
    endpoint = f"{account}.{region}.fc.aliyuncs.com"
    return FCClient(Config(**credentials, endpoint=endpoint, region_id=region)), endpoint


def _models():
    from alibabacloud_fc20230330 import models

    return models


def _runtime_options():
    from darabonba.runtime import RuntimeOptions

    return RuntimeOptions(autoretry=False, connect_timeout=15_000, read_timeout=390_000)


def _download(url: str) -> bytes:
    import httpx

    parsed = urlsplit(url)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise FCDeploymentError("FC did not provide a HTTPS code download")
    with httpx.Client(trust_env=False, timeout=120, follow_redirects=True) as client:
        response = client.get(url)
        response.raise_for_status()
        return response.content


def _read_native(response) -> bytes:
    body = _field(response, "body")
    try:
        headers = _field(response, "headers")
        if not isinstance(headers, Mapping) or _field(response, "status_code") != 200:
            raise FCDeploymentError("Native smoke HTTP response failed")
        headers = {str(key).lower(): value for key, value in headers.items()}
        if any(
            headers.get(name)
            for name in (
                "x-fc-error",
                "x-fc-error-type",
                "x-fc-function-error",
            )
        ):
            raise FCDeploymentError("Native smoke function failed")
        if isinstance(body, bytes):
            return body
        if isinstance(body, str):
            return body.encode("utf-8")
        if not callable(getattr(body, "read", None)):
            raise FCDeploymentError("Native smoke body is unreadable")
        chunks = []
        while chunk := body.read(65_536):
            if not isinstance(chunk, bytes):
                raise FCDeploymentError("Native smoke body is not a byte stream")
            chunks.append(chunk)
        return b"".join(chunks)
    finally:
        if callable(getattr(body, "close", None)):
            body.close()


def _source_parser(manifest):
    # The smoke parser must be the same source that was actually uploaded.
    root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(root))
    modules = {}
    for name in ("yahoo_indices", "yahoo_intraday_indices"):
        path = "src/data/" + name + ".py"
        if digest((root / path).read_bytes()) != manifest["worker_files"].get(path):
            raise FCDeploymentError("Local smoke parser differs from the release")
        module = importlib.import_module("src.data." + name)
        if digest(Path(module.__file__).read_bytes()) != manifest["worker_files"].get(path):
            raise FCDeploymentError("Imported smoke parser differs from the release")
        modules[name] = module

    def parse(source, request, fetched_at):
        if request["capability"] in ("minute_history", "hour_history"):
            return modules["yahoo_intraday_indices"].parse_intraday_chart(
                source,
                symbol=request["symbol"],
                market=request["market"],
                fetched_at=fetched_at,
                interval=request["interval"],
                start=request["start"],
                end=request["end"],
            )
        return modules["yahoo_indices"].parse_chart(
            source,
            symbol=request["symbol"],
            market=request["market"],
            fetched_at=fetched_at,
            capability=request["capability"],
        )

    return parse


def _smoke_requests(now: int):
    for symbol, market, capability, interval in (
        ("^SOX", "US", "daily_history", None),
        ("KOSPI-10.KS", "KR", "snapshot_only", None),
        ("^SOX", "US", "minute_history", "1m"),
        ("KOSPI-10.KS", "KR", "minute_history", "5m"),
        ("KOSPI-10.KS", "KR", "hour_history", "1h"),
    ):
        request = {
            "schema_version": 1,
            "request_id": uuid.uuid4().hex,
            "symbol": symbol,
            "market": market,
            "start": None,
            "capability": capability,
        }
        if interval:
            request.update(start=now - 7 * 86400, end=now, interval=interval)
        yield request


def _verify_smoke(raw_response: bytes, request: dict, region: str, parse_source) -> dict:
    try:
        envelope = json.loads(raw_response)
    except (ValueError, UnicodeDecodeError):
        raise FCDeploymentError("Native smoke returned invalid JSON") from None
    if not isinstance(envelope, dict) or any(
        type(envelope.get(key)) is not type(value) or envelope.get(key) != value
        for key, value in request.items()
    ):
        raise FCDeploymentError("Native smoke request identity differs")
    raw, fetched_at, runtime = (
        envelope.get("raw_json"),
        envelope.get("fetched_at"),
        envelope.get("runtime"),
    )
    if (
        not isinstance(raw, str)
        or type(fetched_at) is not int
        or not 0 < fetched_at < 2**63
        or not isinstance(runtime, dict)
        or runtime.get("region") != region
        or not isinstance(runtime.get("fc_request_id"), str)
        or not runtime["fc_request_id"]
        or not isinstance(envelope.get("request_url"), str)
    ):
        raise FCDeploymentError("Native smoke source clocks or provenance are absent")
    url = urlsplit(envelope["request_url"])
    if (
        url.scheme != "https"
        or url.hostname
        not in (
            "query1.finance.yahoo.com",
            "query2.finance.yahoo.com",
        )
        or url.username
        or url.password
        or not url.path.startswith("/v8/finance/chart/")
    ):
        raise FCDeploymentError("Native smoke source URL is not a Yahoo chart")
    source_sha = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    if source_sha != envelope.get("payload_sha256"):
        raise FCDeploymentError("Native smoke source SHA256 differs")
    try:
        source = json.loads(raw)
        chart = source["chart"]
        results = chart["result"]
        if chart.get("error") or not isinstance(results, list) or len(results) != 1:
            raise ValueError()
        meta = results[0]["meta"]
        if meta.get("symbol") != request["symbol"] or meta.get("instrumentType") != "INDEX":
            raise ValueError()
        expected_currency = "USD" if request["market"] == "US" else "KRW"
        if meta.get("currency") != expected_currency:
            raise ValueError()
        timezone = meta.get("exchangeTimezoneName")
        if (
            not isinstance(timezone, str)
            or (request["market"] == "KR" and timezone != "Asia/Seoul")
            or (request["market"] == "US" and not timezone.startswith("America/"))
        ):
            raise ValueError()
        _, points = parse_source(source, request, fetched_at)
        if not points:
            raise ValueError()
        expected_kind = "hour_bar" if request.get("interval") == "1h" else "minute_bar"
        if request["capability"] in ("minute_history", "hour_history") and (
            meta.get("dataGranularity") != request["interval"]
            or not any(point["data_kind"] == expected_kind for point in points)
        ):
            raise ValueError()
    except (ValueError, TypeError, KeyError, AttributeError):
        raise FCDeploymentError("Native smoke source failed local validation") from None
    return {
        "symbol": request["symbol"],
        "market": request["market"],
        "capability": request["capability"],
        "interval": request.get("interval"),
        "source_fetched_at": fetched_at,
        "source_sha256": source_sha,
        "source_bytes": len(raw.encode("utf-8")),
        "points": len(points),
        "minute_bars": sum(point["data_kind"] == "minute_bar" for point in points),
        "hour_bars": sum(point["data_kind"] == "hour_bar" for point in points),
        "currency": expected_currency,
        "exchange_timezone": timezone,
        "fc_request_id": runtime["fc_request_id"],
        "verified": True,
    }


def deploy_release(
    *,
    release_dir: Path,
    region: str = REGION,
    function_name: str = FUNCTION,
    client=None,
    endpoint: str | None = None,
    models_module=None,
    runtime_options=None,
    download=None,
    parse_source=None,
    now: int | None = None,
):
    """Test seams inject only the cloud calls; the CLI always uses official SDKs."""
    if region != REGION or function_name != FUNCTION:
        raise FCDeploymentError("Target differs from the dedicated v15 US Yahoo function")
    os.environ["DEBUG"] = ""
    release_dir = Path(release_dir)
    manifest = verify_release(release_dir)
    receipt = release_dir / "fc-deployment.json"
    receipt.unlink(missing_ok=True)
    parse_source = parse_source or _source_parser(manifest)
    if client is None:
        client, endpoint = _sdk_client(region)
    if not isinstance(endpoint, str) or not endpoint.endswith(f".{region}.fc.aliyuncs.com"):
        raise FCDeploymentError("FC endpoint does not match the deployment region")
    models = models_module or _models()
    options = runtime_options if runtime_options is not None else _runtime_options()
    previous = _successful(client.get_function(function_name, models.GetFunctionRequest()))
    if _field(previous, "function_name") != function_name:
        raise FCDeploymentError("GetFunction returned a different function")
    environment = dict(_field(previous, "environment_variables") or {})
    environment.update(
        {
            SOURCE_REVISION_ENV: manifest["revision"],
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONUNBUFFERED": "1",
        }
    )
    raw_zip = (release_dir / "worker.zip").read_bytes()
    _successful(
        client.update_function(
            function_name,
            models.UpdateFunctionRequest(
                body=models.UpdateFunctionInput(
                    **FUNCTION_SETTINGS,
                    environment_variables=environment,
                    code=models.InputCodeLocation(
                        zip_file=base64.b64encode(raw_zip).decode("ascii")
                    ),
                ),
            ),
        )
    )
    actual = _successful(client.get_function(function_name, models.GetFunctionRequest()))
    if (
        _field(actual, "function_name") != function_name
        or any(
            _field(actual, key) != value
            for key, value in FUNCTION_SETTINGS.items()
            if key != "instance_concurrency"
        )
        or _field(actual, "instance_concurrency")
        not in (None, FUNCTION_SETTINGS["instance_concurrency"])
        or (_field(actual, "environment_variables") or {}).get(SOURCE_REVISION_ENV)
        != manifest["revision"]
    ):
        raise FCDeploymentError("Cloud function settings or source revision differ")
    code = _successful(client.get_function_code(function_name, models.GetFunctionCodeRequest()))
    url = _field(code, "url")
    if not isinstance(url, str) or not url:
        raise FCDeploymentError("Cloud code package has no download location")
    cloud_zip = (download or _download)(url)
    if not isinstance(cloud_zip, bytes) or digest(cloud_zip) != manifest["worker_sha256"]:
        raise FCDeploymentError("Cloud ZIP bytes differ from the release")
    smoke = []
    for request in _smoke_requests(int(time.time()) if now is None else now):
        with io.BytesIO(json.dumps(request, separators=(",", ":")).encode("utf-8")) as stream:
            response = client.invoke_function_with_options(
                function_name,
                models.InvokeFunctionRequest(body=stream),
                models.InvokeFunctionHeaders(x_fc_invocation_type="Sync"),
                options,
            )
        smoke.append(_verify_smoke(_read_native(response), request, region, parse_source))
    result = {
        "schema_version": 1,
        "checked_at": datetime.now(UTC).isoformat(),
        "revision": manifest["revision"],
        "function_name": function_name,
        "region": region,
        "endpoint": endpoint,
        "code_zip_sha256": manifest["worker_sha256"],
        "cloud_zip_sha256": digest(cloud_zip),
        "uploaded_zip_exact_cloud_readback": True,
        "verified": True,
        # This management field is absent in actual us-west-1 readback. Keep
        # the requested value separate from a value verified by the API.
        "instance_concurrency_requested": FUNCTION_SETTINGS["instance_concurrency"],
        "instance_concurrency_readback": _field(actual, "instance_concurrency"),
        "instance_concurrency_confirmed": _field(actual, "instance_concurrency")
        == FUNCTION_SETTINGS["instance_concurrency"],
        "native_smoke": smoke,
    }
    _atomic_write(receipt, json.dumps(result, indent=2).encode("utf-8"))
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release-dir", type=Path, required=True)
    parser.add_argument("--region", default=REGION)
    parser.add_argument("--function", default=FUNCTION)
    args = parser.parse_args(argv)
    # Official SDK DEBUG can print signed request headers, so disable it in this
    # isolated deployment process before lazy-loading any SDK.
    os.environ["DEBUG"] = ""
    try:
        result = deploy_release(
            release_dir=args.release_dir,
            region=args.region,
            function_name=args.function,
        )
    except Exception as exc:
        status = getattr(exc, "status_code", None)
        code = getattr(exc, "code", None)
        code = (
            code if isinstance(code, str) and re.fullmatch(r"[A-Za-z0-9_.-]{1,80}", code) else None
        )
        print(
            json.dumps(
                {
                    "error_type": type(exc).__name__,
                    "code": code,
                    "status_code": status if type(status) is int else None,
                }
            )
        )
        return 1
    print(
        json.dumps(
            {key: result[key] for key in ("revision", "function_name", "region", "verified")}
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
