"""Deployment failure/state behavior. Injected SSH is not production evidence."""

import copy
import hashlib
import importlib.util
import io
import json
import sys
import tarfile
from pathlib import Path
from types import SimpleNamespace

import pytest

FILE = Path(__file__).resolve().parents[2] / "deploy/cross-market/deploy_collectors.py"
SPEC = importlib.util.spec_from_file_location("collectors_release_deploy", FILE)
deploy = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(deploy)

REVISION = "a" * 40
IMAGE = "registry.example/team/trading-service:" + REVISION
IMMUTABLE = "sha256:" + "b" * 64


def test_ssh_output_after_exit_status_is_not_truncated(monkeypatch):
    class Channel:
        closed = False

        def __init__(self):
            self.stdout = [b'{"files":']
            self.stderr = []
            self.close_called = False

        def exec_command(self, command):
            assert command == "python3 snapshot.py"

        def recv_ready(self):
            return bool(self.stdout)

        def recv(self, size):
            return self.stdout.pop(0)

        def recv_stderr_ready(self):
            return bool(self.stderr)

        def recv_stderr(self, size):
            return self.stderr.pop(0)

        def exit_status_ready(self):
            # A process status is not EOF on its stdout/stderr streams.
            return True

        def recv_exit_status(self):
            return 0

        def deliver_remaining_streams(self, seconds):
            self.stdout.append(b'{"key":"value"}}\n')
            self.stderr.append(b"credential-bearing diagnostic must not join stdout")
            self.closed = True

        def close(self):
            self.close_called = True

    channel = Channel()
    remote = deploy.SSHRemote.__new__(deploy.SSHRemote)
    remote.client = SimpleNamespace(
        get_transport=lambda: SimpleNamespace(open_session=lambda **kwargs: channel)
    )
    monkeypatch.setattr(deploy.time, "sleep", channel.deliver_remaining_streams)
    raw = remote.run(["python3", "snapshot.py"])
    assert raw == b'{"files":{"key":"value"}}\n'
    assert json.loads(raw) == {"files": {"key": "value"}}
    assert channel.close_called


def test_snapshot_invalid_json_has_safe_stage_diagnostics():
    raw = b"invalid source AK=never-emit-this-credential"
    collector = deploy.CollectorDeployer.__new__(deploy.CollectorDeployer)
    collector.remote = SimpleNamespace(run=lambda argv: raw)
    collector.root = "/opt/ashare-cross-market"
    with pytest.raises(deploy.DeploymentError) as caught:
        collector.snapshot()
    assert caught.value.code == "RemoteJSONInvalid"
    assert caught.value.phase == "collector_state_snapshot"
    assert caught.value.stdout_bytes == len(raw)
    assert caught.value.json_error_position == 0
    assert "never-emit" not in str(caught.value)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


@pytest.fixture
def release(tmp_path):
    root = tmp_path / "release"
    root.mkdir()
    reference = {
        "indices": [
            {"market": "US", "symbol": "^TEST"},
            {"market": "KR", "symbol": "KOSPI-TEST.KS"},
        ],
        "industries": [{"markets": {"US": {}, "KR": {}}}],
    }
    reference_bytes = json.dumps(reference).encode()
    capability = {
        "base_reference_sha256": sha(reference_bytes),
        "limits": {"1h": {"update_mode": "history_seed"}},
        "indices": [
            {**item, "intervals": ["1m", "5m"] + (["1h"] if item["market"] == "KR" else [])}
            for item in reference["indices"]
        ],
    }
    content = {
        "src/data/reference/cross_market/industry_indices.json": reference_bytes,
        "src/data/reference/cross_market/intraday_indices.json": json.dumps(capability).encode(),
        "vendor/library.py": b"value=1\n",
    }
    with tarfile.open(root / "runtime.tar.gz", "w:gz") as archive:
        for name, raw in content.items():
            member = tarfile.TarInfo(name)
            member.size = len(raw)
            archive.addfile(member, io.BytesIO(raw))
    (root / "worker.zip").write_bytes(b"worker artifact fixture")
    (root / "docker-compose.yml").write_text("services: {}\n")
    files = {name: sha(raw) for name, raw in content.items()}
    manifest = {
        "schema_version": 1,
        "revision": REVISION,
        "files": files,
        "bundle_sha256": sha(json.dumps(files, sort_keys=True, separators=(",", ":")).encode()),
        "runtime_archive_sha256": sha((root / "runtime.tar.gz").read_bytes()),
        "worker_sha256": sha((root / "worker.zip").read_bytes()),
        "worker_files": {},
        "compose_sha256": sha((root / "docker-compose.yml").read_bytes()),
    }
    (root / "manifest.json").write_text(json.dumps(manifest))
    fc = {
        "revision": REVISION,
        "function_name": "ashare_yahoo_indices_v15",
        "region": "us-west-1",
        "endpoint": "account.us-west-1.fc.aliyuncs.com",
        "verified": True,
        "uploaded_zip_exact_cloud_readback": True,
        "code_zip_sha256": manifest["worker_sha256"],
        "cloud_zip_sha256": manifest["worker_sha256"],
        "native_smoke": [{"fixture": True}],
    }
    (root / "fc-deployment.json").write_text(json.dumps(fc))
    return root


def state_fixture():
    path = "state-intraday-fc/existing.state.json"
    pending = "state-intraday-fc/work.pending.json"
    return {
        "files": {path: "unchanged", pending: "saved-source-sha"},
        "states": {
            path: {
                "identity": {"interval": "1m", "symbol": "KOSPI-TEST.KS"},
                "backfill_complete_until_s": 125,
                "covered_until_s": 125,
            }
        },
        "pending": {pending: {"end_s": 120}},
    }


class Remote:
    """Record commands and control failure boundaries without contacting a host."""

    def __init__(self, release):
        self.release = deploy.validate_release(release, IMAGE)
        self.calls, self.uploads = [], {}
        self.snapshot_count = 0
        self.fail = None
        self.failed_once = False
        self.logs_valid = True
        self.seed_target = 300

    def upload(self, raw, path):
        self.uploads[path] = raw

    def cycle(self, minute):
        manifest = self.release["manifest"]
        expected = self.release["minute" if minute else "daily"]
        results = [
            {
                "market": key[0],
                "symbol": key[1],
                "status": "verified",
                "rows": 2,
                **(
                    {
                        "interval": key[2],
                        "covered_until_s": 300,
                        "backfill_complete_until_s": 300,
                        "windows": [{"rows": 2, "verified_rows": 2, "source_sha256": "c" * 64}],
                    }
                    if minute
                    else {}
                ),
            }
            for key in sorted(expected)
        ]
        cycle = {
            "status": "verified",
            "reference_sha256": manifest["files"][
                "src/data/reference/cross_market/industry_indices.json"
            ],
            "results" if minute else "indices": results,
        }
        if minute:
            cycle.update(
                run_end_s=300,
                capability_sha256=manifest["files"][
                    "src/data/reference/cross_market/intraday_indices.json"
                ],
            )
        else:
            cycle["mapping"] = {"status": "previously_verified"}
        return cycle

    def run(self, args):
        self.calls.append(args)
        text = " ".join(args)
        if self.fail == "uploaded_sha" and "assert all(hashlib" in text:
            raise deploy.DeploymentError("uploaded artifact corrupt")
        if args[:3] == ["docker", "image", "inspect"]:
            return json.dumps([{"Id": IMMUTABLE}]).encode()
        if "p.read_text() if p.exists()" in text:
            return json.dumps(
                "CROSS_MARKET_INTRADAY_BATCH_SIZE=37\nCROSS_MARKET_LOOP_SECONDS=91\n"
            ).encode()
        if "def _extract(" in text:
            return json.dumps({"files_verified": len(self.release["manifest"]["files"])}).encode()
        if "def _snapshot(" in text:
            self.snapshot_count += 1
            state = state_fixture()
            if self.snapshot_count > 2:
                # New hour state is allowed. Existing completed-minute markers
                # survive, and the old pending has actually advanced its cursor.
                state["states"]["state-intraday-fc/hour.state.json"] = {
                    "identity": {"interval": "1h", "market": "KR", "symbol": "KOSPI-TEST.KS"},
                    "backfill_complete_until_s": self.seed_target,
                    "covered_until_s": self.seed_target,
                    "last_verified_window": {"end_s": self.seed_target},
                }
                state["pending"] = {}
                state["states"]["state-intraday-fc/work.state.json"] = {"covered_until_s": 120}
            return json.dumps(state).encode()
        if args[:3] == ["docker", "ps", "-a"]:
            return b""
        if args[:2] == ["docker", "inspect"]:
            if args[2:] == ["root_greptimedb_1"]:
                return json.dumps(
                    [
                        {
                            "Image": "original-db",
                            "RestartCount": 0,
                            "State": {"StartedAt": "unchanged"},
                        }
                    ]
                ).encode()
            actuals = []
            for name in args[2:]:
                is_daily = name == deploy.NAMES[0]
                actuals.append(
                    {
                        "Name": "/" + name,
                        "Image": IMMUTABLE,
                        "State": {"Running": True},
                        "Config": {
                            "Env": ["HTTP_PROXY=", "DEBUG="],
                            "Labels": {
                                "org.ashare.cross-market.source-revision": REVISION,
                                "org.ashare.cross-market.bundle-sha256": self.release["manifest"][
                                    "bundle_sha256"
                                ],
                            },
                        },
                        "HostConfig": {
                            "ReadonlyRootfs": True,
                            "RestartPolicy": {"Name": "unless-stopped"},
                        },
                        "Mounts": [
                            {
                                "Destination": "/collector",
                                "Source": "/opt/ashare-cross-market/runtime",
                                "RW": False,
                            },
                            {
                                "Destination": "/state",
                                "Source": "/opt/ashare-cross-market/"
                                + ("state-fc" if is_daily else "state-intraday-fc"),
                                "RW": True,
                            },
                        ],
                    }
                )
            return json.dumps(actuals).encode()
        if " up " in " " + text + " " and self.fail == "up" and not self.failed_once:
            self.failed_once = True
            raise deploy.DeploymentError("partial compose up failure")
        if " stop " in " " + text + " " and self.fail == "stop" and not self.failed_once:
            self.failed_once = True
            raise deploy.DeploymentError("partial compose stop failure")
        if args[:2] == ["docker", "logs"]:
            return (
                json.dumps(self.cycle(args[2] == deploy.NAMES[1]))
                if self.logs_valid
                else "starting"
            ).encode()
        if args[:2] == ["docker", "exec"] and args[-1] == deploy.READBACK:
            count = len(self.release["daily" if args[2] == deploy.NAMES[0] else "minute"])
            return json.dumps(
                {
                    "price_keys_verified": count,
                    "price_fields_read": 18,
                    "mapping_rows_verified": self.release["mapping_count"],
                }
            ).encode()
        return b""


def collector_stops(remote):
    return [a for a in remote.calls if a[0] == "docker-compose" and "stop" in a]


def test_wrong_local_sha_never_opens_or_stops_remote(release):
    remote = Remote(release)
    (release / "runtime.tar.gz").write_bytes(b"corrupt after building")
    with pytest.raises(ValueError, match="artifact SHA256"):
        deploy.CollectorDeployer(remote, release, IMAGE, "/opt/ashare-cross-market")
    assert remote.calls == []


def test_uploaded_sha_failure_occurs_before_any_collector_stop(release):
    remote = Remote(release)
    remote.fail = "uploaded_sha"
    job = deploy.CollectorDeployer(remote, release, IMAGE, "/opt/ashare-cross-market")
    with pytest.raises(deploy.DeploymentError):
        job.deploy(b"private-env\n")
    assert collector_stops(remote) == []


@pytest.mark.parametrize(
    "name,kind",
    [
        ("../escape", "file"),
        ("/absolute", "file"),
        ("vendor/link", "symlink"),
        ("vendor/link", "hardlink"),
    ],
)
def test_traversal_and_links_rejected_before_extraction(tmp_path, name, kind):
    path = tmp_path / "hostile.tar.gz"
    with tarfile.open(path, "w:gz") as stream:
        member = tarfile.TarInfo(name)
        if kind != "file":
            member.type = tarfile.SYMTYPE if kind == "symlink" else tarfile.LNKTYPE
            member.linkname = "outside"
            stream.addfile(member)
        else:
            member.size = 1
            stream.addfile(member, io.BytesIO(b"x"))
    with pytest.raises(ValueError):
        deploy.validate_archive(path, {name: sha(b"x")})


def test_duplicate_archive_member_does_not_overwrite_verified_file(tmp_path):
    path = tmp_path / "duplicate.tar.gz"
    with tarfile.open(path, "w:gz") as stream:
        for raw in (b"a", b"b"):
            member = tarfile.TarInfo("src/file.py")
            member.size = 1
            stream.addfile(member, io.BytesIO(raw))
    with pytest.raises(ValueError, match="duplicate"):
        deploy.validate_archive(path, {"src/file.py": sha(b"a")})


def test_partial_filesystem_switch_rolls_back_config_and_runtime_not_state(tmp_path, monkeypatch):
    root, stage = tmp_path / "host", tmp_path / "stage"
    root.mkdir()
    stage.mkdir()
    for base, prefix in ((root, "old"), (stage, "new")):
        (base / "runtime").mkdir()
        (base / "runtime/code.py").write_text(prefix)
        for name in deploy.CONFIG_FILES[1:]:
            (base / name).write_text(prefix + name)
    (root / "state-intraday-fc").mkdir()
    state = root / "state-intraday-fc/saved.state.json"
    pending = root / "state-intraday-fc/saved.pending.json"
    state.write_text('{"backfill_complete_until_s":125}')
    pending.write_bytes(b"original source pending bytes")
    original_replace = deploy.os.replace

    def fail_mid_switch(source, target):
        if Path(source) == stage / ".env":
            raise OSError("cutover interrupted")
        return original_replace(source, target)

    monkeypatch.setattr(deploy.os, "replace", fail_mid_switch)
    with pytest.raises(OSError):
        deploy._switch(str(root), str(stage))
    deploy._rollback(str(root), str(stage))
    assert (root / "runtime/code.py").read_text() == "old"
    assert all((root / name).read_text() == "old" + name for name in deploy.CONFIG_FILES[1:])
    assert state.read_text() == '{"backfill_complete_until_s":125}'
    assert pending.read_bytes() == b"original source pending bytes"


def test_compose_v1_rotation_uses_one_actual_image_and_preserves_defaults(release):
    remote = Remote(release)
    receipt = deploy.CollectorDeployer(remote, release, IMAGE, "/opt/ashare-cross-market").deploy(
        b"private"
    )
    assert receipt["status"] == "verified"
    assert receipt["immutable_image_id"] == IMMUTABLE
    operations = [
        a for a in remote.calls if a[0] == "docker-compose" and a[-2:] == list(deploy.SERVICES)
    ]
    assert [next(x for x in ("stop", "rm", "up") if x in a) for a in operations] == [
        "stop",
        "rm",
        "up",
    ]
    assert "--no-deps" in operations[-1] and "-f" in operations[1]
    assert all("root_greptimedb_1" not in a for a in operations)
    env = next(raw for path, raw in remote.uploads.items() if path.endswith("/.env"))
    assert b"CROSS_MARKET_RUNTIME_IMAGE=" + IMMUTABLE.encode() in env
    assert b"CROSS_MARKET_INTRADAY_BATCH_SIZE=37" in env and b"CROSS_MARKET_LOOP_SECONDS=91" in env
    assert (
        receipt["readbacks"][1]["price_keys_verified"] == 5
    )  # Includes new KR1h scope dynamically.


def test_partial_compose_up_restores_old_release_before_retry(release):
    remote = Remote(release)
    remote.fail = "up"
    with pytest.raises(deploy.DeploymentError):
        deploy.CollectorDeployer(remote, release, IMAGE, "/opt/ashare-cross-market").deploy(
            b"private"
        )
    assert any("def _rollback(" in " ".join(a) for a in remote.calls)
    ups = [a for a in remote.calls if a[0] == "docker-compose" and "up" in a]
    assert len(ups) == 2 and all("--no-deps" in a for a in ups)


def test_partial_stop_restores_old_running_services_before_switch(release):
    remote = Remote(release)
    remote.fail = "stop"
    with pytest.raises(deploy.DeploymentError):
        deploy.CollectorDeployer(remote, release, IMAGE, "/opt/ashare-cross-market").deploy(
            b"private"
        )
    assert any(a[0] == "docker-compose" and "up" in a for a in remote.calls)
    assert not any("def _switch(" in " ".join(a) for a in remote.calls)


def test_remote_helper_extracts_and_verifies_real_manifest_files(release, tmp_path):
    import shutil

    stage = tmp_path / "uploaded"
    stage.mkdir()
    shutil.copyfile(release / "runtime.tar.gz", stage / "runtime.tar.gz")
    shutil.copyfile(release / "manifest.json", stage / "runtime-manifest.json")
    deploy._extract(str(stage))
    manifest = json.loads((release / "manifest.json").read_text())
    assert all(
        sha((stage / "runtime" / path).read_bytes()) == expected
        for path, expected in manifest["files"].items()
    )


@pytest.mark.parametrize(
    "field,value", [("revision", "b" * 40), ("verified", False), ("cloud_zip_sha256", "wrong")]
)
def test_other_or_unverified_cloud_release_cannot_stop_collectors(release, field, value):
    remote = Remote(release)
    path = release / "fc-deployment.json"
    receipt = json.loads(path.read_text())
    receipt[field] = value
    path.write_text(json.dumps(receipt))
    with pytest.raises(ValueError, match="verified deployment"):
        deploy.CollectorDeployer(remote, release, IMAGE, "/opt/ashare-cross-market")
    assert remote.calls == []


def test_timeout_is_not_success_or_rollback_and_collectors_continue(release):
    remote = Remote(release)
    remote.logs_valid = False
    with pytest.raises(deploy.VerificationTimeout):
        deploy.CollectorDeployer(remote, release, IMAGE, "/opt/ashare-cross-market").deploy(
            b"private", verify_timeout=0.01, poll_seconds=0.001
        )
    receipt = json.loads((release / "collectors-deployment.json").read_text())
    assert receipt["status"] == "running_verification_incomplete"
    assert len(collector_stops(remote)) == 1
    assert not any("def _rollback(" in " ".join(a) for a in remote.calls)


def test_default_window_accepts_both_complete_cycles_after_first_fifteen_minutes(
    release, monkeypatch
):
    remote = Remote(release)
    ticks = iter((0, 1000))
    monkeypatch.setattr(deploy.time, "monotonic", lambda: next(ticks, 1000))
    receipt = deploy.CollectorDeployer(remote, release, IMAGE, "/opt/ashare-cross-market").deploy(
        b"private"
    )
    assert receipt["status"] == "verified"
    assert len(receipt["completed_cycles"]) == 2
    assert len(receipt["completed_cycles"][0]["indices"]) == 2
    assert len(receipt["completed_cycles"][1]["results"]) == 5
    assert [r["price_keys_verified"] for r in receipt["readbacks"]] == [2, 5]


def test_domestic_cli_forwards_default_complete_cycle_window(release, monkeypatch, capsys):
    received = []
    monkeypatch.setattr(deploy, "credentials", lambda env: b"private")
    monkeypatch.setattr(deploy, "SSHRemote", lambda env: SimpleNamespace(close=lambda: None))

    class Collector:
        def __init__(self, *args):
            pass

        def deploy(self, private, timeout):
            received.append(timeout)
            return {
                "status": "verified",
                "revision": REVISION,
                "bundle_sha256": "c" * 64,
                "immutable_image_id": IMMUTABLE,
            }

    monkeypatch.setattr(deploy, "CollectorDeployer", Collector)
    monkeypatch.setattr(
        sys,
        "argv",
        ["deploy_collectors.py", "--release-dir", str(release), "--runtime-image", IMAGE],
    )
    deploy.main()
    assert received == [1800]
    assert json.loads(capsys.readouterr().out)["status"] == "verified"


def test_completed_hour_seed_is_reused_without_advancing_its_original_target(release):
    remote = Remote(release)
    remote.seed_target = 250
    original_cycle = remote.cycle

    def cycles(minute):
        value = original_cycle(minute)
        if minute:
            hour = next(r for r in value["results"] if r["interval"] == "1h")
            hour.update(
                status="previously_verified",
                rows=0,
                windows=[],
                collection_mode="history_seed",
                covered_until_s=250,
                backfill_complete_until_s=250,
                last_verified_window={"end_s": 250},
                gap_timestamps=[230000],
            )
        return value

    remote.cycle = cycles
    receipt = deploy.CollectorDeployer(remote, release, IMAGE, "/opt/ashare-cross-market").deploy(
        b"private", verify_timeout=0.01, poll_seconds=0.001
    )
    assert receipt["status"] == "verified"
    assert receipt["history_seed_series"] == 1 and receipt["continuous_series"] == 4
    hour = next(r for r in receipt["completed_cycles"][1]["results"] if r["interval"] == "1h")
    assert hour["covered_until_s"] == hour["backfill_complete_until_s"] == 250
    assert hour["gap_timestamps"] == [230000]


@pytest.mark.parametrize("change", ["continuous_skip", "missing_initial", "wrong_mode"])
def test_seed_skip_cannot_stand_in_for_continuous_or_unverified_data(release, change):
    remote = Remote(release)
    cycle = remote.cycle(True)
    selected = next(
        r
        for r in cycle["results"]
        if r["interval"] == ("1m" if change == "continuous_skip" else "1h")
    )
    selected.update(
        status="previously_verified",
        rows=0,
        windows=[],
        collection_mode="history_seed",
        covered_until_s=250,
        backfill_complete_until_s=None if change == "missing_initial" else 250,
    )
    if change == "wrong_mode":
        selected["collection_mode"] = "continuous"
    manifest = remote.release["manifest"]
    assert (
        deploy.cycle_from_logs(
            json.dumps(cycle).encode(),
            remote.release["minute"],
            manifest["files"]["src/data/reference/cross_market/industry_indices.json"],
            manifest["files"]["src/data/reference/cross_market/intraday_indices.json"],
            remote.release["history_seed"],
        )
        is None
    )


@pytest.mark.parametrize(
    "change",
    [
        "missing_identity",
        "failed",
        "wrong_readback",
        "wrong_hash",
        "old_target",
        "no_source_window",
    ],
)
def test_partial_or_different_source_cycle_cannot_pass(release, change):
    remote = Remote(release)
    cycle = remote.cycle(True)
    if change == "missing_identity":
        cycle["results"].pop()
    elif change == "failed":
        cycle["results"][0]["status"] = "failed"
    elif change == "wrong_readback":
        cycle["results"][0]["windows"][0]["verified_rows"] = 1
    elif change == "wrong_hash":
        cycle["capability_sha256"] = "wrong"
    elif change == "old_target":
        cycle["results"][0]["covered_until_s"] = 299
    else:
        cycle["results"][0]["windows"] = []
    manifest = remote.release["manifest"]
    result = deploy.cycle_from_logs(
        json.dumps(cycle).encode(),
        remote.release["minute"],
        manifest["files"]["src/data/reference/cross_market/industry_indices.json"],
        manifest["files"]["src/data/reference/cross_market/intraday_indices.json"],
    )
    assert result is None


def test_new_hour_states_allowed_but_completed_minute_marker_cannot_reset():
    before = state_fixture()
    after = copy.deepcopy(before)
    after["states"]["state-intraday-fc/hour.state.json"] = {"backfill_complete_until_s": 150}
    assert deploy.state_preserved(before, after)
    after["states"]["state-intraday-fc/existing.state.json"]["backfill_complete_until_s"] = None
    assert not deploy.state_preserved(before, after)


def test_pending_can_disappear_only_after_its_coverage_is_verified():
    before, after = state_fixture(), state_fixture()
    after["pending"] = {}
    assert not deploy.state_preserved(before, after)
    after["states"]["state-intraday-fc/work.state.json"] = {"covered_until_s": 120}
    assert deploy.state_preserved(before, after)


@pytest.mark.parametrize(
    "image",
    [
        "registry/trading:latest",
        "registry/trading:" + "c" * 40,
        "registry/trading:" + REVISION + ";touch /tmp/pwn",
    ],
)
def test_wrong_version_and_shell_injected_image_are_rejected(release, image):
    with pytest.raises(ValueError, match="commit tag"):
        deploy.validate_release(release, image)


def test_ssh_shell_quoting_is_argument_preserving_not_interpolation():
    import shlex

    argv = ["python3", "-c", "print('literal $HOME;`whoami`')", "x'; touch /tmp/pwn;'"]
    assert shlex.split(deploy.shlex.join(argv)) == argv


def test_ssh_requires_known_host_and_never_adds_unknown_key(monkeypatch):
    client = SimpleNamespace(
        set_missing_host_key_policy=lambda policy: setattr(client, "policy", policy),
        load_host_keys=lambda path: setattr(client, "loaded", Path(path).read_text()),
        connect=lambda *a, **kw: setattr(client, "connection", kw),
    )
    reject = object()
    monkeypatch.setitem(
        sys.modules,
        "paramiko",
        SimpleNamespace(SSHClient=lambda: client, RejectPolicy=lambda: reject),
    )
    deploy.SSHRemote(
        {
            "CROSS_MARKET_SSH_HOST": "host",
            "CROSS_MARKET_SSH_USER": "user",
            "CROSS_MARKET_SSH_PASSWORD": "secret",
            "CROSS_MARKET_SSH_KNOWN_HOSTS": "host ssh-ed25519 public-key",
        }
    )
    assert client.policy is reject and "public-key" in client.loaded
    assert client.connection["allow_agent"] is False and client.connection["look_for_keys"] is False
    with pytest.raises(ValueError, match="known_hosts"):
        deploy.SSHRemote({"CROSS_MARKET_SSH_HOST": "host"})


def test_credentials_aliases_and_newline_injection_do_not_leak_values():
    assert deploy.credentials(
        {"ALIBABA_CLOUD_ACCESS_KEY_ID": "key", "ALIBABA_CLOUD_ACCESS_KEY_SECRET": "secret"}
    ) == (b"ALIYUN_ACCESS_KEY_ID=key\nALIYUN_ACCESS_KEY_SECRET=secret\n")
    with pytest.raises(ValueError) as error:
        deploy.credentials(
            {"ALIYUN_ACCESS_KEY_ID": "key", "ALIYUN_ACCESS_KEY_SECRET": "secret\nX=1"}
        )
    assert "secret" not in str(error.value)


def test_upload_sets_private_permission_without_logging_credentials():
    written = {}
    sftp = SimpleNamespace(
        putfo=lambda stream, path: written.update({path: stream.read()}),
        chmod=lambda path, mode: written.update({"mode": mode}),
        close=lambda: None,
    )
    remote = object.__new__(deploy.SSHRemote)
    remote.client = SimpleNamespace(open_sftp=lambda: sftp)
    remote.upload(b"private-secret", "/stage/fc.credentials.env")
    assert written["/stage/fc.credentials.env"] == b"private-secret" and written["mode"] == 0o600


@pytest.mark.parametrize(
    "change", [None, "business_json", "missing_key", "duplicate_key", "invalid_clock"]
)
def test_actual_readback_uses_persisted_mapping_clock_and_checks_business_fields(
    monkeypatch, capsys, change
):
    # Static mappings may have been verified days before this deployment. The
    # deployment's new inspection clock is not a new source observation.
    import src.data.cross_market_ingest as ingest_module
    import src.data.cross_market_store as store_module

    mappings = [
        dict(
            provider="yahoo",
            market=market,
            sw_code="110100",
            reference_at=1000,
            mapping_json={"status": "matched", "sources": ["official"]},
            fetched_at=9000,
        )
        for market in ("US", "KR")
    ]
    reference = {"indices": [{"market": "US", "symbol": "^TEST"}], "mappings": mappings}
    persisted = [store_module._normalise_reference({**r, "fetched_at": 2000}) for r in mappings]
    if change == "business_json":
        persisted[0]["mapping_json"] = '{"status":"wrong_index"}'
    elif change == "missing_key":
        persisted.pop()
    elif change == "duplicate_key":
        persisted.append(copy.deepcopy(persisted[0]))
    elif change == "invalid_clock":
        persisted[0]["fetched_at"] = True
    state = {
        "identity": {"market": "US", "symbol": "^TEST"},
        "capability": "daily_history",
        "last_verified_ms": 1000,
    }
    price = {field: None for field in store_module.PRICE_COLUMNS}
    price.update(
        provider="yahoo", market="US", symbol="^TEST", interval="1d", data_kind="daily_bar", ts=1000
    )
    calls = []

    class ReadOnlyStore(store_module.CrossMarketStore):
        def __init__(self, *args, **kwargs):
            pass

        async def aclose(self):
            pass

        async def read_industry_indices(self, references):
            calls.append("mapping_read")
            return copy.deepcopy(persisted)

        async def read_prices(self, keys):
            calls.append("price_read")
            return [price]

    original_read_text = Path.read_text
    original_glob = Path.glob

    def fake_read_text(path, *args, **kwargs):
        if path == Path("/state/one.state.json"):
            return json.dumps(state)
        if path == Path("/collector/src/data/reference/cross_market/intraday_indices.json"):
            return json.dumps({"indices": []})
        return original_read_text(path, *args, **kwargs)

    monkeypatch.setattr(ingest_module, "load_reference", lambda *args: reference)
    monkeypatch.setattr(store_module, "CrossMarketStore", ReadOnlyStore)
    monkeypatch.setattr(Path, "read_text", fake_read_text)
    monkeypatch.setattr(
        Path,
        "glob",
        lambda path, pattern: (
            iter([Path("/state/one.state.json")])
            if path == Path("/state")
            else original_glob(path, pattern)
        ),
    )
    if change is None:
        exec(deploy.READBACK, {})
        assert json.loads(capsys.readouterr().out)["mapping_rows_verified"] == 2
        assert calls == ["mapping_read", "price_read"]
    else:
        with pytest.raises((store_module.GreptimeReadbackError, AssertionError, ValueError)):
            exec(deploy.READBACK, {})
        assert calls == ["mapping_read"]
