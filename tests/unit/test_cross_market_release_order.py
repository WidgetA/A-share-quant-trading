"""Real commit ancestry and stubbed deployment calls, without any remote write."""

import importlib.util
import json
import os
import sys
from types import SimpleNamespace

import pytest

from tests.unit.test_cross_market_release import DEPLOY_DIR, RELEASE, git, make_release_inputs

sys.path.insert(0, str(DEPLOY_DIR))
SPEC = importlib.util.spec_from_file_location(
    "cross_market_deploy_release", DEPLOY_DIR / "deploy_release.py"
)
ORDER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ORDER)


def advance(repository, name="newer B"):
    (repository / "README.md").write_text(name)
    git(repository, "add", "README.md")
    git(
        repository,
        "-c",
        "user.name=Order test",
        "-c",
        "user.email=order@example.invalid",
        "-c",
        "commit.gpgsign=false",
        "commit",
        "-qm",
        name,
    )
    return git(repository, "rev-parse", "HEAD").decode().strip()


@pytest.fixture
def release(tmp_path):
    inputs = make_release_inputs(tmp_path)
    manifest = RELEASE.build_release(**inputs)
    return SimpleNamespace(**inputs, manifest=manifest)


class Remote:
    def __init__(self, revisions):
        self.revisions, self.calls = revisions, []

    def run(self, argv):
        self.calls.append(argv)
        assert argv[0] == "python3" and argv[1] == "-c"
        assert ".Config.Env" not in argv[2]
        assert json.loads(argv[-1]) == list(ORDER.NAMES)
        return json.dumps(self.revisions).encode()


def execute(release, head, deployed, *, fc_error=False):
    events = []
    release.events = events
    client = SimpleNamespace(
        get_function=lambda name, request: SimpleNamespace(
            status_code=200,
            body={
                "function_name": ORDER.FUNCTION,
                "environment_variables": {
                    ORDER.SOURCE_REVISION_ENV: deployed.get("fc"),
                    "SECRET": "never_print_this",
                },
            },
        )
    )
    remote = Remote({k: v for k, v in deployed.items() if k != "fc"})

    def fc(**kwargs):
        events.append("fc")
        if fc_error:
            raise RuntimeError("private failure never printed")
        return {"verified": True}

    class Collectors:
        def __init__(self, *args):
            events.append("construct_collectors")

        def deploy(self, private, verify_timeout):
            assert private == b"private_dummy_credentials"
            assert verify_timeout == 900
            events.append("collectors")
            return {"status": "verified"}

    result = ORDER.deploy_all(
        release_dir=release.output_dir,
        source_root=release.source_root,
        runtime_image="registry.invalid/ns/trading-service:" + release.revision,
        head=head,
        client=client,
        endpoint="123.us-west-1.fc.aliyuncs.com",
        remote=remote,
        private=b"private_dummy_credentials",
        models=SimpleNamespace(GetFunctionRequest=SimpleNamespace),
        fc_deploy=fc,
        collector_factory=Collectors,
    )
    return result, events, remote


@pytest.mark.parametrize("newer_target", ["fc", "host", "daily", "intraday"])
def test_late_ancestor_A_cannot_overwrite_any_already_deployed_B(release, newer_target):
    newer = advance(release.source_root)
    result, events, remote = execute(release, newer, {newer_target: newer})
    assert result["status"] == "superseded" and result["reason"] == "deployed_descendant"
    assert result["newer_deployed"] == {newer_target: newer}
    assert events == []
    assert len(remote.calls) == 1
    assert not (release.output_dir / "fc-deployment.json").exists()


def test_doc_only_new_head_does_not_block_legitimate_A_retry(release):
    newer = advance(release.source_root, "documentation only")
    result, events, _ = execute(release, newer, {"fc": release.revision, "host": release.revision})
    assert result["status"] == "verified"
    assert events == ["fc", "construct_collectors", "collectors"]


def test_partial_new_release_can_finish_without_downgrading(release):
    older = release.revision
    newer = advance(release.source_root)
    release.revision = newer
    inputs = {
        k: getattr(release, k)
        for k in ["source_root", "revision", "output_dir", "worker_vendor", "collector_vendor"]
    }
    release.manifest = RELEASE.build_release(**inputs)
    result, events, _ = execute(
        release, newer, {"fc": newer, "host": older, "daily": older, "intraday": older}
    )
    assert result["status"] == "verified" and events[-1] == "collectors"


def test_FC_failure_never_starts_domestic_cutover(release):
    with pytest.raises(RuntimeError):
        execute(release, release.revision, {"host": release.revision}, fc_error=True)
    receipt = json.loads((release.output_dir / "release-order.json").read_text())
    assert receipt["status"] == "ready"
    assert not receipt.get("collectors_verified")
    assert release.events == ["fc"]


def test_cli_failure_does_not_print_provider_message_or_arbitrary_code(monkeypatch, capsys):
    class Failure(RuntimeError):
        code = "https://private.invalid/?Signature=secret"

    def fail(argv):
        raise Failure("credential endpoint private-secret")

    monkeypatch.setattr(ORDER, "main", fail)
    assert ORDER.cli([]) == 1
    assert json.loads(capsys.readouterr().out) == {
        "status": "failed",
        "error_type": "Failure",
        "code": "DeploymentFailed",
    }


def test_unversioned_legacy_target_can_receive_first_ordered_release(release):
    result, events, _ = execute(release, release.revision, {})
    assert result["status"] == "verified" and events[0] == "fc"


def test_sdk_debug_is_disabled_before_management_read(release, monkeypatch):
    monkeypatch.setenv("DEBUG", "verbose_sdk_credentials")
    original = ORDER.deployed_revisions

    def checked(*args):
        assert os.environ["DEBUG"] == ""
        return original(*args)

    monkeypatch.setattr(ORDER, "deployed_revisions", checked)
    execute(release, release.revision, {})


def test_unknown_revision_never_reaches_deployment(release):
    with pytest.raises(ValueError):
        ORDER.release_decision(
            release.source_root, release.revision, release.revision, {"fc": "unknown"}
        )


def test_superseded_branch_candidate_is_not_released(release):
    repository = release.source_root
    git(repository, "checkout", "--orphan", "replacement")
    git(repository, "rm", "-rf", ".")
    (repository / "README.md").write_text("new lineage")
    git(repository, "add", "README.md")
    git(
        repository,
        "-c",
        "user.name=Order test",
        "-c",
        "user.email=order@example.invalid",
        "-c",
        "commit.gpgsign=false",
        "commit",
        "-qm",
        "replacement",
    )
    head = git(repository, "rev-parse", "HEAD").decode().strip()
    result, events, _ = execute(release, head, {"host": head})
    assert result["reason"] == "no_longer_on_branch" and events == []


def test_other_branch_and_wrong_image_fail_before_remote_calls(release):
    with pytest.raises(ORDER.ReleaseOrderError):
        ORDER.deploy_all(
            release_dir=release.output_dir,
            source_root=release.source_root,
            runtime_image="registry.invalid/ns/trading-service:" + release.revision,
            branch="main",
        )
    with pytest.raises(ORDER.ReleaseOrderError):
        ORDER.deploy_all(
            release_dir=release.output_dir,
            source_root=release.source_root,
            runtime_image="registry.invalid/ns/trading-service:" + "f" * 40,
        )
