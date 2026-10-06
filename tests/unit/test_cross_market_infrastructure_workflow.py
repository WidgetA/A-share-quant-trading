"""The v15 push range releases index infrastructure without deploying training docs."""

import importlib
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]


def workflow():
    return yaml.load(
        (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8"), Loader=yaml.BaseLoader
    )


def detector():
    return importlib.import_module("scripts.cross_market_release_changes")


def git(repository, *arguments):
    return subprocess.check_output(
        ["git", "-C", str(repository), *arguments], stderr=subprocess.PIPE, text=True
    ).strip()


@pytest.fixture
def repository(tmp_path):
    git(tmp_path, "init", "-q")
    git(tmp_path, "config", "user.name", "Workflow test")
    git(tmp_path, "config", "user.email", "workflow-test@example.invalid")
    return tmp_path


def commit(repository, files):
    for name, content in files.items():
        path = repository / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    git(repository, "add", "-A")
    git(repository, "-c", "commit.gpgsign=false", "commit", "-qm", "test change")
    return git(repository, "rev-parse", "HEAD")


def test_v15_release_waits_for_image_and_native_fc():
    job = workflow()["jobs"]["deploy-cross-market"]
    assert {"lint", "test", "build-and-push", "release-changes"} <= set(job["needs"])
    assert "refs/heads/refactor/cleanup-v15-only" in job["if"]
    assert "refs/heads/main" not in job["if"]
    assert "github.event_name == 'push'" in job["if"]
    assert "vars.ACR_ENABLED == 'true'" in job["if"]
    assert "needs.release-changes.outputs.cross_market == 'true'" in job["if"]
    steps = [step.get("run", "") for step in job["steps"]]
    assert any("deploy_release.py" in value for value in steps)
    assert not any(step.get("continue-on-error") == "true" for step in job["steps"])
    assert any("trading-service:${{ github.sha }}" in str(step.get("env")) for step in job["steps"])
    assert job["concurrency"]["cancel-in-progress"] == "false"
    assert job["concurrency"]["queue"] == "max"


def test_workflow_uses_complete_push_range_for_both_deployments():
    jobs = workflow()["jobs"]
    changes = jobs["release-changes"]
    assert any(step.get("with", {}).get("fetch-depth") == "0" for step in changes["steps"])
    run = "\n".join(step.get("run", "") for step in changes["steps"])
    assert "cross_market_release_changes.py" in run
    assert "--before" in run and "--after" in run
    assert any(
        step.get("env", {}).get("PUSH_BEFORE") == "${{ github.event.before }}"
        for step in changes["steps"]
    )
    training = jobs["deploy-serverless"]
    assert "release-changes" in training["needs"]
    assert "needs.release-changes.outputs.training == 'true'" in training["if"]
    assert "HEAD~1" not in str(training)


def test_multiple_commits_deploy_earlier_runtime_change(repository):
    before = commit(repository, {"README.md": "baseline"})
    commit(repository, {"src/data/cross_market_intraday_ingest.py": "new runtime"})
    after = commit(repository, {"serverless/yahoo_indices/README.md": "docs only at tip"})
    assert (
        git(repository, "diff", "--name-only", "HEAD~1", "HEAD")
        == "serverless/yahoo_indices/README.md"
    )
    result = detector().detect_release_changes(before, after, repository=repository)
    assert result["cross_market"] is True
    assert result["training"] is False
    assert result["changed_count"] == 2


def test_first_push_releases_existing_index_infrastructure(repository):
    after = commit(repository, {"README.md": "first branch snapshot"})
    result = detector().detect_release_changes("0" * 40, after, repository=repository)
    assert result["first_push"] is True
    assert result["cross_market"] is True


@pytest.mark.parametrize(
    "path",
    [
        "src/data/reference/cross_market/industry_indices.json",
        "serverless/yahoo_indices/handler.py",
        "deploy/cross-market/requirements-fc.txt",
        "deploy/cross-market/deploy_collectors.py",
        "scripts/collect_cross_market_indices.py",
        "Dockerfile",
        ".github/workflows/ci.yml",
    ],
)
def test_runtime_build_inputs_trigger_only_cross_market(path):
    assert detector().classify_paths([path]) == {"cross_market": True, "training": False}


def test_documentation_and_business_changes_do_not_restart_collectors():
    paths = [
        "serverless/yahoo_indices/README.md",
        "deploy/cross-market/README.md",
        "docs/cross-market-index-operations.md",
        "src/web/trading_ui.py",
    ]
    assert detector().classify_paths(paths) == {"cross_market": False, "training": False}


def test_original_training_sources_keep_their_deployment():
    assert detector().classify_paths(["serverless/app.py"]) == {
        "cross_market": False,
        "training": True,
    }
    assert detector().classify_paths(["serverless/yahoo_indices/s.yaml"]) == {
        "cross_market": True,
        "training": False,
    }


def test_rename_preserves_removed_training_source_detection(repository):
    before = commit(repository, {"serverless/app.py": "training"})
    destination = repository / "serverless/yahoo_indices/helper.py"
    destination.parent.mkdir(parents=True)
    (repository / "serverless/app.py").rename(destination)
    after = commit(repository, {"README.md": "rename"})
    result = detector().detect_release_changes(before, after, repository=repository)
    assert result["training"] is True
    assert result["cross_market"] is True


def test_unknown_push_base_fails_instead_of_silently_skipping(repository):
    after = commit(repository, {"README.md": "baseline"})
    with pytest.raises(RuntimeError, match="complete push range"):
        detector().detect_release_changes("f" * 40, after, repository=repository)


def test_cli_emits_lowercase_github_flags(repository, tmp_path):
    before = commit(repository, {"README.md": "baseline"})
    after = commit(repository, {"serverless/yahoo_indices/requirements.txt": "httpx==0.28.1"})
    output = tmp_path / "github-output"
    assert (
        detector().main(
            [
                "--repository",
                str(repository),
                "--before",
                before,
                "--after",
                after,
                "--github-output",
                str(output),
            ]
        )
        == 0
    )
    assert output.read_text(encoding="utf-8").splitlines() == [
        "cross_market=true",
        "training=false",
    ]


def test_v15_mutable_tag_is_only_promoted_after_build_under_its_own_lock():
    jobs = workflow()["jobs"]
    promotion = jobs["promote-v15-image"]
    assert "build-and-push" in promotion["needs"]
    assert "refs/heads/main" not in promotion["if"]
    assert promotion["concurrency"]["cancel-in-progress"] == "false"
    assert promotion["concurrency"]["queue"] == "max"
    run = "\n".join(step.get("run", "") for step in promotion["steps"])
    assert "--publish-if-current" in run
    assert "imagetools create" in run
    assert run.index("--publish-if-current") < run.index("imagetools create")
    build = jobs["build-and-push"]
    assert "trading-service:test" not in str(build)
    tags = next(step["with"]["tags"] for step in build["steps"] if "tags" in step.get("with", {}))
    assert "trading-service:${{ github.sha }}" in tags
    assert "${{ steps.tag.outputs.mutable_tag }}" in tags


def test_v15_unified_release_checks_order_before_either_deployment():
    job = workflow()["jobs"]["deploy-cross-market"]
    assert any(step.get("with", {}).get("fetch-depth") == "0" for step in job["steps"])
    commands = "\n".join(step.get("run", "") for step in job["steps"])
    assert "deploy_release.py" in commands
    assert "deploy_fc.py" not in commands
    assert "deploy_collectors.py" not in commands


@pytest.mark.parametrize(
    "path",
    [
        "src/data/massive_indices.py",
        "src/data/cross_market_massive_ingest.py",
        "scripts/backfill_cross_market_massive.py",
    ],
)
def test_massive_runtime_sources_trigger_v15_release(path):
    assert detector().classify_paths([path]) == {"cross_market": True, "training": False}


def test_late_build_of_A_does_not_promote_test_after_B(repository):
    older = commit(repository, {"README.md": "A"})
    git(repository, "branch", "-M", "refactor/cleanup-v15-only")
    git(repository, "remote", "add", "origin", str(repository))
    newer = commit(repository, {"README.md": "B"})
    assert (
        detector().mutable_publication(repository, newer, "refactor/cleanup-v15-only")[
            "publish_v15"
        ]
        is True
    )
    assert (
        detector().mutable_publication(repository, older, "refactor/cleanup-v15-only")[
            "publish_v15"
        ]
        is False
    )


def test_failed_live_branch_read_cannot_be_treated_as_fresh(repository):
    revision = commit(repository, {"README.md": "A"})
    with pytest.raises(RuntimeError):
        detector().mutable_publication(repository, revision, "refactor/cleanup-v15-only")
