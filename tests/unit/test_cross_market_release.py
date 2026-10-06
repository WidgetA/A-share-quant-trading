"""Real Git and archive checks for releases; no cloud or production credentials."""

import hashlib
import importlib.util
import io
import json
import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path

import pytest

DEPLOY_DIR = Path(__file__).resolve().parents[2] / "deploy/cross-market"
SPEC = importlib.util.spec_from_file_location("build_release", DEPLOY_DIR / "build_release.py")
RELEASE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = RELEASE
SPEC.loader.exec_module(RELEASE)


def git(root, *args, input=None):
    return subprocess.run(
        ["git", "-C", str(root), *args],
        input=input,
        capture_output=True,
        check=True,
    ).stdout


def make_release_inputs(tmp_path):
    root = tmp_path / "repo"
    root.mkdir()
    paths = set(RELEASE.SOURCE_PATHS) | set(RELEASE.WORKER_SOURCES.values())
    paths.add(RELEASE.COMPOSE_PATH)
    for name in paths:
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(("committed file " + name).encode())
    git(root, "init")
    git(root, "add", ".")
    git(
        root,
        "-c",
        "user.name=Release Test",
        "-c",
        "user.email=test@example.invalid",
        "commit",
        "-m",
        "release fixture",
    )
    revision = git(root, "rev-parse", "HEAD").decode().strip()
    worker, collector = tmp_path / "worker", tmp_path / "collector"
    for directory in (worker, collector):
        (directory / "httpx").mkdir(parents=True)
        (directory / "httpx/__init__.py").write_bytes(b"# Linux pure Python vendor\n")
        (directory / "certifi").mkdir()
        (directory / "certifi/cacert.pem").write_bytes(b"-----BEGIN CERTIFICATE-----\npublic CA")
    return {
        "source_root": root,
        "revision": revision,
        "output_dir": tmp_path / "release",
        "worker_vendor": worker,
        "collector_vendor": collector,
    }


@pytest.fixture
def inputs(tmp_path):
    return make_release_inputs(tmp_path)


def test_release_reads_commit_not_dirty_worktree_and_contains_both_collectors(inputs):
    dirty = inputs["source_root"] / "src/data/yahoo_indices.py"
    original = dirty.read_bytes()
    dirty.write_bytes(b"uncommitted source must never be deployed")
    (inputs["source_root"] / ".env.local").write_text("SECRET_DO_NOT_SHIP=private")
    manifest = RELEASE.build_release(**inputs)
    assert RELEASE.verify_release(inputs["output_dir"]) == manifest
    assert manifest["schema_version"] == 1
    assert manifest["revision"] == inputs["revision"]
    with tarfile.open(inputs["output_dir"] / "runtime.tar.gz") as archive:
        names = set(archive.getnames())
        assert set(RELEASE.SOURCE_PATHS) | set(RELEASE.EMPTY_PACKAGES) <= names
        assert len([name for name in names if not name.startswith("vendor/")]) == 18
        assert "scripts/collect_cross_market_intraday.py" in names
        assert "src/data/reference/cross_market/intraday_indices.json" in names
        assert RELEASE.COMPOSE_PATH not in names
        assert ".env.local" not in names
        assert archive.extractfile("src/data/yahoo_indices.py").read() == original
        assert archive.extractfile("src/__init__.py").read() == b""
    with zipfile.ZipFile(inputs["output_dir"] / "worker.zip") as archive:
        assert archive.read("src/data/yahoo_indices.py") == original
        assert archive.read("handler.py") == git(
            inputs["source_root"],
            "show",
            inputs["revision"] + ":serverless/yahoo_indices/handler.py",
        )
        assert "src/data/yahoo_intraday_indices.py" in archive.namelist()
        assert "src/data/fc_yahoo_worker.py" in archive.namelist()
    assert (inputs["output_dir"] / "docker-compose.yml").read_bytes() == git(
        inputs["source_root"],
        "show",
        inputs["revision"] + ":" + RELEASE.COMPOSE_PATH,
    )
    canonical = json.dumps(manifest["files"], sort_keys=True, separators=(",", ":")).encode()
    assert hashlib.sha256(canonical).hexdigest() == manifest["bundle_sha256"]


def test_massive_backfill_runtime_is_packaged_from_the_same_commit(inputs):
    required = {
        "src/data/massive_indices.py",
        "src/data/cross_market_massive_ingest.py",
        "scripts/backfill_cross_market_massive.py",
        "src/data/reference/cross_market/massive_indices.json",
    }
    RELEASE.build_release(**inputs)
    with tarfile.open(inputs["output_dir"] / "runtime.tar.gz") as archive:
        assert required <= set(archive.getnames())
        for name in required:
            assert archive.extractfile(name).read() == git(
                inputs["source_root"], "show", inputs["revision"] + ":" + name
            )


def test_archives_are_reproducible_independent_of_input_mtime(inputs, tmp_path):
    first = RELEASE.build_release(**inputs)
    first_bytes = {
        name: (inputs["output_dir"] / name).read_bytes()
        for name in (
            "worker.zip",
            "runtime.tar.gz",
            "manifest.json",
            "docker-compose.yml",
        )
    }
    import os

    os.utime(inputs["worker_vendor"] / "httpx/__init__.py", (1234567, 1234567))
    inputs["output_dir"] = tmp_path / "release-again"
    assert RELEASE.build_release(**inputs) == first
    assert first_bytes == {name: (inputs["output_dir"] / name).read_bytes() for name in first_bytes}


@pytest.mark.parametrize(
    "name,raw",
    [
        ("native/module.pyd", b"binary"),
        ("native/module.dll", b"binary"),
        ("Scripts/httpx.exe", b"binary"),
        ("httpx/__pycache__/module.pyc", b"bytecode"),
        (".env.local", b"ALIYUN_ACCESS_KEY_SECRET=private"),
        ("fc.credentials.env", b"private"),
        (".aws/credentials", b"private"),
        ("credentials.json", b"private"),
        ("pkg.dist-info/direct_url.json", b"private URL"),
        ("private.pem", b"-----BEGIN PRIVATE KEY-----\nprivate"),
        ("native/module.so", b"Windows PE image"),
    ],
)
def test_unsafe_vendor_fails_before_producing_release(inputs, name, raw):
    path = inputs["worker_vendor"] / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    with pytest.raises(RELEASE.ReleaseError):
        RELEASE.build_release(**inputs)
    assert not inputs["output_dir"].exists()


@pytest.mark.parametrize(
    "name",
    [
        "/etc/passwd",
        "../secret",
        "a/../../secret",
        "C:/secret",
        "a\\secret",
        "a//b",
        "a/./b",
        "a\x00b",
    ],
)
def test_archive_paths_cannot_escape_release(name):
    with pytest.raises(RELEASE.ReleaseError):
        RELEASE.safe_name(name)


def test_vendor_cannot_overwrite_worker_source(inputs):
    path = inputs["worker_vendor"] / "src/data/yahoo_indices.py"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"vendor impostor")
    with pytest.raises(RELEASE.ReleaseError, match="overwrite"):
        RELEASE.build_release(**inputs)


def test_sdk_private_key_header_constant_is_code_not_a_credential(inputs):
    path = inputs["collector_vendor"] / "alibabacloud_tea_openapi/utils.py"
    path.parent.mkdir(parents=True)
    path.write_bytes(b'PRIVATE_KEY_HEADER = "-----BEGIN PRIVATE KEY-----"\n')
    manifest = RELEASE.build_release(**inputs)
    assert "vendor/alibabacloud_tea_openapi/utils.py" in manifest["files"]


def test_git_symlink_is_rejected_without_reading_link_destination(inputs):
    root = inputs["source_root"]
    blob = git(root, "hash-object", "-w", "--stdin", input=b"../../secret").decode().strip()
    git(root, "update-index", "--cacheinfo", f"120000,{blob},src/data/yahoo_indices.py")
    git(
        root,
        "-c",
        "user.name=Release Test",
        "-c",
        "user.email=test@example.invalid",
        "commit",
        "-m",
        "unsafe tracked link",
    )
    inputs["revision"] = git(root, "rev-parse", "HEAD").decode().strip()
    with pytest.raises(RELEASE.ReleaseError, match="regular committed"):
        RELEASE.build_release(**inputs)


@pytest.mark.parametrize("revision", ["HEAD", "a" * 39, "a" * 40])
def test_missing_or_nonexact_revision_is_rejected(inputs, revision):
    inputs["revision"] = revision
    with pytest.raises(RELEASE.ReleaseError):
        RELEASE.build_release(**inputs)


def test_corrupt_zip_or_wrong_manifest_cannot_verify(inputs):
    manifest = RELEASE.build_release(**inputs)
    archive = inputs["output_dir"] / "worker.zip"
    original = archive.read_bytes()
    archive.write_bytes(original + b"tampering")
    with pytest.raises(RELEASE.ReleaseError, match="artifact SHA256"):
        RELEASE.verify_release(inputs["output_dir"])
    archive.write_bytes(original)
    manifest["files"]["src/data/yahoo_indices.py"] = "0" * 64
    (inputs["output_dir"] / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(RELEASE.ReleaseError, match="manifest SHA256"):
        RELEASE.verify_release(inputs["output_dir"])


def test_tar_link_cannot_pass_even_with_matching_artifact_hash(inputs):
    manifest = RELEASE.build_release(**inputs)
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        info = tarfile.TarInfo("vendor/link")
        info.type, info.linkname = tarfile.SYMTYPE, "/etc/passwd"
        archive.addfile(info)
    raw = buffer.getvalue()
    (inputs["output_dir"] / "runtime.tar.gz").write_bytes(raw)
    manifest["runtime_archive_sha256"] = RELEASE.digest(raw)
    (inputs["output_dir"] / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(RELEASE.ReleaseError, match="non-regular runtime"):
        RELEASE.verify_release(inputs["output_dir"])


def test_source_list_missing_at_revision_fails(inputs):
    root = inputs["source_root"]
    git(root, "rm", "scripts/collect_cross_market_intraday.py")
    git(
        root,
        "-c",
        "user.name=Release Test",
        "-c",
        "user.email=test@example.invalid",
        "commit",
        "-m",
        "missing entry",
    )
    inputs["revision"] = git(root, "rev-parse", "HEAD").decode().strip()
    with pytest.raises(RELEASE.ReleaseError, match="missing"):
        RELEASE.build_release(**inputs)
