"""Build reproducible FC and collector releases from one committed revision.

Vendor directories must be installed for Linux Python 3.12 (worker) and 3.13
(collector) with pip --no-compile. This tool never reads local credentials.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import re
import stat
import subprocess
import tarfile
import tempfile
import zipfile
from pathlib import Path, PurePosixPath

SOURCE_PATHS = (
    "src/data/yahoo_indices.py",
    "src/data/yahoo_intraday_indices.py",
    "src/data/fc_yahoo_indices.py",
    "src/data/fc_intraday_indices.py",
    "src/data/cross_market_store.py",
    "src/data/cross_market_ingest.py",
    "src/data/cross_market_intraday_ingest.py",
    "src/data/massive_indices.py",
    "src/data/cross_market_massive_ingest.py",
    "scripts/collect_cross_market_indices.py",
    "scripts/collect_cross_market_intraday.py",
    "scripts/backfill_cross_market_massive.py",
    "src/data/reference/cross_market/industry_boards.json",
    "src/data/reference/cross_market/industry_indices.json",
    "src/data/reference/cross_market/intraday_indices.json",
    "src/data/reference/cross_market/massive_indices.json",
)
EMPTY_PACKAGES = {"src/__init__.py": b"", "src/data/__init__.py": b""}
WORKER_SOURCES = {
    "handler.py": "serverless/yahoo_indices/handler.py",
    **{
        path: path
        for path in (
            "src/data/yahoo_indices.py",
            "src/data/yahoo_intraday_indices.py",
            "src/data/fc_yahoo_worker.py",
        )
    },
}
COMPOSE_PATH = "deploy/cross-market/docker-compose.yml"
SHA256 = re.compile(r"[0-9a-f]{64}\Z")
REVISION = re.compile(r"[0-9a-f]{40}\Z")


class ReleaseError(ValueError):
    """The supplied release cannot establish safe, exact committed artifacts."""

    code = "ReleaseValidationFailed"


def digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def files_digest(files: dict[str, str]) -> str:
    return digest(json.dumps(files, sort_keys=True, separators=(",", ":")).encode("utf-8"))


def safe_name(name: str) -> None:
    if (
        not isinstance(name, str)
        or not name
        or "\\" in name
        or ":" in name
        or any(ord(c) < 32 for c in name)
        or name.startswith("/")
        or name != PurePosixPath(name).as_posix()
        or any(part in ("", ".", "..") for part in name.split("/"))
    ):
        raise ReleaseError("Unsafe release member path")
    path = PurePosixPath(name)
    lowered = [part.lower() for part in path.parts]
    filename = lowered[-1]
    if (
        path.suffix.lower() in (".dll", ".pyd", ".exe", ".pyc", ".pyo")
        or "__pycache__" in lowered
        or any(part in (".git", ".ssh", ".aws", ".aliyun", ".s") for part in lowered)
        or filename.startswith(".env")
        or filename.endswith(".env")
        or filename
        in (
            "credentials",
            "credentials.json",
            "credentials.ini",
            "credentials.yaml",
            "credentials.yml",
            ".netrc",
            "id_rsa",
            "id_ed25519",
            "id_ecdsa",
            "direct_url.json",
        )
        or path.suffix.lower() in (".key", ".p12", ".pfx")
    ):
        raise ReleaseError("Forbidden executable, bytecode or credential file")


def _validate_entry(name: str, raw: bytes) -> None:
    safe_name(name)
    # Public CA bundles such as certifi/cacert.pem are required dependencies.
    if re.match(
        rb"-----BEGIN (?:RSA |DSA |EC |OPENSSH |ENCRYPTED )?PRIVATE KEY-----",
        raw.lstrip(),
    ):
        raise ReleaseError("Private key material in release")
    if PurePosixPath(name).suffix == ".so" and not raw.startswith(b"\x7fELF"):
        raise ReleaseError("Non-Linux shared library in release")


def _git(source_root: Path, *args: str) -> bytes:
    result = subprocess.run(
        ["git", "-C", str(source_root), *args],
        capture_output=True,
        check=False,
    )
    if result.returncode:
        raise ReleaseError("Committed release source could not be read")
    return result.stdout


def _committed_files(source_root: Path, revision: str) -> dict[str, bytes]:
    if not REVISION.fullmatch(revision):
        raise ReleaseError("Revision must be a full lowercase commit SHA")
    if _git(source_root, "cat-file", "-t", revision).strip() != b"commit":
        raise ReleaseError("Revision does not identify a commit")
    paths = sorted(set(SOURCE_PATHS) | set(WORKER_SOURCES.values()) | {COMPOSE_PATH})
    entries = {}
    for item in _git(source_root, "ls-tree", "-rz", revision, "--", *paths).split(b"\0"):
        if not item:
            continue
        metadata, name = item.split(b"\t", 1)
        mode, kind, _ = metadata.split(b" ", 2)
        if kind != b"blob" or mode not in (b"100644", b"100755"):
            raise ReleaseError("Release source must be a regular committed file")
        entries[name.decode("utf-8")] = True
    if set(entries) != set(paths):
        raise ReleaseError("Revision is missing a required release source")
    return {path: _git(source_root, "show", f"{revision}:{path}") for path in paths}


def _vendor(directory: Path) -> dict[str, bytes]:
    if directory.is_symlink() or not directory.is_dir():
        raise ReleaseError("Vendor must be an existing regular directory")
    result = {}
    for path in sorted(directory.rglob("*")):
        if path.is_symlink():
            raise ReleaseError("Vendor contains a symbolic link")
        if path.is_dir():
            continue
        if not path.is_file():
            raise ReleaseError("Vendor contains a non-regular file")
        name = path.relative_to(directory).as_posix()
        raw = path.read_bytes()
        _validate_entry(name, raw)
        # pip entry-point scripts are not part of these import-only runtimes.
        if "bin" not in PurePosixPath(name).parts:
            result[name] = raw
    if not result:
        raise ReleaseError("Vendor directory contains no importable files")
    return result


def _zip(files: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for name, raw in sorted(files.items()):
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.create_system = 3
            info.external_attr = (stat.S_IFREG | 0o600) << 16
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, raw)
    return buffer.getvalue()


def _tar(files: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", mtime=0) as compressed:
        with tarfile.open(fileobj=compressed, mode="w") as archive:
            for name, raw in sorted(files.items()):
                info = tarfile.TarInfo(name)
                info.size, info.mode, info.mtime = len(raw), 0o600, 0
                archive.addfile(info, io.BytesIO(raw))
    return buffer.getvalue()


def _atomic_write(path: Path, raw: bytes) -> None:
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".release-", delete=False) as stream:
        temporary = Path(stream.name)
    try:
        with temporary.open("wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def build_release(
    *,
    source_root: Path,
    revision: str,
    output_dir: Path,
    worker_vendor: Path,
    collector_vendor: Path,
) -> dict:
    source_root, output_dir = Path(source_root), Path(output_dir)
    for vendor in (Path(worker_vendor), Path(collector_vendor)):
        if output_dir.resolve().is_relative_to(vendor.resolve()):
            raise ReleaseError("Output directory must be outside vendor inputs")
    committed = _committed_files(source_root, revision)
    worker = {path: committed[source] for path, source in WORKER_SOURCES.items()}
    worker.update(EMPTY_PACKAGES)
    dependencies = _vendor(Path(worker_vendor))
    if worker.keys() & dependencies.keys():
        raise ReleaseError("Worker vendor would overwrite committed source")
    worker.update(dependencies)
    runtime = {path: committed[path] for path in SOURCE_PATHS}
    runtime.update(EMPTY_PACKAGES)
    runtime.update({"vendor/" + path: raw for path, raw in _vendor(Path(collector_vendor)).items()})
    for name, raw in list(worker.items()) + list(runtime.items()):
        _validate_entry(name, raw)
    worker_zip, runtime_tar = _zip(worker), _tar(runtime)
    files = {name: digest(raw) for name, raw in sorted(runtime.items())}
    manifest = {
        "schema_version": 1,
        "revision": revision,
        "bundle_sha256": files_digest(files),
        "files": files,
        "runtime_archive_sha256": digest(runtime_tar),
        "worker_sha256": digest(worker_zip),
        "worker_files": {name: digest(raw) for name, raw in sorted(worker.items())},
        "compose_sha256": digest(committed[COMPOSE_PATH]),
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    for name, raw in {
        "worker.zip": worker_zip,
        "runtime.tar.gz": runtime_tar,
        "docker-compose.yml": committed[COMPOSE_PATH],
    }.items():
        _atomic_write(output_dir / name, raw)
    _atomic_write(output_dir / "manifest.json", json.dumps(manifest, indent=2).encode("utf-8"))
    return manifest


def _hash_map(value: object) -> dict[str, str]:
    if not isinstance(value, dict) or not value:
        raise ReleaseError("Release file hashes are absent")
    for name, sha in value.items():
        safe_name(name)
        if not isinstance(sha, str) or not SHA256.fullmatch(sha):
            raise ReleaseError("Invalid release file SHA256")
    return value


def verify_release(release_dir: Path) -> dict:
    """Verify all artifacts and exact member bytes before any remote side effect."""
    release_dir = Path(release_dir)
    try:
        manifest = json.loads((release_dir / "manifest.json").read_bytes())
    except (ValueError, OSError):
        raise ReleaseError("Release manifest could not be read") from None
    if (
        not isinstance(manifest, dict)
        or type(manifest.get("schema_version")) is not int
        or manifest["schema_version"] != 1
        or not isinstance(manifest.get("revision"), str)
        or not REVISION.fullmatch(manifest["revision"])
    ):
        raise ReleaseError("Invalid release schema or revision")
    files = _hash_map(manifest.get("files"))
    worker_files = _hash_map(manifest.get("worker_files"))
    required = set(SOURCE_PATHS) | set(EMPTY_PACKAGES)
    if not required <= files.keys() or any(
        name not in required and not name.startswith("vendor/") for name in files
    ):
        raise ReleaseError("Runtime source list does not match the collector release")
    if not (set(WORKER_SOURCES) | set(EMPTY_PACKAGES)) <= worker_files.keys():
        raise ReleaseError("Worker is missing committed source")
    if manifest.get("bundle_sha256") != files_digest(files):
        raise ReleaseError("Runtime bundle manifest SHA256 differs")
    for name, key in (
        ("worker.zip", "worker_sha256"),
        ("runtime.tar.gz", "runtime_archive_sha256"),
        ("docker-compose.yml", "compose_sha256"),
    ):
        sha = manifest.get(key)
        if not isinstance(sha, str) or not SHA256.fullmatch(sha):
            raise ReleaseError("Release artifact SHA256 is absent")
        if digest((release_dir / name).read_bytes()) != sha:
            raise ReleaseError("Release artifact SHA256 differs")
    observed = {}
    with zipfile.ZipFile(release_dir / "worker.zip") as archive:
        for member in archive.infolist():
            if (
                member.filename in observed
                or member.is_dir()
                or stat.S_ISLNK(member.external_attr >> 16)
            ):
                raise ReleaseError("Duplicate or non-regular worker archive member")
            raw = archive.read(member)
            _validate_entry(member.filename, raw)
            observed[member.filename] = digest(raw)
    if observed != worker_files:
        raise ReleaseError("Worker ZIP members differ from manifest")
    observed = {}
    with tarfile.open(release_dir / "runtime.tar.gz", "r:gz") as archive:
        for member in archive.getmembers():
            if member.name in observed or not member.isfile():
                raise ReleaseError("Duplicate or non-regular runtime archive member")
            raw = archive.extractfile(member).read()
            _validate_entry(member.name, raw)
            observed[member.name] = digest(raw)
    if observed != files:
        raise ReleaseError("Runtime archive members differ from manifest")
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("source-root", "revision", "output-dir", "worker-vendor", "collector-vendor"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args(argv)
    try:
        manifest = build_release(
            source_root=Path(args.source_root),
            revision=args.revision,
            output_dir=Path(args.output_dir),
            worker_vendor=Path(args.worker_vendor),
            collector_vendor=Path(args.collector_vendor),
        )
        verify_release(Path(args.output_dir))
    except Exception as exc:
        print(json.dumps({"error_type": type(exc).__name__, "code": getattr(exc, "code", None)}))
        return 1
    print(
        json.dumps(
            {
                "revision": manifest["revision"],
                "bundle_sha256": manifest["bundle_sha256"],
                "worker_sha256": manifest["worker_sha256"],
                "verified": True,
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
