"""Deploy a verified release to the two FC collectors, preserving durable state.

The default verification waits for new complete production cycles and performs
read-only Greptime checks. A verification timeout exits unsuccessfully while the
new collectors continue running; a failed cutover restores the old runtime.
"""

from __future__ import annotations

import argparse
import hashlib
import inspect
import io
import json
import os
import re
import shlex
import tarfile
import tempfile
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit

SERVICES = ("cross-market-collector", "cross-market-intraday-collector")
NAMES = tuple("ashare-cross-market_" + service + "_1" for service in SERVICES)
CONFIG_FILES = (
    "runtime",
    "docker-compose.yml",
    ".env",
    "fc.credentials.env",
    "runtime-manifest.json",
)
DEFAULTS = {
    "CROSS_MARKET_CONCURRENCY": "2",
    "CROSS_MARKET_LOOP_SECONDS": "300",
    "CROSS_MARKET_BATCH_SIZE": "100",
    "CROSS_MARKET_INTRADAY_CONCURRENCY": "2",
    "CROSS_MARKET_INTRADAY_LOOP_SECONDS": "300",
    "CROSS_MARKET_INTRADAY_BATCH_SIZE": "1000",
}


class DeploymentError(RuntimeError):
    """A release/cutover was not established; messages never include secrets."""


class VerificationTimeout(DeploymentError):
    """Collectors remain active, but a complete new cycle was not verified."""


class RemoteJSONError(DeploymentError):
    """An SSH state response failed parsing, without exposing its contents."""

    code = "RemoteJSONInvalid"
    phase = "collector_state_snapshot"

    def __init__(self, stdout_bytes: int, json_error_position: int):
        super().__init__("State snapshot response was not valid JSON")
        self.stdout_bytes = stdout_bytes
        self.json_error_position = json_error_position


def digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def safe_member(name: str) -> str:
    path = PurePosixPath(name)
    if (
        not isinstance(name, str)
        or not name
        or "\\" in name
        or "\x00" in name
        or path.is_absolute()
        or any(p in ("", ".", "..") for p in name.split("/"))
    ):
        raise ValueError("Unsafe release archive member")
    return name


def validate_archive(archive: Path, files: dict[str, str]) -> None:
    seen = set()
    with tarfile.open(archive, "r:gz") as stream:
        for member in stream.getmembers():
            safe_member(member.name)
            if not member.isfile() or member.name in seen or member.name not in files:
                raise ValueError("Unexpected/duplicate/non-file runtime member")
            seen.add(member.name)
            with stream.extractfile(member) as source:
                if digest(source.read()) != files[member.name]:
                    raise ValueError("Runtime member SHA256 differs")
    if seen != set(files):
        raise ValueError("Runtime archive omits manifest files")


def validate_release(directory: Path, runtime_image: str) -> dict:
    manifest = json.loads((directory / "manifest.json").read_bytes())
    fc = json.loads((directory / "fc-deployment.json").read_bytes())
    revision = manifest.get("revision")
    if (
        manifest.get("schema_version") != 1
        or not isinstance(revision, str)
        or not re.fullmatch(r"[0-9a-f]{40}", revision)
    ):
        raise ValueError("Invalid release revision/schema")
    if (
        not re.fullmatch(r"[a-z0-9][a-z0-9._:/-]*:[0-9a-f]{40}", runtime_image)
        or runtime_image.rsplit(":", 1)[-1] != revision
    ):
        raise ValueError("Runtime image must have the same full commit tag")
    files = manifest.get("files")
    if not isinstance(files, dict) or not files:
        raise ValueError("Missing runtime file manifest")
    for name, checksum in files.items():
        safe_member(name)
        if not isinstance(checksum, str) or not re.fullmatch(r"[0-9a-f]{64}", checksum):
            raise ValueError("Invalid runtime file SHA256")
    canonical = json.dumps(files, sort_keys=True, separators=(",", ":")).encode()
    if digest(canonical) != manifest.get("bundle_sha256"):
        raise ValueError("Canonical runtime manifest SHA256 differs")
    for filename, field in (
        ("runtime.tar.gz", "runtime_archive_sha256"),
        ("worker.zip", "worker_sha256"),
        ("docker-compose.yml", "compose_sha256"),
    ):
        if digest((directory / filename).read_bytes()) != manifest.get(field):
            raise ValueError("Release artifact SHA256 differs")
    validate_archive(directory / "runtime.tar.gz", files)
    if (
        fc.get("revision") != revision
        or fc.get("function_name") != "ashare_yahoo_indices_v15"
        or fc.get("region") != "us-west-1"
        or fc.get("verified") is not True
        or fc.get("uploaded_zip_exact_cloud_readback") is not True
        or fc.get("code_zip_sha256") != manifest["worker_sha256"]
        or fc.get("cloud_zip_sha256") != manifest["worker_sha256"]
        or not isinstance(fc.get("native_smoke"), list)
        or not fc["native_smoke"]
    ):
        raise ValueError("FC receipt is not a verified deployment of this release")
    endpoint = fc.get("endpoint")
    parsed = urlsplit(
        endpoint if isinstance(endpoint, str) and "://" in endpoint else "https://" + str(endpoint)
    )
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.path not in ("", "/")
        or parsed.query
        or parsed.fragment
        or any(c.isspace() for c in str(endpoint))
    ):
        raise ValueError("Invalid authenticated FC origin")
    with tarfile.open(directory / "runtime.tar.gz", "r:gz") as archive:

        def document(path):
            with archive.extractfile(path) as content:
                return json.load(content)

        reference = document("src/data/reference/cross_market/industry_indices.json")
        capability = document("src/data/reference/cross_market/intraday_indices.json")
    daily = {(i["market"], i["symbol"]) for i in reference["indices"]}
    minute = {
        (i["market"], i["symbol"], grain) for i in capability["indices"] for grain in i["intervals"]
    }
    if (
        len(daily) != len(reference["indices"])
        or not daily
        or not minute
        or len(minute) != sum(len(i["intervals"]) for i in capability["indices"])
        or any((market, symbol) not in daily for market, symbol, _ in minute)
        or capability.get("base_reference_sha256")
        != files["src/data/reference/cross_market/industry_indices.json"]
    ):
        raise ValueError("Invalid or unbound collector catalogue")
    return {
        "manifest": manifest,
        "endpoint": parsed.netloc,
        "daily": daily,
        "minute": minute,
        "mapping_count": sum(len(i["markets"]) for i in reference["industries"]),
        "history_seed": {
            (market, symbol, grain)
            for market, symbol, grain in minute
            if capability.get("limits", {}).get(grain, {}).get("update_mode", "continuous")
            == "history_seed"
        },
    }


def credentials(environ: dict) -> bytes:
    values = []
    for suffix in ("ID", "SECRET"):
        one = environ.get("ALIYUN_ACCESS_KEY_" + suffix)
        two = environ.get("ALIBABA_CLOUD_ACCESS_KEY_" + suffix)
        if one and two and one != two:
            raise ValueError("Conflicting FC access credential aliases")
        value = one or two
        if not value or any(c in value for c in "\r\n\x00"):
            raise ValueError("Missing or invalid FC access credentials")
        values.append("ALIYUN_ACCESS_KEY_" + suffix + "=" + value)
    token = environ.get("ALIYUN_SECURITY_TOKEN") or environ.get("ALIBABA_CLOUD_SECURITY_TOKEN")
    if token:
        if any(c in token for c in "\r\n\x00"):
            raise ValueError("Invalid FC security token")
        values.append("ALIYUN_SECURITY_TOKEN=" + token)
    return ("\n".join(values) + "\n").encode()


class SSHRemote:
    def __init__(self, environ: dict):
        import paramiko

        required = [
            environ.get("CROSS_MARKET_SSH_" + key)
            for key in ("HOST", "USER", "PASSWORD", "KNOWN_HOSTS")
        ]
        if not all(required):
            raise ValueError("SSH host/user/password/known_hosts are required")
        host, user, password, known = required
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.RejectPolicy())
        if "\n" not in known and Path(known).is_file():
            self.client.load_host_keys(known)
        else:
            with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", delete=False) as handle:
                path = handle.name
                handle.write(known + "\n")
            try:
                self.client.load_host_keys(path)
            finally:
                os.unlink(path)
        self.client.connect(
            host,
            username=user,
            password=password,
            timeout=30,
            allow_agent=False,
            look_for_keys=False,
        )

    def run(self, argv: list[str]) -> bytes:
        channel = self.client.get_transport().open_session(timeout=30)
        channel.exec_command(shlex.join(argv))
        stdout, stderr = bytearray(), bytearray()
        deadline = time.monotonic() + 900
        try:
            while True:
                while channel.recv_ready():
                    stdout.extend(channel.recv(65536))
                while channel.recv_stderr_ready():
                    stderr.extend(channel.recv_stderr(65536))
                # Exit status can arrive before final stream data. Drain until
                # the channel has closed, rather than closing it on status alone.
                if (
                    channel.closed
                    and channel.exit_status_ready()
                    and not channel.recv_ready()
                    and not channel.recv_stderr_ready()
                ):
                    break
                if time.monotonic() >= deadline:
                    raise DeploymentError("Remote operation timed out")
                time.sleep(0.02)
            if channel.recv_exit_status() != 0:
                # Arbitrary SSH/docker stderr may include env values; never emit it.
                raise DeploymentError("Remote operation failed")
            return bytes(stdout)
        finally:
            channel.close()

    def upload(self, raw: bytes, path: str) -> None:
        sftp = self.client.open_sftp()
        try:
            sftp.putfo(io.BytesIO(raw), path)
            sftp.chmod(path, 0o600)
        finally:
            sftp.close()

    def close(self):
        self.client.close()


# These small stdlib functions also run on the host; tests exercise them on real
# temporary files. No state directory is renamed, copied back or recursively removed.
def _extract(stage: str):
    directory = Path(stage)
    manifest = json.loads((directory / "runtime-manifest.json").read_bytes())
    if digest((directory / "runtime.tar.gz").read_bytes()) != manifest["runtime_archive_sha256"]:
        raise ValueError("Uploaded runtime archive SHA256 differs")
    validate_archive(directory / "runtime.tar.gz", manifest["files"])
    runtime = directory / "runtime"
    runtime.mkdir(mode=0o700)
    with tarfile.open(directory / "runtime.tar.gz", "r:gz") as archive:
        for member in archive.getmembers():
            target = runtime / member.name
            target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            with archive.extractfile(member) as source, target.open("wb") as output:
                output.write(source.read())
            target.chmod(0o600)
    print(json.dumps({"files_verified": len(manifest["files"])}))


def _switch(root: str, stage: str):
    base, release = Path(root), Path(stage)
    backup = release / "previous"
    backup.mkdir(mode=0o700)
    journal = {name: (base / name).exists() for name in CONFIG_FILES}
    (release / "previous-exists.json").write_text(json.dumps(journal))
    for name in CONFIG_FILES:
        if (base / name).exists():
            os.replace(base / name, backup / name)
        os.replace(release / name, base / name)


def _rollback(root: str, stage: str):
    base, release = Path(root), Path(stage)
    if not (release / "previous-exists.json").exists():
        return
    journal = json.loads((release / "previous-exists.json").read_text())
    failed = release / "failed"
    failed.mkdir(exist_ok=True, mode=0o700)
    for name in CONFIG_FILES:
        old, current = release / "previous" / name, base / name
        if old.exists():
            if current.exists():
                os.replace(current, failed / name)
            os.replace(old, current)
        elif not journal[name] and current.exists():
            os.replace(current, failed / name)


def _snapshot(root: str):
    base = Path(root)
    result = {"files": {}, "states": {}, "pending": {}}
    for dirname in ("state-fc", "state-intraday-fc"):
        directory = base / dirname
        directory.mkdir(exist_ok=True, mode=0o700)
        for path in directory.glob("*.json"):
            relative = dirname + "/" + path.name
            raw = path.read_bytes()
            result["files"][relative] = digest(raw)
            value = json.loads(raw)
            if path.name.endswith(".state.json"):
                result["states"][relative] = value
            elif path.name.endswith(".pending.json"):
                result["pending"][relative] = value
    print(json.dumps(result))


def remote_script(function, *args) -> list[str]:
    functions = (digest, safe_member, validate_archive, function)
    prelude = (
        "from __future__ import annotations\n"
        "import pathlib,json,hashlib,tarfile,os\nfrom pathlib import Path,PurePosixPath\n"
    )
    prelude += "CONFIG_FILES=" + repr(CONFIG_FILES) + "\n"
    code = prelude + "\n".join(inspect.getsource(fn) for fn in functions)
    code += "\n" + function.__name__ + "(*json.loads(" + repr(json.dumps(args)) + "))\n"
    return ["python3", "-c", code]


def cycle_from_logs(
    raw: bytes,
    expected: set,
    reference_sha: str,
    capability_sha: str | None = None,
    history_seed: set | None = None,
):
    history_seed = history_seed or set()
    for line in reversed(raw.decode("utf-8", errors="replace").splitlines()):
        try:
            cycle = json.loads(line)
        except ValueError:
            continue
        if not isinstance(cycle, dict) or cycle.get("status") not in (
            "verified",
            "verified_with_gaps",
        ):
            continue
        if cycle.get("reference_sha256") != reference_sha:
            continue
        results = cycle.get("results" if capability_sha else "indices")
        if not isinstance(results, list):
            continue
        try:
            found = {
                (r["market"], r["symbol"], r["interval"])
                if capability_sha
                else (r["market"], r["symbol"])
                for r in results
            }
            if found != expected or len(results) != len(expected):
                continue
            if any(
                r["status"] not in ("verified", "verified_with_gaps", "previously_verified")
                for r in results
            ):
                continue
            if capability_sha:
                if cycle.get("capability_sha256") != capability_sha:
                    continue
                invalid = False
                for result in results:
                    identity = (result["market"], result["symbol"], result["interval"])
                    if identity in history_seed and result["status"] == "previously_verified":
                        covered = result.get("covered_until_s")
                        initial = result.get("backfill_complete_until_s")
                        if (
                            result.get("collection_mode") != "history_seed"
                            or type(covered) is not int
                            or type(initial) is not int
                            or not 0 < covered <= initial
                            or result["rows"] != 0
                            or result["windows"] != []
                        ):
                            invalid = True
                    elif (
                        result["status"] == "previously_verified"
                        or result["covered_until_s"] < cycle["run_end_s"]
                        or result["backfill_complete_until_s"] < cycle["run_end_s"]
                    ):
                        invalid = True
                if invalid:
                    continue
                if any(
                    w["rows"] != w["verified_rows"]
                    or not re.fullmatch(r"[0-9a-f]{64}", w["source_sha256"])
                    for r in results
                    for w in r["windows"]
                ):
                    continue
                if any(
                    not r["windows"]
                    and not r.get("replayed_rows")
                    and r["status"] != "previously_verified"
                    for r in results
                ):
                    continue
            elif any(r["status"] == "previously_verified" for r in results) or cycle.get(
                "mapping", {}
            ).get("status") not in ("verified", "previously_verified"):
                continue
            return cycle
        except (KeyError, TypeError):
            continue
    return None


def state_preserved(before: dict, after: dict) -> bool:
    for path, previous in before["states"].items():
        current = after["states"].get(path)
        if current is None or current.get("identity") != previous.get("identity"):
            return False
        for field in ("backfill_complete_until_s", "covered_until_s", "last_verified_ms"):
            marker = previous.get(field)
            if marker is not None and (current.get(field) is None or current[field] < marker):
                return False
    for path, pending in before["pending"].items():
        if path in after["pending"] and before["files"][path] == after["files"][path]:
            continue
        state = after["states"].get(path.replace(".pending.json", ".state.json"), {})
        if "end_s" in pending:
            if state.get("covered_until_s", 0) < pending["end_s"]:
                return False
        elif pending.get("source", {}).get("points"):
            if state.get("last_verified_ms", 0) < max(p["ts"] for p in pending["source"]["points"]):
                return False
        elif state.get("reference_sha256") != pending.get("reference_sha256"):
            return False
    return True


READBACK = """import asyncio,json,pathlib
from src.data.cross_market_ingest import load_reference
from src.data.cross_market_store import CrossMarketStore,REFERENCE_KEYS,_normalise_reference
async def check():
 ref=load_reference(pathlib.Path('/collector/src/data/reference/cross_market/industry_indices.json'),pathlib.Path('/collector/src/data/reference/cross_market/industry_boards.json'))
 states=[json.loads(p.read_text()) for p in pathlib.Path('/state').glob('*.state.json')]
 minute=any('interval' in s.get('identity',{}) for s in states)
 cap=json.loads(pathlib.Path('/collector/src/data/reference/cross_market/intraday_indices.json').read_text())
 expected=({(i['market'],i['symbol'],grain)
            for i in cap['indices'] for grain in i['intervals']} if minute
           else {(i['market'],i['symbol']) for i in ref['indices']})
 keys=[]
 for s in states:
  i=s.get('identity',{})
  if not i.get('symbol'):continue
  identity=(i['market'],i['symbol'],i['interval']) if minute else (i['market'],i['symbol'])
  if identity not in expected:continue
  kind=(('hour_bar' if i['interval']=='1h' else 'minute_bar') if minute
        else ('daily_bar' if s['capability']=='daily_history' else 'quote_snapshot'))
  grain=i['interval'] if minute else ('1d' if s['capability']=='daily_history' else 'quote')
  stamp=s['last_source_bar_ms'] if minute else s['last_verified_ms']
  keys.append(dict(provider='yahoo',market=i['market'],symbol=i['symbol'],
                   interval=grain,data_kind=kind,ts=stamp))
 assert len(keys)==len(expected)
 async with CrossMarketStore('http://greptimedb:4000',timeout=120) as db:
  # Cached mapping content retains its original observation clock. A new
  # deployment check must not require it to equal load_reference's new clock.
  stored=await db.read_industry_indices(ref['mappings'])
  saved={tuple(r[k] for k in REFERENCE_KEYS):r['fetched_at'] for r in stored}
  expected_mappings=[]
  for r in ref['mappings']:
   stamp=saved.get(tuple(r[k] for k in REFERENCE_KEYS))
   assert type(stamp) is int
   expected_mappings.append(_normalise_reference(dict(r,fetched_at=stamp)))
  mappings=db._verify(expected_mappings,stored,REFERENCE_KEYS)
  rows=await db.read_prices(keys)
  identity=lambda r:tuple(r[k] for k in ('provider','market','symbol','interval','data_kind','ts'))
  assert len(rows)==len(keys) and {identity(r) for r in rows}=={identity(r) for r in keys}
  assert all(len(r)==18 for r in rows)
  print(json.dumps(dict(mapping_rows_verified=mappings,price_keys_verified=len(rows),price_fields_read=18)))
asyncio.run(check())
"""


class CollectorDeployer:
    def __init__(self, remote, release_dir: Path, runtime_image: str, remote_root: str):
        if (
            not re.fullmatch(r"/[a-zA-Z0-9._/-]+", remote_root)
            or remote_root == "/"
            or ".." in PurePosixPath(remote_root).parts
        ):
            raise ValueError("Invalid absolute deployment root")
        self.remote, self.directory, self.root = remote, release_dir, remote_root.rstrip("/")
        self.release = validate_release(release_dir, runtime_image)
        self.image = runtime_image

    def compose(self):
        return [
            "docker-compose",
            "-p",
            "ashare-cross-market",
            "--env-file",
            self.root + "/.env",
            "-f",
            self.root + "/docker-compose.yml",
        ]

    def snapshot(self):
        raw = self.remote.run(remote_script(_snapshot, self.root))
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RemoteJSONError(len(raw), exc.pos) from None
        except UnicodeDecodeError as exc:
            raise RemoteJSONError(len(raw), exc.start) from None

    def deploy(self, private: bytes, verify_timeout: float = 1800, poll_seconds: float = 5):
        manifest = self.release["manifest"]
        revision = manifest["revision"]
        stage = self.root + "/.releases/" + revision + "-" + uuid.uuid4().hex
        self.remote.run(["mkdir", "-p", "-m", "700", stage])
        self.remote.run(["docker", "pull", self.image])
        immutable = json.loads(self.remote.run(["docker", "image", "inspect", self.image]))[0]["Id"]
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", immutable):
            raise DeploymentError("Pulled image has no immutable image ID")
        config = json.loads(
            self.remote.run(
                [
                    "python3",
                    "-c",
                    "import pathlib,json; p=pathlib.Path(__import__('sys').argv[1]);"
                    "print(json.dumps(p.read_text() if p.exists() else ''))",
                    self.root + "/.env",
                ]
            )
        )
        values = {}
        for line in config.splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                key, value = line.split("=", 1)
                if re.fullmatch(r"[A-Z_][A-Z_0-9]*", key):
                    values[key] = value
        for key, default in DEFAULTS.items():
            values.setdefault(key, default)
        values.update(
            CROSS_MARKET_RUNTIME_IMAGE=immutable,
            CROSS_MARKET_SOURCE_REVISION=revision,
            CROSS_MARKET_BUNDLE_SHA256=manifest["bundle_sha256"],
            CROSS_MARKET_FC_ENDPOINT=self.release["endpoint"],
        )
        env = (
            "\n".join(key + "=" + value for key, value in sorted(values.items())) + "\n"
        ).encode()
        compose = (
            (self.directory / "docker-compose.yml")
            .read_bytes()
            .replace(b"/opt/ashare-cross-market", self.root.encode())
        )
        for name, content in {
            "runtime.tar.gz": (self.directory / "runtime.tar.gz").read_bytes(),
            "runtime-manifest.json": json.dumps(manifest).encode(),
            "docker-compose.yml": compose,
            ".env": env,
            "fc.credentials.env": private,
        }.items():
            self.remote.upload(content, stage + "/" + name)
        uploads = {
            "runtime.tar.gz": (self.directory / "runtime.tar.gz").read_bytes(),
            "runtime-manifest.json": json.dumps(manifest).encode(),
            "docker-compose.yml": compose,
            ".env": env,
            "fc.credentials.env": private,
        }
        checksums = {name: digest(raw) for name, raw in uploads.items()}
        self.remote.run(
            [
                "python3",
                "-c",
                "import pathlib,json,hashlib,sys; root=pathlib.Path(sys.argv[1]);"
                " expected=json.loads(sys.argv[2]);"
                "assert all(hashlib.sha256((root/name).read_bytes()).hexdigest()==sha"
                " for name,sha in expected.items())",
                stage,
                json.dumps(checksums),
            ]
        )
        extraction = json.loads(self.remote.run(remote_script(_extract, stage)))
        if extraction["files_verified"] != len(manifest["files"]):
            raise DeploymentError("Staged runtime verification incomplete")
        # All mounts in a stage configuration must point at the staged runtime
        # and private credentials for validation; active state paths stay intact.
        stage_compose = compose.replace(
            (self.root + "/runtime").encode(), (stage + "/runtime").encode()
        )
        stage_compose = stage_compose.replace(
            (self.root + "/fc.credentials.env").encode(), (stage + "/fc.credentials.env").encode()
        )
        self.remote.upload(stage_compose, stage + "/validate-compose.yml")
        self.remote.run(
            [
                "docker-compose",
                "-p",
                "ashare-cross-market",
                "--env-file",
                stage + "/.env",
                "-f",
                stage + "/validate-compose.yml",
                "config",
                "--quiet",
            ]
        )
        self.remote.run(
            [
                "docker",
                "run",
                "--rm",
                "--network",
                "none",
                "--read-only",
                "--entrypoint",
                "python",
                "-e",
                "PYTHONPATH=/collector/vendor:/collector",
                "-e",
                "PYTHONDONTWRITEBYTECODE=1",
                "-v",
                stage + "/runtime:/collector:ro",
                immutable,
                "-c",
                "import alibabacloud_fc20230330.client,requests,src.data.fc_intraday_indices",
            ]
        )
        database_before = json.loads(self.remote.run(["docker", "inspect", "root_greptimedb_1"]))[0]
        available = set(
            self.remote.run(["docker", "ps", "-a", "--format", "{{.Names}}"]).decode().splitlines()
        )
        proxy = "ashare-cross-market_yahoo-index-proxy_1"
        if proxy in available:
            inspected = json.loads(self.remote.run(["docker", "inspect", proxy]))[0]
            if inspected["State"]["Running"]:
                self.remote.run(["docker", "stop", proxy])
        switched, stopped, frozen = False, False, None
        try:
            stopped = True
            self.remote.run(self.compose() + ["stop"] + list(SERVICES))
            frozen = self.snapshot()
            switched = True  # A partially failed _switch must also restore its journal.
            self.remote.run(remote_script(_switch, self.root, stage))
            if self.snapshot()["files"] != frozen["files"]:
                raise DeploymentError("Cutover changed durable state/pending files")
            self.remote.run(self.compose() + ["rm", "-f"] + list(SERVICES))
            self.remote.run(self.compose() + ["up", "-d", "--no-deps"] + list(SERVICES))
            actuals = json.loads(self.remote.run(["docker", "inspect"] + list(NAMES)))
            if len(actuals) != len(NAMES) or {a["Name"].lstrip("/") for a in actuals} != set(NAMES):
                raise DeploymentError("New collector container identities differ")
            for actual in actuals:
                labels = actual["Config"]["Labels"]
                environment = dict(v.split("=", 1) for v in actual["Config"]["Env"])
                mounts = {m["Destination"]: m for m in actual["Mounts"]}
                state_dir = (
                    "state-fc" if actual["Name"].lstrip("/") == NAMES[0] else "state-intraday-fc"
                )
                if (
                    not actual["State"]["Running"]
                    or actual["Image"] != immutable
                    or labels.get("org.ashare.cross-market.source-revision") != revision
                    or labels.get("org.ashare.cross-market.bundle-sha256")
                    != manifest["bundle_sha256"]
                    or any(
                        environment.get(k)
                        for k in (
                            "HTTP_PROXY",
                            "HTTPS_PROXY",
                            "ALL_PROXY",
                            "http_proxy",
                            "https_proxy",
                            "all_proxy",
                            "DEBUG",
                        )
                    )
                    or not actual["HostConfig"]["ReadonlyRootfs"]
                    or actual["HostConfig"]["RestartPolicy"]["Name"] != "unless-stopped"
                    or actual["HostConfig"].get("PortBindings")
                    or mounts.get("/collector", {}).get("Source") != self.root + "/runtime"
                    or mounts.get("/collector", {}).get("RW") is not False
                    or mounts.get("/state", {}).get("Source") != self.root + "/" + state_dir
                    or mounts.get("/state", {}).get("RW") is not True
                ):
                    raise DeploymentError("New collector image/labels/no-proxy verification failed")
                self.remote.run(
                    [
                        "docker",
                        "exec",
                        actual["Name"].lstrip("/"),
                        "python",
                        "-c",
                        "import alibabacloud_fc20230330.client,requests,"
                        "src.data.fc_intraday_indices",
                    ]
                )
            database_after = json.loads(
                self.remote.run(["docker", "inspect", "root_greptimedb_1"])
            )[0]
            if (
                any(database_before[k] != database_after[k] for k in ("Image", "RestartCount"))
                or database_before["State"]["StartedAt"] != database_after["State"]["StartedAt"]
            ):
                raise DeploymentError(
                    "Existing Greptime container changed during collector cutover"
                )
        except Exception:
            if switched:
                self.remote.run(self.compose() + ["stop"] + list(SERVICES))
                self.remote.run(remote_script(_rollback, self.root, stage))
                self.remote.run(self.compose() + ["rm", "-f"] + list(SERVICES))
                self.remote.run(self.compose() + ["up", "-d", "--no-deps"] + list(SERVICES))
            elif stopped:
                self.remote.run(self.compose() + ["up", "-d", "--no-deps"] + list(SERVICES))
            raise
        receipt = {
            "revision": revision,
            "bundle_sha256": manifest["bundle_sha256"],
            "immutable_image_id": immutable,
            "runtime_files_verified": extraction["files_verified"],
            "backup_directory": stage + "/previous",
            "status": "running_verification_incomplete",
            "history_seed_series": len(self.release["history_seed"]),
            "continuous_series": len(self.release["minute"]) - len(self.release["history_seed"]),
        }
        deadline = time.monotonic() + verify_timeout
        cycles = None
        try:
            while time.monotonic() < deadline:
                found = []
                for name, expected, cap in zip(
                    NAMES,
                    (self.release["daily"], self.release["minute"]),
                    (
                        None,
                        manifest["files"]["src/data/reference/cross_market/intraday_indices.json"],
                    ),
                ):
                    raw = self.remote.run(["docker", "logs", name])
                    found.append(
                        cycle_from_logs(
                            raw,
                            expected,
                            manifest["files"][
                                "src/data/reference/cross_market/industry_indices.json"
                            ],
                            cap,
                            self.release["history_seed"] if cap else None,
                        )
                    )
                if all(found):
                    cycles = found
                    break
                time.sleep(poll_seconds)
            if cycles is None:
                raise VerificationTimeout(
                    "New complete collection cycles were not verified before timeout"
                )
            after = self.snapshot()
            if not state_preserved(frozen, after):
                raise DeploymentError("Durable state/pending progression was not established")
            seed_targets = []
            for identity in sorted(self.release["history_seed"]):
                selected = [
                    state
                    for state in after["states"].values()
                    if (
                        state.get("identity", {}).get("market"),
                        state.get("identity", {}).get("symbol"),
                        state.get("identity", {}).get("interval"),
                    )
                    == identity
                ]
                if len(selected) != 1:
                    raise DeploymentError("History seed state identity was not established")
                state = selected[0]
                initial, covered = (
                    state.get("backfill_complete_until_s"),
                    state.get("covered_until_s"),
                )
                window_end = state.get("last_verified_window", {}).get("end_s")
                cycle_seed = next(
                    row
                    for row in cycles[1]["results"]
                    if (row["market"], row["symbol"], row["interval"]) == identity
                )
                if (
                    type(initial) is not int
                    or type(covered) is not int
                    or type(window_end) is not int
                    or not 0 < max(covered, window_end) <= initial
                    or initial < cycle_seed["backfill_complete_until_s"]
                    or covered < cycle_seed["covered_until_s"]
                ):
                    raise DeploymentError("History seed lacks a complete original source window")
                seed_targets.append(
                    {
                        "market": identity[0],
                        "symbol": identity[1],
                        "interval": identity[2],
                        "covered_until_s": covered,
                        "backfill_complete_until_s": initial,
                        "last_verified_window_end_s": window_end,
                        "gap_timestamps": state.get("gaps", []),
                    }
                )
            readbacks = [
                json.loads(self.remote.run(["docker", "exec", name, "python", "-c", READBACK]))
                for name in NAMES
            ]
            if [r["price_keys_verified"] for r in readbacks] != [
                len(self.release["daily"]),
                len(self.release["minute"]),
            ] or any(
                r["price_fields_read"] != 18
                or r["mapping_rows_verified"] != self.release["mapping_count"]
                for r in readbacks
            ):
                raise DeploymentError(
                    "Actual Greptime readback did not match the complete catalogue"
                )
            receipt.update(
                status="verified",
                completed_cycles=cycles,
                readbacks=readbacks,
                prior_state_and_pending_preserved=True,
                proxy_not_used=True,
                checked_at=datetime.now(UTC).isoformat(),
                history_seed_targets=seed_targets,
            )
            return receipt
        finally:
            (self.directory / "collectors-deployment.json").write_text(
                json.dumps(receipt, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
            )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release-dir", type=Path, required=True)
    parser.add_argument("--runtime-image", required=True)
    parser.add_argument("--remote-root", default="/opt/ashare-cross-market")
    parser.add_argument(
        "--verify",
        action="store_true",
        default=True,
        help="Wait for real complete cycles/readback (always enabled in releases)",
    )
    parser.add_argument("--verify-timeout", type=float, default=1800)
    args = parser.parse_args()
    if not 0 < args.verify_timeout < float("inf"):
        parser.error("--verify-timeout must be positive and finite")
    # Validate artifacts and credentials before opening SSH or changing production.
    validate_release(args.release_dir, args.runtime_image)
    private = credentials(dict(os.environ))
    remote = SSHRemote(dict(os.environ))
    try:
        receipt = CollectorDeployer(
            remote, args.release_dir, args.runtime_image, args.remote_root
        ).deploy(private, args.verify_timeout)
        print(
            json.dumps(
                {
                    k: receipt[k]
                    for k in ("status", "revision", "bundle_sha256", "immutable_image_id")
                }
            )
        )
    finally:
        remote.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        reason = str(exc) if isinstance(exc, (DeploymentError, ValueError)) else ""
        print(json.dumps({"status": "failed", "error_type": type(exc).__name__, "reason": reason}))
        raise SystemExit(1) from None
