"""Order the v15 FC and collector release inside CI's shared publication lock.

An older ancestor cannot overwrite any already deployed descendant. A newer
branch tip containing only docs does not block a still-needed runtime release.
"""
# The direct script entry needs the repository root before importing scripts/.
# ruff: noqa: E402

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from build_release import _atomic_write, verify_release
from deploy_collectors import NAMES, CollectorDeployer, RemoteJSONError, SSHRemote, credentials
from deploy_fc import (
    FUNCTION,
    REGION,
    SOURCE_REVISION_ENV,
    _field,
    _models,
    _sdk_client,
    _successful,
)
from deploy_fc import deploy_release as deploy_fc

from scripts.cross_market_release_changes import _ensure_commit, _git, latest_branch_head

BRANCH = "refactor/cleanup-v15-only"


class ReleaseOrderError(ValueError):
    code = "ReleaseOrderNotEstablished"


def _ancestor(repository: Path, older: str, newer: str) -> bool:
    result = _git(repository, "merge-base", "--is-ancestor", older, newer, check=False)
    if result.returncode not in (0, 1):
        raise ReleaseOrderError("Commit ancestry could not be established")
    return result.returncode == 0


def release_decision(repository: Path, incoming: str, head: str, deployed: dict) -> dict:
    for revision in (incoming, head, *[v for v in deployed.values() if v]):
        if not isinstance(revision, str) or not re.fullmatch(r"[0-9a-f]{40}", revision):
            raise ReleaseOrderError("A deployment revision is invalid")
        _ensure_commit(repository, revision)
    if not _ancestor(repository, incoming, head):
        return {
            "status": "superseded",
            "reason": "no_longer_on_branch",
            "revision": incoming,
            "branch_head": head,
            "deployed_revisions": deployed,
        }
    newer = {
        target: rev
        for target, rev in deployed.items()
        if rev and rev != incoming and _ancestor(repository, incoming, rev)
    }
    return {
        "status": "superseded" if newer else "ready",
        "revision": incoming,
        "reason": "deployed_descendant" if newer else "not_superseded",
        "branch_head": head,
        "deployed_revisions": deployed,
        "newer_deployed": newer,
    }


def deployed_revisions(remote, client, remote_root: str, models=None) -> dict:
    current = _successful(client.get_function(FUNCTION, (models or _models()).GetFunctionRequest()))
    if _field(current, "function_name") != FUNCTION:
        raise ReleaseOrderError("The existing function is not the v15 target")
    fc_revision = (_field(current, "environment_variables") or {}).get(SOURCE_REVISION_ENV)
    # Only public version markers are returned; never return the host's env or
    # docker inspect's credential-bearing complete Config.Env.
    code = r"""import pathlib,json,subprocess,sys
base=pathlib.Path(sys.argv[1]); result={}
p=base/'.env'
if p.exists():
 for line in p.read_text().splitlines():
  key,sep,value=line.partition('=')
  if sep and key=='CROSS_MARKET_SOURCE_REVISION': result['host']=value
fmt='{{ index .Config.Labels "org.ashare.cross-market.source-revision" }}'
for key,name in zip(('daily','intraday'),json.loads(sys.argv[2])):
 response=subprocess.run(['docker','inspect','--format',fmt,name],capture_output=True,text=True)
 if response.returncode==0:
  value=response.stdout.strip()
  if value not in ('','<no value>'):result[key]=value
print(json.dumps(result))
"""
    deployed = json.loads(remote.run(["python3", "-c", code, remote_root, json.dumps(NAMES)]))
    if not isinstance(deployed, dict) or set(deployed) - {"host", "daily", "intraday"}:
        raise ReleaseOrderError("Existing collector revisions could not be read")
    return {"fc": fc_revision, **deployed}


def deploy_all(
    *,
    release_dir: Path,
    source_root: Path,
    runtime_image: str,
    remote_root: str = "/opt/ashare-cross-market",
    branch: str = BRANCH,
    client=None,
    endpoint=None,
    remote=None,
    private: bytes | None = None,
    head: str | None = None,
    models=None,
    fc_deploy=None,
    collector_factory=None,
    verify_timeout: float = 1800,
):
    os.environ["DEBUG"] = ""
    if branch != BRANCH:
        raise ReleaseOrderError("Only the v15 infrastructure branch is targeted")
    if not 0 < verify_timeout < float("inf"):
        raise ReleaseOrderError("Verification timeout must be positive and finite")
    if (
        not re.fullmatch(r"/[a-zA-Z0-9._/-]+", remote_root)
        or remote_root == "/"
        or ".." in Path(remote_root).parts
    ):
        raise ReleaseOrderError("Invalid deployment directory")
    manifest = verify_release(release_dir)
    incoming = manifest["revision"]
    if (
        not re.fullmatch(r"[a-z0-9][a-z0-9._:/-]*:[0-9a-f]{40}", runtime_image)
        or runtime_image.rsplit(":", 1)[-1] != incoming
    ):
        raise ReleaseOrderError("Runtime image is not the incoming revision")
    # CI checks out complete history. Read the remote branch now, rather than
    # making the queued event's HEAD stand in for the current branch head.
    head = head or latest_branch_head(source_root, branch)
    if client is None:
        client, endpoint = _sdk_client(REGION)
    owned_remote = remote is None
    remote = remote if remote is not None else SSHRemote(dict(os.environ))
    try:
        current = deployed_revisions(remote, client, remote_root, models)
        decision = release_decision(source_root, incoming, head, current)
        _atomic_write(
            Path(release_dir) / "release-order.json", json.dumps(decision, indent=2).encode()
        )
        if decision["status"] == "superseded":
            return decision
        if private is None:
            private = credentials(dict(os.environ))
        fc_receipt = (fc_deploy or deploy_fc)(
            release_dir=release_dir, client=client, endpoint=endpoint
        )
        domestic = (collector_factory or CollectorDeployer)(
            remote, release_dir, runtime_image, remote_root
        ).deploy(private, verify_timeout=verify_timeout)
        if fc_receipt.get("verified") is not True or domestic.get("status") != "verified":
            raise ReleaseOrderError("The unified deployment is not fully verified")
        decision.update(status="verified", fc_verified=True, collectors_verified=True)
        _atomic_write(
            Path(release_dir) / "release-order.json", json.dumps(decision, indent=2).encode()
        )
        return decision
    finally:
        if owned_remote:
            remote.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release-dir", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, default=Path("."))
    parser.add_argument("--runtime-image", required=True)
    parser.add_argument("--remote-root", default="/opt/ashare-cross-market")
    parser.add_argument("--verify-timeout", type=float, default=1800)
    args = parser.parse_args(argv)
    os.environ["DEBUG"] = ""
    result = deploy_all(**vars(args))
    print(json.dumps({k: result[k] for k in ("status", "revision", "reason")}))
    return 0


def cli(argv=None):
    try:
        return main(argv)
    except Exception as exc:
        diagnostic = (
            {
                "phase": exc.phase,
                "stdout_bytes": exc.stdout_bytes,
                "json_error_position": exc.json_error_position,
            }
            if isinstance(exc, RemoteJSONError)
            else {}
        )
        print(
            json.dumps(
                {
                    "status": "failed",
                    "error_type": type(exc).__name__,
                    "code": ReleaseOrderError.code
                    if isinstance(exc, ReleaseOrderError)
                    else RemoteJSONError.code
                    if isinstance(exc, RemoteJSONError)
                    else "DeploymentFailed",
                    **diagnostic,
                }
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(cli())
