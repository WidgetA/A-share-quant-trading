from __future__ import annotations

from argparse import Namespace
from datetime import date

import pytest

import scripts.export_v20_checkpoint as exporter
from scripts.export_v20_checkpoint import _write_immutable


def test_checkpoint_writer_is_idempotent_but_never_overwrites(tmp_path) -> None:
    target = tmp_path / "accepted.json"
    first = b'{"schema_version":"v20-bootstrap-checkpoint/v2"}\n'

    _write_immutable(target, first)
    _write_immutable(target, first)

    assert target.read_bytes() == first
    with pytest.raises(FileExistsError, match="refusing to overwrite"):
        _write_immutable(target, b"different\n")
    assert target.read_bytes() == first


@pytest.mark.asyncio
async def test_export_connects_without_running_migrations(tmp_path, monkeypatch) -> None:
    class Repository:
        def __init__(self) -> None:
            self.connect_kwargs = None
            self.closed = False

        async def connect(self, **kwargs) -> None:
            self.connect_kwargs = kwargs

        async def export_bootstrap_checkpoint(self, **_kwargs):
            return {
                "schema_version": "v20-bootstrap-checkpoint/v2",
                "state_shadow_batches": [],
            }

        async def close(self) -> None:
            self.closed = True

    repository = Repository()
    monkeypatch.setattr(
        exporter,
        "create_v20_repository_from_config",
        lambda _path: repository,
    )
    args = Namespace(
        database_config=tmp_path / "database.yaml",
        source_stream="shadow-stream",
        source_lineage="shadow-lineage",
        target_stream="production-stream",
        target_lineage="production-lineage",
        as_of=date(2026, 8, 31),
        output=tmp_path / "checkpoint.json",
    )

    await exporter._run(args)

    assert repository.connect_kwargs == {"migrate": False}
    assert repository.closed is True
