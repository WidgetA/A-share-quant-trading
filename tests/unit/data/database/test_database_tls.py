import hashlib
import ssl
from pathlib import Path

import certifi
import pytest

from src.data.database.fundamentals_db import (
    FundamentalsDB,
    FundamentalsDBConfig,
    create_fundamentals_db_from_config,
)


@pytest.mark.asyncio
async def test_legacy_fundamentals_require_mode_remains_compatible(monkeypatch) -> None:
    captured = {}

    async def create_pool(**kwargs):
        captured.update(kwargs)
        return object()

    monkeypatch.setattr("src.data.database.fundamentals_db.asyncpg.create_pool", create_pool)
    database = FundamentalsDB(FundamentalsDBConfig(ssl_mode="require"))

    await database.connect()

    assert captured["ssl"] == "require"


@pytest.mark.asyncio
async def test_v20_style_fundamentals_verify_full_uses_reviewed_ca(monkeypatch) -> None:
    captured = {}
    ca_path = Path(certifi.where())

    async def create_pool(**kwargs):
        captured.update(kwargs)
        return object()

    monkeypatch.setattr("src.data.database.fundamentals_db.asyncpg.create_pool", create_pool)
    database = FundamentalsDB(
        FundamentalsDBConfig(
            ssl_mode="verify-full",
            ssl_root_cert=str(ca_path),
            ssl_root_cert_sha256=hashlib.sha256(ca_path.read_bytes()).hexdigest(),
        )
    )

    await database.connect()

    assert isinstance(captured["ssl"], ssl.SSLContext)
    assert captured["ssl"].verify_mode == ssl.CERT_REQUIRED
    assert captured["ssl"].check_hostname is True


@pytest.mark.asyncio
async def test_verify_full_fails_before_pool_when_ca_is_missing(monkeypatch) -> None:
    called = False

    async def create_pool(**_kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr("src.data.database.fundamentals_db.asyncpg.create_pool", create_pool)
    database = FundamentalsDB(
        FundamentalsDBConfig(
            ssl_mode="verify-full",
            ssl_root_cert="missing-ca.pem",
            ssl_root_cert_sha256="a" * 64,
        )
    )

    with pytest.raises(ConnectionError, match="CA file is not readable"):
        await database.connect()

    assert called is False


def test_fundamentals_factory_explicit_path_is_independent_of_cwd(tmp_path, monkeypatch) -> None:
    config_path = tmp_path / "deployment" / "database-config.yaml"
    config_path.parent.mkdir()
    config_path.write_text(
        """
database:
  fundamentals:
    host: db.internal
    port: 5432
    database: strategy
    user: reader
    password: secret
    schema: public
    ssl_mode: require
""".strip(),
        encoding="utf-8",
    )
    other_cwd = tmp_path / "other"
    other_cwd.mkdir()
    monkeypatch.chdir(other_cwd)

    database = create_fundamentals_db_from_config(config_path.resolve())

    assert database._config.host == "db.internal"
    assert database._config.ssl_mode == "require"
