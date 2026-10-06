"""The production CLI uses the proven SQL timeout without a new key-location rule."""

from types import SimpleNamespace

import pytest

from scripts import backfill_cross_market_massive as cli


def test_existing_ignored_dotenv_location_is_accepted(tmp_path, monkeypatch):
    key = tmp_path / ".env"
    key.write_text("MASSIVE_API_KEY=fake-unit-key\n", encoding="utf8")
    monkeypatch.setattr(cli, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr("sys.argv", ["backfill", "--key-file", str(key)])
    args = cli.arguments()
    assert args.key_file == key


@pytest.mark.asyncio
async def test_cli_large_backfill_uses_proven_120_second_readback_timeout(tmp_path, monkeypatch):
    opened = {}

    class Client:
        def __init__(self, **kwargs):
            opened["source"] = kwargs

        async def aclose(self):
            pass

    class Store:
        def __init__(self, url, **kwargs):
            opened["store"] = {"url": url, **kwargs}

        async def aclose(self):
            pass

    class Producer:
        def __init__(self, **kwargs):
            pass

        async def run_once(self, **kwargs):
            opened["window"] = kwargs
            return {"status": "verified", "verified_rows": 2}

    monkeypatch.setattr(cli, "MassiveIndexClient", Client)
    monkeypatch.setattr(cli, "CrossMarketStore", Store)
    monkeypatch.setattr(cli, "CrossMarketMassiveIngestor", Producer)
    monkeypatch.setattr(
        "src.data.cross_market_massive_ingest.load_massive_reference", lambda *args: {}
    )
    args = SimpleNamespace(
        reference=tmp_path / "ref.json",
        industry_reference=tmp_path / "base.json",
        key_file=tmp_path / "external.env",
        key_variable="MASSIVE_API_KEY",
        proxy=None,
        state_dir=tmp_path / "state",
        batch_size=1000,
        greptime_url="http://unit-sql-relay",
        start_date="2023-01-01",
        end_date="2026-10-06",
    )
    assert await cli.run(args) == 0
    assert opened["store"] == {"url": args.greptime_url, "batch_size": 1000, "timeout": 120}
    assert opened["source"]["rate_state_path"] == tmp_path / "state/rate.json"
    assert opened["window"] == {"start_date": "2023-01-01", "end_date": "2026-10-06"}
