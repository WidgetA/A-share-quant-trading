from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from src.data.clients.tushare_realtime import TushareMinuteBar
from src.data.clients.v20_market_data import (
    ENTRY_REQUIRED_LABELS,
    V20EarlyBarCollector,
    V20MarketDataError,
    exact_reference_prices,
)


def _bar(code: str, label: str, *, close: float = 10.0) -> TushareMinuteBar:
    return TushareMinuteBar(
        stock_code=code,
        bar_end=datetime.fromisoformat(f"2026-08-31T{label}:00").replace(
            tzinfo=ZoneInfo("Asia/Shanghai")
        ),
        end_label=label,
        open_price=9.9,
        close_price=close,
        high_price=max(10.1, close),
        low_price=9.8,
        volume=100.0,
        amount=1000.0,
    )


def _early_path(code: str) -> list[TushareMinuteBar]:
    return [_bar(code, label) for label in ENTRY_REQUIRED_LABELS]


def test_freeze_requires_exact_0939_and_never_falls_back() -> None:
    collector = V20EarlyBarCollector(date(2026, 8, 31), ["000001", "600000"])
    collector.ingest(_early_path("000001"))
    collector.ingest([_bar("600000", "09:38")])

    snapshot = collector.freeze()

    assert tuple(snapshot.quotes) == ("000001",)
    assert snapshot.missing_codes == ("600000",)
    assert snapshot.last_complete_label == "09:39"
    assert snapshot.quotes["000001"].early_close == 10.0
    assert snapshot.quotes["000001"].early_volume == 900.0


def test_terminal_0939_without_full_volume_path_is_not_usable() -> None:
    collector = V20EarlyBarCollector(date(2026, 8, 31), ["000001"])
    collector.ingest([_bar("000001", "09:39")])

    snapshot = collector.freeze()

    assert snapshot.quotes == {}
    assert snapshot.missing_codes == ("000001",)
    assert collector.incomplete_codes() == ("000001",)


def test_breadth_terminal_snapshot_needs_only_unconflicted_0939() -> None:
    collector = V20EarlyBarCollector(date(2026, 8, 31), ["000001", "600000"])
    collector.ingest([_bar("000001", "09:35"), _bar("000001", "09:39")])
    collector.ingest([_bar("000001", "09:35", close=10.2)])
    collector.ingest([_bar("600000", "09:39"), _bar("600000", "09:39", close=10.2)])

    snapshot = collector.freeze_terminal()

    assert tuple(snapshot.quotes) == ("000001",)
    assert snapshot.quotes["000001"].early_close == 10.0
    assert snapshot.missing_codes == ("600000",)
    assert snapshot.conflict_codes == ("600000",)


def test_conflicting_same_label_revision_excludes_only_that_code() -> None:
    collector = V20EarlyBarCollector(date(2026, 8, 31), ["000001", "600000"])
    collector.ingest(_early_path("600000"))
    collector.ingest([_bar("000001", "09:39")])
    collector.ingest([_bar("000001", "09:39", close=10.2)])

    snapshot = collector.freeze()

    assert tuple(snapshot.quotes) == ("600000",)
    assert snapshot.missing_codes == ("000001",)
    assert snapshot.conflict_codes == ("000001",)


def test_reference_profile_accepts_only_raw_0941_open() -> None:
    zero_flow = _bar("000002", "09:41")
    object.__setattr__(zero_flow, "volume", 0.0)
    prices, missing, source_hash = exact_reference_prices(
        {
            "000001": _bar("000001", "09:41"),
            "000002": zero_flow,
            "600000": _bar("600000", "09:40"),
        },
        ["000001", "000002", "600000"],
        trade_date=date(2026, 8, 31),
    )

    assert prices == {"000001": 9.9}
    assert missing == ("000002", "600000")
    assert len(source_hash) == 64


def test_entry_and_reference_profiles_cannot_be_relabeled() -> None:
    collector = V20EarlyBarCollector(date(2026, 8, 31), ["000001"])
    collector.ingest([_bar("000001", "09:39")])

    with pytest.raises(ValueError, match="requires raw label 09:39"):
        collector.freeze(expected_label="09:38")

    with pytest.raises(ValueError, match="requires raw label 09:41"):
        exact_reference_prices(
            {"000001": _bar("000001", "09:40")},
            ["000001"],
            trade_date=date(2026, 8, 31),
            expected_label="09:40",
        )


def test_naive_or_mislabeled_bar_is_rejected() -> None:
    collector = V20EarlyBarCollector(date(2026, 8, 31), ["000001"])
    naive = _bar("000001", "09:39")
    object.__setattr__(naive, "bar_end", naive.bar_end.replace(tzinfo=None))

    with pytest.raises(V20MarketDataError, match="invalid minute bar"):
        collector.ingest([naive])
