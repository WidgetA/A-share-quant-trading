# === MODULE PURPOSE ===
# _to_ts_code maps a bare 6-digit code to a Tushare ts_code. Getting 北交所
# (4xxxxx / 8xxxxx / 92xxxx) wrong — mapping it to .SZ instead of .BJ — made
# Tushare query a non-existent symbol and return EMPTY, which silently looked
# like "北交所 minute 数据缺失" even though Tushare has the data. Lock the
# exchange-suffix rules so that regression can't come back.

from __future__ import annotations

import pytest

from src.data.clients.tushare_realtime import TushareRealtimeClient

to = TushareRealtimeClient._to_ts_code


@pytest.mark.parametrize(
    "bare,expected",
    [
        ("600519", "600519.SH"),  # Shanghai main
        ("688981", "688981.SH"),  # STAR
        ("000001", "000001.SZ"),  # Shenzhen main
        ("300750", "300750.SZ"),  # ChiNext
        ("002594", "002594.SZ"),  # SME
        ("920000", "920000.BJ"),  # 北交所 new 920xxx
        ("920992", "920992.BJ"),
        ("830799", "830799.BJ"),  # 北交所 legacy 8xxxxx
        ("871981", "871981.BJ"),
        ("430047", "430047.BJ"),  # 北交所 legacy 4xxxxx
    ],
)
def test_to_ts_code_exchange_suffix(bare: str, expected: str) -> None:
    assert to(bare) == expected


def test_bse_codes_never_map_to_sz() -> None:
    """The exact regression: 北交所 must NOT become .SZ (→ empty Tushare result)."""
    for bare in ("920000", "920339", "830799", "430047"):
        assert to(bare).endswith(".BJ")
        assert not to(bare).endswith(".SZ")
