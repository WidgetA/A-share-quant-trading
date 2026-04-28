# Tests for stock_blacklist module.
# The blacklist is hardcoded in source; we patch BLACKLISTED_STOCKS to test
# the filter behaviour without depending on which codes are currently banned.

from unittest.mock import patch

from src.strategy.filters import stock_blacklist


class TestIsBlacklisted:
    def test_returns_true_for_listed_code(self):
        with patch.dict(stock_blacklist.BLACKLISTED_STOCKS, {"600000": "test"}, clear=True):
            assert stock_blacklist.is_blacklisted("600000") is True

    def test_returns_false_for_unlisted_code(self):
        with patch.dict(stock_blacklist.BLACKLISTED_STOCKS, {"600000": "test"}, clear=True):
            assert stock_blacklist.is_blacklisted("000001") is False

    def test_returns_false_when_blacklist_empty(self):
        with patch.dict(stock_blacklist.BLACKLISTED_STOCKS, {}, clear=True):
            assert stock_blacklist.is_blacklisted("600000") is False


class TestFilterBlacklisted:
    def test_filters_out_blacklisted_codes(self):
        with patch.dict(
            stock_blacklist.BLACKLISTED_STOCKS,
            {"600000": "x", "000002": "y"},
            clear=True,
        ):
            codes = ["600000", "000001", "000002", "600519"]
            assert list(stock_blacklist.filter_blacklisted(codes)) == ["000001", "600519"]

    def test_passthrough_when_blacklist_empty(self):
        with patch.dict(stock_blacklist.BLACKLISTED_STOCKS, {}, clear=True):
            codes = ["600000", "000001"]
            assert list(stock_blacklist.filter_blacklisted(codes)) == codes


class TestModuleInvariants:
    def test_reasons_are_non_empty_strings(self):
        # Catches accidentally adding a code with reason=None or "".
        # If this fails, fill in a real reason — that's the audit trail.
        for code, reason in stock_blacklist.BLACKLISTED_STOCKS.items():
            assert isinstance(reason, str) and reason.strip(), (
                f"Blacklist entry {code!r} has empty reason — fill it in"
            )

    def test_codes_are_six_digit_bare_codes(self):
        for code in stock_blacklist.BLACKLISTED_STOCKS:
            assert len(code) == 6 and code.isdigit(), (
                f"Blacklist entry {code!r} is not a 6-digit bare code"
            )
