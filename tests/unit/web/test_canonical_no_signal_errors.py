"""A valid empty selection must remain distinguishable from lost input data."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from src.strategy.v20.selection_scanner import V16ScanResult
from src.web import v20_canonical_selection as canonical
from tests.unit.web.test_v16_canonical_scan import (
    _compute_whole_ticket_history_case,
)
from tests.unit.web.test_v16_canonical_scan import (
    fakes as _shared_market_fixture,
)


@pytest.fixture(name="fakes")
def _market_fixture(monkeypatch):
    return _shared_market_fixture.__wrapped__(monkeypatch)


async def _case(
    fakes,
    monkeypatch,
    *,
    profile,
    failed_codes,
    empty,
    invalid_ohlc=False,
    early_failure=None,
    short_history=False,
):
    state, _received = await _compute_whole_ticket_history_case(fakes, monkeypatch, 5, failed_codes)
    state.selection_version = profile
    if invalid_ohlc or short_history:
        original_history = state.historical_adapter.history_quotes

        async def malformed_history(**kwargs):
            response = await original_history(**kwargs)
            table = response["tables"][0]["table"]
            if invalid_ohlc:
                table["high"][-1] = 1.0
            else:
                response["tables"][0]["table"] = {
                    field: values[-4:] for field, values in table.items()
                }
            return response

        state.historical_adapter.history_quotes = malformed_history
    if profile == "V24.5":
        original_early = state.realtime_client.batch_get_early_market_data

        async def full_early(codes, expected_trade_date=None):
            response = await original_early(codes, expected_trade_date)
            return {
                code: replace(
                    data,
                    early_bars=tuple(
                        replace(
                            data.early_bars[0],
                            bar_end=data.early_bars[0].bar_end.replace(minute=minute),
                            end_label=f"09:{minute:02}",
                        )
                        for minute in range(30, 40)
                    ),
                )
                for code, data in response.items()
            }

        state.realtime_client.batch_get_early_market_data = full_early
        monkeypatch.setattr(
            "src.strategy.v22_slim.selection.make_scanner",
            lambda: (
                fakes.scanner(),
                SimpleNamespace(model_sha256="m" * 64, feature_list_sha256="f" * 64),
                None,
            ),
        )
    if early_failure:
        complete_early_loader = state.realtime_client.batch_get_early_market_data
        if early_failure == "breadth_only":
            original_closes = state.realtime_client.fetch_prev_closes

            async def expanded_breadth(day):
                return {**await original_closes(day), "000999": 10.5}

            state.realtime_client.fetch_prev_closes = expanded_breadth

        async def incomplete_early(codes, expected_trade_date=None):
            response = await complete_early_loader(codes, expected_trade_date)
            if early_failure in {"missing_response", "breadth_only"}:
                response.pop("000999" if early_failure == "breadth_only" else "600000")
            elif early_failure == "not_trading":
                response["600000"] = replace(
                    response["600000"], quote=replace(response["600000"].quote, open_price=0.0)
                )
            else:
                response["600000"] = replace(
                    response["600000"],
                    early_bars=tuple(
                        bar for bar in response["600000"].early_bars if bar.end_label != "09:39"
                    ),
                )
            return response

        state.realtime_client.batch_get_early_market_data = incomplete_early
    if empty:

        async def no_candidates(self, stock_data, clean_boards):
            type(self).scan_calls += 1
            return V16ScanResult(
                step0_universe_count=len(stock_data),
                step0_codes=sorted(stock_data),
            )

        monkeypatch.setattr(fakes.scanner, "scan", no_candidates)
    return await canonical.compute_canonical_v16_scan(state, fakes.trade_date)


@pytest.mark.parametrize("profile", ["V20", "V24.5"])
async def test_missing_history_cannot_become_normal_no_signal(fakes, monkeypatch, profile):
    """One failed input plus four valid rejects is not evidence of no signal."""
    with pytest.raises(canonical.CanonicalV16ScanError, match="empty selection.*input errors"):
        await _case(fakes, monkeypatch, profile=profile, failed_codes={"600000"}, empty=True)


async def test_invalid_v245_ohlc_cannot_become_new_listing_and_no_signal(fakes, monkeypatch):
    with pytest.raises(canonical.CanonicalV16ScanError, match="empty selection.*input errors"):
        await _case(
            fakes,
            monkeypatch,
            profile="V24.5",
            failed_codes=set(),
            empty=True,
            invalid_ohlc=True,
        )


@pytest.mark.parametrize("profile", ["V20", "V24.5"])
async def test_complete_input_without_candidates_is_normal_empty(fakes, monkeypatch, profile):
    bundle = await _case(fakes, monkeypatch, profile=profile, failed_codes=set(), empty=True)
    assert bundle.scan_result.recommended == []
    assert bundle.failed_no_history == ()
    assert bundle.data_error_notification is None


@pytest.mark.parametrize("profile", ["V20", "V24.5"])
async def test_breadth_only_missing_data_does_not_change_empty_selection(
    fakes, monkeypatch, profile
):
    bundle = await _case(
        fakes,
        monkeypatch,
        profile=profile,
        failed_codes=set(),
        empty=True,
        early_failure="breadth_only",
    )
    assert bundle.scan_result.recommended == []
    assert len(bundle.stock_data) == len(bundle.universe) == 5
    assert bundle.breadth_market_missing_codes == ("000999",)


async def test_known_non_trading_stock_does_not_change_empty_selection(fakes, monkeypatch):
    bundle = await _case(
        fakes,
        monkeypatch,
        profile="V20",
        failed_codes=set(),
        empty=True,
        early_failure="not_trading",
    )
    assert bundle.scan_result.recommended == []
    assert len(bundle.stock_data) == 4
    assert len(bundle.quotes) == 5


async def test_v245_short_history_skip_does_not_change_empty_selection(fakes, monkeypatch):
    bundle = await _case(
        fakes,
        monkeypatch,
        profile="V24.5",
        failed_codes=set(),
        empty=True,
        short_history=True,
    )
    assert bundle.scan_result.recommended == []
    assert len(bundle.stock_data) == 4
    assert bundle.skipped_new_listings == ("600000",)
    assert bundle.data_error_notification is None


@pytest.mark.parametrize("profile", ["V20", "V24.5"])
async def test_existing_minority_tolerance_with_candidates_is_preserved(
    fakes, monkeypatch, profile
):
    bundle = await _case(fakes, monkeypatch, profile=profile, failed_codes={"600000"}, empty=False)
    assert bundle.scan_result.recommended
    assert len(bundle.failed_no_history) == 1
    assert len(bundle.stock_data) == 4


@pytest.mark.parametrize("profile", ["V20", "V24.5"])
@pytest.mark.parametrize("early_failure", ["missing_response", "missing_required_bar"])
async def test_incomplete_realtime_cannot_become_normal_no_signal(
    fakes, monkeypatch, profile, early_failure
):
    with pytest.raises(canonical.CanonicalV16ScanError, match="empty selection.*input errors"):
        await _case(
            fakes,
            monkeypatch,
            profile=profile,
            failed_codes=set(),
            empty=True,
            early_failure=early_failure,
        )


@pytest.mark.parametrize("profile", ["V20", "V24.5"])
@pytest.mark.parametrize("early_failure", ["missing_response", "missing_required_bar"])
async def test_realtime_eighty_percent_tolerance_with_candidates_is_preserved(
    fakes, monkeypatch, profile, early_failure
):
    bundle = await _case(
        fakes,
        monkeypatch,
        profile=profile,
        failed_codes=set(),
        empty=False,
        early_failure=early_failure,
    )
    assert bundle.scan_result.recommended
    assert len(bundle.stock_data) == 4
    assert len(bundle.universe) == 5
    assert bundle.failed_no_history == ()
