from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock

from src.strategy.v24_5.references import effective_reference_status


async def test_failed_slot_does_not_hide_later_valid_reference_or_mutate_slot():
    invalid = SimpleNamespace(action="INPUT_INVALID", event_id="failed")
    valid = SimpleNamespace(action="BLOCK", event_id="valid")
    repository = SimpleNamespace(get_valid_selection_reference=AsyncMock(return_value=valid))
    result = await effective_reference_status(
        repository,
        invalid,
        official_stream_id="old-stream",
        lineage_id="old-lineage",
        trade_date=date(2026, 11, 16),
    )
    assert result is valid
    assert invalid.action == "INPUT_INVALID" and invalid.event_id == "failed"
    repository.get_valid_selection_reference.assert_awaited_once_with(
        "old-stream", date(2026, 11, 16), lineage_id="old-lineage"
    )


async def test_no_valid_data_stays_unknown_and_does_not_manufacture_a_reference():
    invalid = SimpleNamespace(action="INPUT_INVALID")
    repository = SimpleNamespace(get_valid_selection_reference=AsyncMock(return_value=None))
    assert (
        await effective_reference_status(
            repository,
            invalid,
            official_stream_id="old-stream",
            lineage_id="old-lineage",
            trade_date=date(2026, 11, 16),
        )
        is invalid
    )


async def test_existing_valid_daily_reference_stays_first_even_if_new_manual_result_differs():
    first = SimpleNamespace(action="ENTER", event_id="first")
    repository = SimpleNamespace(get_valid_selection_reference=AsyncMock())
    assert (
        await effective_reference_status(
            repository,
            first,
            official_stream_id="old-stream",
            lineage_id="old-lineage",
            trade_date=date(2026, 11, 16),
        )
        is first
    )
    repository.get_valid_selection_reference.assert_not_awaited()
