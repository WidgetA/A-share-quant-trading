"""Resolve usable original reference inputs without replacing a daily decision."""

from datetime import date
from typing import Any


async def effective_reference_status(
    repository: Any,
    official_status: Any,
    *,
    official_stream_id: str,
    lineage_id: str,
    trade_date: date,
) -> Any:
    """Use the first valid recalculation only when the official slot is invalid.

    No prices are fetched here, no state is advanced, and no receipt deadline is
    changed. D0 collection and D1 arbitration still enforce their original raw
    evidence clocks. An unresolved failure stays invalid rather than closed.
    """
    if official_status is None or official_status.action != "INPUT_INVALID":
        return official_status
    recovered = await repository.get_valid_selection_reference(
        official_stream_id, trade_date, lineage_id=lineage_id
    )
    return recovered if recovered is not None else official_status
