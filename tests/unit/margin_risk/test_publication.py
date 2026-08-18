from __future__ import annotations

from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from src.margin_risk.publication import (
    MARGIN_PUBLISH_TIME,
    latest_published_trade_date,
)

BEIJING_TZ = ZoneInfo("Asia/Shanghai")

# Thu, Fri, Mon, Tue — a weekend sits between 08-07 and 08-10.
OPEN_DAYS = [date(2026, 8, 6), date(2026, 8, 7), date(2026, 8, 10), date(2026, 8, 11)]


def _beijing(day: date, hour: int, minute: int) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=BEIJING_TZ)


def test_publication_time_is_0910_beijing() -> None:
    assert (MARGIN_PUBLISH_TIME.hour, MARGIN_PUBLISH_TIME.minute) == (9, 10)


def test_before_publication_the_previous_session_is_not_available_yet() -> None:
    # 03:00 on a trading day: today's 09:10 publication has not happened, so the
    # newest available day is the one published yesterday morning.
    assert latest_published_trade_date(OPEN_DAYS, now=_beijing(date(2026, 8, 11), 3, 0)) == date(
        2026, 8, 7
    )
    assert latest_published_trade_date(OPEN_DAYS, now=_beijing(date(2026, 8, 11), 9, 9)) == date(
        2026, 8, 7
    )


def test_after_publication_the_previous_session_becomes_available() -> None:
    assert latest_published_trade_date(OPEN_DAYS, now=_beijing(date(2026, 8, 11), 9, 10)) == date(
        2026, 8, 10
    )
    assert latest_published_trade_date(OPEN_DAYS, now=_beijing(date(2026, 8, 11), 15, 0)) == date(
        2026, 8, 10
    )


def test_weekend_and_monday_morning_follow_trading_days_not_calendar_days() -> None:
    # Friday's data is only published on Monday 09:10 — a naive "today - 1 day"
    # would wrongly consider it available all weekend.
    assert latest_published_trade_date(OPEN_DAYS, now=_beijing(date(2026, 8, 8), 12, 0)) == date(
        2026, 8, 6
    )
    assert latest_published_trade_date(OPEN_DAYS, now=_beijing(date(2026, 8, 10), 3, 0)) == date(
        2026, 8, 6
    )
    assert latest_published_trade_date(OPEN_DAYS, now=_beijing(date(2026, 8, 10), 9, 30)) == date(
        2026, 8, 7
    )


def test_no_published_day_when_the_calendar_has_no_earlier_session() -> None:
    assert latest_published_trade_date([], now=_beijing(date(2026, 8, 11), 10, 0)) is None
    first_only = [date(2026, 8, 11)]
    assert latest_published_trade_date(first_only, now=_beijing(date(2026, 8, 11), 10, 0)) is None


def test_non_beijing_timezones_are_converted_before_comparing() -> None:
    # 01:30 UTC == 09:30 Beijing → the 09:10 publication already happened.
    utc_now = datetime(2026, 8, 11, 1, 30, tzinfo=timezone.utc)
    assert latest_published_trade_date(OPEN_DAYS, now=utc_now) == date(2026, 8, 10)


def test_naive_datetime_is_rejected_instead_of_silently_assumed() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        latest_published_trade_date(OPEN_DAYS, now=datetime(2026, 8, 11, 10, 0))
