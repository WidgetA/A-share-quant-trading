# === MODULE PURPOSE ===
# Tests for the gap classifier pure logic — how each missing (day, stock) is
# sorted into A (name-list wrong) / B (real-missing) / C (legitimately no data)
# / PENDING (needs AI listing-date check first).

from __future__ import annotations

from datetime import date

from src.data.services.gap_classifier import (
    CLASS_A,
    CLASS_B,
    CLASS_C,
    PENDING,
    classify_daily_gap,
    classify_minute_gap,
    summarize_classes,
)

D = date(2024, 6, 3)


# ---------------- daily gaps ----------------


def test_daily_not_yet_listed_is_A():
    meta = {"list_date": date(2025, 1, 1), "delist_date": None, "verified": True}
    cls, why = classify_daily_gap(D, meta, suspended=False)
    assert cls == CLASS_A and "还没上市" in why


def test_daily_already_delisted_is_A():
    meta = {"list_date": date(2010, 1, 1), "delist_date": date(2024, 1, 1), "verified": True}
    cls, why = classify_daily_gap(D, meta, suspended=False)
    assert cls == CLASS_A and "已退市" in why


def test_daily_suspended_is_C():
    meta = {"list_date": date(2010, 1, 1), "delist_date": None, "verified": True}
    cls, why = classify_daily_gap(D, meta, suspended=True)
    assert cls == CLASS_C and "停牌" in why


def test_daily_unverified_is_pending():
    cls, why = classify_daily_gap(D, None, suspended=False)
    assert cls == PENDING
    # also: has a row but verified=False
    cls2, _ = classify_daily_gap(D, {"list_date": None, "verified": False}, suspended=False)
    assert cls2 == PENDING


def test_daily_listed_not_suspended_is_B():
    meta = {"list_date": date(2010, 1, 1), "delist_date": None, "verified": True}
    cls, why = classify_daily_gap(D, meta, suspended=False)
    assert cls == CLASS_B and "重下" in why


# ---------------- minute gaps ----------------


def test_minute_ipo_day_is_C():
    meta = {"list_date": D, "delist_date": None, "verified": True}
    cls, why = classify_minute_gap(D, meta, minute_bar_count=120)
    assert cls == CLASS_C and "上市首日" in why


def test_minute_zero_bars_is_B():
    cls, why = classify_minute_gap(D, None, minute_bar_count=0)
    assert cls == CLASS_B and "重下" in why


def test_minute_partial_bars_is_B():
    cls, why = classify_minute_gap(D, {"list_date": date(2010, 1, 1), "verified": True}, 200)
    assert cls == CLASS_B and "200/241" in why


# ---------------- summary ----------------


def test_summarize_counts():
    items = [{"class": CLASS_A}, {"class": CLASS_A}, {"class": CLASS_B}, {"class": PENDING}]
    out = summarize_classes(items)
    assert out == {CLASS_A: 2, CLASS_B: 1, CLASS_C: 0, PENDING: 1}
