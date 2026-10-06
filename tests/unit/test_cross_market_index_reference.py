"""Checks for source-backed identity and the actual disputed business bridges."""

import json
from pathlib import Path

from src.data.cross_market_ingest import load_reference

ROOT = Path(__file__).resolve().parents[2]
DIRECTORY = ROOT / "src/data/reference/cross_market"


def reference():
    return json.loads((DIRECTORY / "industry_indices.json").read_text(encoding="utf-8"))


def symbols(code, market):
    document = reference()
    indices = {item["index_id"]: item for item in document["indices"]}
    row = next(item for item in document["industries"] if item["sw_code"] == code)
    return {indices[m["index_id"]]["symbol"] for m in row["markets"][market]["matches"]}


def test_complete_foundation_and_all_selected_indices_resolve():
    loaded = load_reference(DIRECTORY / "industry_indices.json", DIRECTORY / "industry_boards.json")
    assert len(loaded["mappings"]) == 268
    assert len({row["sw_code"] for row in loaded["mappings"]}) == 134
    document = reference()
    assert {i["index_id"] for i in loaded["indices"]} == {
        i["index_id"] for i in document["indices"]
    }
    for index in loaded["indices"]:
        assert index["instrument_type"] == "INDEX"
        assert index["yahoo_names"] and index["name"]
        assert index["currency"] == {"US": "USD", "KR": "KRW"}[index["market"]]
        assert index["capability"] in {"snapshot_only", "daily_history"}
    for row in document["industries"]:
        for market in ("US", "KR"):
            cell = row["markets"][market]
            if not cell["matches"]:
                assert cell["gap_note"]
            for match in cell["matches"]:
                assert match["scope_note"]


def test_disproved_names_and_nonindustry_substitutes_are_absent():
    all_symbols = {i["symbol"] for i in reference()["indices"]}
    assert not ({"^QGRD", "^GSPC", "^KS11", "^KQ11", "KOSPI-27.KS", "^KQ12"} & all_symbols)
    assert "^NQUSB10102020" not in symbols("270600", "US")  # chemicals != equipment
    assert "^NQUSB651030" not in symbols("760200", "US")  # waste service != filtration equipment
    assert "^NQUSB50206025" not in symbols("420900", "US")  # rolling stock != rail operation
    for code in ("330100", "330300", "330400", "330700"):
        assert not ({"^NQUSB40202015", "^NQUSB40202025"} & symbols(code, "US"))


def test_verified_direct_business_examples_remain_traceable():
    assert "^SOX" in symbols("270100", "US")
    assert "KOSPI-13.KS" in symbols("270100", "KR")
    assert "KOSPI-25.KS" in symbols("490200", "KR")
    assert "^NQUSB40401010" in symbols("450600", "US")  # actual Amazon participation
    assert "KOSPI-11.KS" in symbols("240400", "KR")  # actual Korea Zinc gold/silver
    assert "^KQ27" in symbols("220900", "KR")  # actual WONIK quartz, not name equivalence
    assert "KOSPI-26.KS" in symbols("710300", "KR")  # actual HYOSUNG ITX ITO
