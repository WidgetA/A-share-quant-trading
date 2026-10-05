"""Checks for the sourced SW2021 L2 board reference, not a trading rule set."""

import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit

import pytest

_REFERENCE_PATH = (
    Path(__file__).resolve().parents[2] / "src/data/reference/cross_market/industry_boards.json"
)
# Independently recomputed from the sourced L2 code/name pairs, not the mapping output.
_SOURCE_PAIRS_SHA256 = "6408d07bb199f85bdcca807a443e2a39c89c89bba74f0b6d23c1d52c6d657a38"
_MARKETS = ("US", "KR")
_STATUSES = {"mapped", "no_match_in_reviewed_catalog", "unverified"}
_RELATIONS = {"same_business", "broader", "narrower", "partial_overlap"}
_EXCLUDED_MISC_BOARD = "KR:NAVER:25"
_BANK_CODES = ("480200", "480300", "480400", "480500", "480600")


@pytest.fixture(scope="module")
def reference() -> dict:
    return json.loads(_REFERENCE_PATH.read_text(encoding="utf-8"))


def _index_unique(items: list[dict], key: str) -> dict[str, dict]:
    identifiers = [item[key] for item in items]
    assert all(isinstance(identifier, str) and identifier for identifier in identifiers)
    assert len(identifiers) == len(set(identifiers)), f"Duplicate {key} references"
    return {item[key]: item for item in items}


def _nonempty_text(value: object) -> None:
    assert isinstance(value, str) and value.strip()


def _public_url(value: object) -> None:
    _nonempty_text(value)
    parsed = urlsplit(value)
    assert parsed.scheme in {"https", "http"} and parsed.netloc
    assert parsed.username is None and parsed.password is None


def test_reference_has_sourced_l2_identity_and_provenance(reference: dict) -> None:
    assert reference["schema_version"] == 1
    _nonempty_text(reference["as_of"])
    datetime.fromisoformat(reference["as_of"].replace("Z", "+00:00"))
    assert reference["taxonomy"]["id"] == "SW2021"
    assert reference["taxonomy"]["level"] == "L2"

    sources = _index_unique(reference["sources"], "id")
    assert reference["taxonomy"]["source_id"] in sources
    for source in sources.values():
        _public_url(source["url"])
        _nonempty_text(source["retrieved_at"])
        datetime.fromisoformat(source["retrieved_at"].replace("Z", "+00:00"))
        assert source["retrieval_method"] in {"http", "web_processed"}
        _nonempty_text(source["locator"])
        assert isinstance(source["facts_supported"], list) and source["facts_supported"]
        for fact in source["facts_supported"]:
            _nonempty_text(fact)
        if source["retrieval_method"] == "http":
            assert re.fullmatch(r"[0-9a-fA-F]{64}", source["raw_sha256"])
        else:
            # A web tool's readable text is evidence, but not an HTTP byte snapshot.
            # In particular, a local 403 body's hash cannot identify the business text.
            assert source["raw_sha256"] is None


def test_all_source_l2_codes_and_names_are_preserved(reference: dict) -> None:
    industries = _index_unique(reference["industries"], "sw_code")
    pairs = sorted((code, industry["sw_name"]) for code, industry in industries.items())
    encoded = json.dumps(pairs, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    assert len(pairs) == 134
    assert hashlib.sha256(encoded).hexdigest() == _SOURCE_PAIRS_SHA256, (
        "Missing, substituted, renamed or invented SW2021 L2 industry"
    )


def test_board_objects_keep_real_identifiers_and_native_levels(reference: dict) -> None:
    sources = _index_unique(reference["sources"], "id")
    boards = _index_unique(reference["boards"], "id")
    assert _EXCLUDED_MISC_BOARD not in boards, "NAVER 기타 mixes fund products"
    for identifier, board in boards.items():
        assert board["listing_market"] in _MARKETS
        assert identifier.startswith(board["listing_market"] + ":")
        _nonempty_text(board["name_original"])
        _nonempty_text(board["provider"])
        _public_url(board["url"])
        assert board["source_id"] in sources
        if identifier.startswith("US:SA:"):
            assert re.fullmatch(r"US:SA:[a-z0-9-]+", identifier)
            assert board["classification_level"] == "industry"
            assert urlsplit(board["url"]).path.rstrip("/") == (
                "/stocks/industry/" + identifier.removeprefix("US:SA:")
            )
        else:
            assert re.fullmatch(r"KR:NAVER:[0-9]+", identifier)
            assert board["classification_level"] == "업종 (upjong)"
            assert urlsplit(board["url"]).path.rstrip("/") == (
                "/market/stock/kr/industry/" + identifier.removeprefix("KR:NAVER:")
            )


def test_each_industry_has_two_consistent_market_results(reference: dict) -> None:
    sources = _index_unique(reference["sources"], "id")
    boards = _index_unique(reference["boards"], "id")
    for industry in reference["industries"]:
        for market in _MARKETS:
            result = industry[market]
            assert result["status"] in _STATUSES
            assert result["reviewed_catalog_source_id"] in sources
            assert isinstance(result["gap_note"], str)
            matches = result["matches"]
            assert isinstance(matches, list)
            match_ids = [match["board_id"] for match in matches]
            assert len(match_ids) == len(set(match_ids))
            if result["status"] == "mapped":
                assert matches, (industry["sw_code"], market, "mapped without a board")
            elif result["status"] == "no_match_in_reviewed_catalog":
                assert not matches, "An absence result cannot also claim matches"
            if result["status"] != "mapped":
                _nonempty_text(result["gap_note"])

            for match in matches:
                assert match["board_id"] != _EXCLUDED_MISC_BOARD
                assert match["board_id"] in boards
                assert boards[match["board_id"]]["listing_market"] == market
                assert match["relation"] in _RELATIONS
                _nonempty_text(match["scope_note"])
                assert match["source_ids"]
                assert all(source_id in sources for source_id in match["source_ids"])

            assert isinstance(result["exclusions"], list)
            assert not set(match_ids) & {
                exclusion["board_id"] for exclusion in result["exclusions"]
            }, "A board cannot be both matched and excluded for the same market result"
            for exclusion in result["exclusions"]:
                excluded_id = exclusion["board_id"]
                # The original mixed-products category may be mentioned only as excluded.
                assert excluded_id in boards or excluded_id == _EXCLUDED_MISC_BOARD
                assert excluded_id.startswith(market + ":")
                _nonempty_text(exclusion["reason"])


def test_semiconductor_mapping_preserves_both_us_boards_and_extra_scope(reference: dict) -> None:
    industry = _index_unique(reference["industries"], "sw_code")["270100"]
    matches = _index_unique(industry["US"]["matches"], "board_id")
    assert "US:SA:semiconductors" in matches
    equipment = matches["US:SA:semiconductor-equipment-and-materials"]
    # DQ is in this real board and describes solar-grade polysilicon, while SW2021
    # places 硅料硅片 under 光伏设备. This is not a pure narrower semiconductor subset.
    assert equipment["relation"] == "partial_overlap"
    assert all(match["relation"] != "same_business" for match in matches.values())
    scope_note = equipment["scope_note"].casefold()
    assert any(term in scope_note for term in ("光伏", "solar", "photovoltaic")), (
        "Observed solar-material scope outside SW semiconductors must remain visible"
    )


@pytest.mark.parametrize("market", _MARKETS)
def test_baijiu_does_not_become_an_equivalent_general_drinks_board(
    reference: dict, market: str
) -> None:
    industry = _index_unique(reference["industries"], "sw_code")["340500"]
    assert all(match["relation"] != "same_business" for match in industry[market]["matches"])


@pytest.mark.parametrize("code", _BANK_CODES)
@pytest.mark.parametrize("market", _MARKETS)
def test_chinese_bank_institution_types_are_not_foreign_business_equivalents(
    reference: dict, code: str, market: str
) -> None:
    industry = _index_unique(reference["industries"], "sw_code")[code]
    assert all(match["relation"] != "same_business" for match in industry[market]["matches"])
