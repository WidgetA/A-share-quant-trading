# === MODULE PURPOSE ===
# Tests for the kimi output parser (src/data/services/kimi_listing_verifier).
# parse_kimi_output is the fragile part — kimi's repr-style --print output
# wraps long Chinese across console width, injecting raw control chars into
# JSON. These tests pin that behaviour without spawning kimi.

from __future__ import annotations

from src.data.services.kimi_listing_verifier import (
    finding_from_result,
    parse_kimi_output,
)


def test_parses_clean_success_json():
    raw = (
        "经搜索新浪财经,贵州茅台于 2001 年上市。\n"
        '{"code":"600519","name":"贵州茅台","list_date":"2001-08-27",'
        '"source":"https://finance.sina.com.cn/600519"}'
    )
    out = parse_kimi_output(raw, "600519")
    assert out is not None
    assert out["code"] == "600519"
    assert out["list_date"] == "2001-08-27"
    assert out["name"] == "贵州茅台"


def test_strips_control_chars_in_wrapped_json():
    # kimi wraps the JSON across the console width, injecting \r\n / \t.
    raw = (
        '{"code":"000001",\r\n"name":"平安银\t行",\r\n"list_date":"1991-04-03","source":"http://x"}'
    )
    out = parse_kimi_output(raw, "000001")
    assert out is not None
    assert out["list_date"] == "1991-04-03"


def test_not_found_returns_error_object():
    raw = (
        "搜索后仍找不到该代码的挂牌日。\n"
        '{"code":"999999","name":null,"list_date":null,"source":null,"error":"not found"}'
    )
    out = parse_kimi_output(raw, "999999")
    assert out is not None
    assert out.get("error") == "not found"
    assert out.get("list_date") is None


def test_prefers_real_date_over_placeholder_echo():
    # kimi sometimes echoes the prompt template placeholder first, then the
    # real answer — we must return the one with a real YYYY-MM-DD.
    raw = (
        '{"code":"600000","name":"<公司中文名>","list_date":"YYYY-MM-DD","source":"<URL>"}\n'
        "经查实际挂牌日如下:\n"
        '{"code":"600000","name":"浦发银行","list_date":"1999-11-10","source":"http://y"}'
    )
    out = parse_kimi_output(raw, "600000")
    assert out is not None
    assert out["list_date"] == "1999-11-10"


def test_no_matching_code_returns_none():
    raw = '{"code":"123456","list_date":"2020-01-01"}'
    assert parse_kimi_output(raw, "600519") is None


def test_empty_output_returns_none():
    assert parse_kimi_output("", "600519") is None


def test_parses_enriched_status_and_note():
    # New prompt asks for status + note + delist_date — they must ride along.
    raw = (
        "830779 是老北交所代码,已迁到 920XXX。\n"
        '{"code":"830779","name":"武汉蓝电","list_date":"2015-06-01",'
        '"delist_date":"2023-09-01","status":"迁移新代码",'
        '"note":"老北交所代码,2023年迁到新代码继续交易","source":"http://x"}'
    )
    out = parse_kimi_output(raw, "830779")
    assert out is not None
    assert out["status"] == "迁移新代码"
    assert out["delist_date"] == "2023-09-01"
    assert "迁到新代码" in out["note"]


def test_finding_uses_kimi_status_and_note():
    result = {
        "code": "830779",
        "name": "武汉蓝电",
        "list_date": "2015-06-01",
        "delist_date": "2023-09-01",
        "status": "迁移新代码",
        "note": "老北交所代码,已迁到新代码",
        "source": "http://x",
    }
    f = finding_from_result("830779", result)
    assert f["status"] == "迁移新代码"
    assert f["note"] == "老北交所代码,已迁到新代码"
    assert f["delist_date"] == "2023-09-01"


def test_finding_infers_status_for_old_style_answer():
    # Old answer with no status field → infer a coarse status from dates.
    delisted = finding_from_result(
        "600355", {"code": "600355", "list_date": "2000-01-01", "delist_date": "2026-04-27"}
    )
    assert delisted["status"] == "已退市"
    trading = finding_from_result("600519", {"code": "600519", "list_date": "2001-08-27"})
    assert trading["status"] == "在交易"
    nf = finding_from_result("999999", {"code": "999999", "error": "not found"})
    assert nf["status"] == "查不到"
    assert nf["note"] == "(kimi 未给出说明)"
