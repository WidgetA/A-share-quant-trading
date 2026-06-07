# === MODULE PURPOSE ===
# Tests for the kimi output parser (src/data/services/kimi_listing_verifier).
# parse_kimi_output is the fragile part — kimi's repr-style --print output
# wraps long Chinese across console width, injecting raw control chars into
# JSON. These tests pin that behaviour without spawning kimi.

from __future__ import annotations

from src.data.services.kimi_listing_verifier import (
    _classify_unparseable,
    _looks_like_api_auth_failure,
    _read_result_file,
    finding_from_result,
    parse_kimi_output,
)


def test_incidental_auth_words_in_trace_are_not_an_auth_failure():
    # REGRESSION (301669 "请检查 kimi 凭证" false alarm): a successful 100K search trace
    # routinely *mentions* 登录/凭证/401/403 (page content + URLs). That must NOT be flagged
    # as an API auth failure — only precise response-format phrases count.
    trace = (
        "正在搜索东方财富… 需要登录查看完整数据… 来源 http://cfi.cn/p20260531000403.html …"
        "存托凭证相关… 状态码 401/403 在文中出现… 已找到挂牌信息。" * 80
    )
    assert _looks_like_api_auth_failure(trace) is False


def test_real_api_auth_rejection_is_flagged():
    # precise LLM-API auth-rejection formats (Anthropic-style + OpenAI-compatible) ARE caught
    assert _looks_like_api_auth_failure("... Error code: 401 - Invalid Authentication ...") is True
    assert _looks_like_api_auth_failure("openai.AuthenticationError: bad key") is True
    assert _looks_like_api_auth_failure("HTTP error code: 403 forbidden") is True
    assert _looks_like_api_auth_failure("{'error': {'code': 'invalid_api_key'}}") is True
    assert _looks_like_api_auth_failure("Incorrect API key provided: sk-xxx") is True


def test_fetched_page_403_is_not_kimi_auth():
    # a web page kimi FETCHED returning 403/unauthorized is NOT a kimi-token problem — must
    # not be flagged as an API auth failure (the short-output heuristic covers a real death).
    assert _looks_like_api_auth_failure("the fetched page returned 403 Forbidden") is False
    assert _looks_like_api_auth_failure("unauthorized access to this paywalled article") is False


def test_classify_unparseable_reports_what_it_actually_is():
    assert _classify_unparseable("", None) == "kimi 无任何输出"
    assert "退出码 1" in _classify_unparseable("partial output that is short", 1)
    # a LONG search trace we couldn't parse → honest "解析失败" (it actually ran + searched)
    long_trace = "搜索东方财富 抓取页面 分析结果 " * 1000
    assert "解析失败" in _classify_unparseable(long_trace, 0)
    # a long trace full of incidental auth words is STILL just "解析失败", NEVER "凭证"
    noisy = "登录 凭证 401 403 credential login quota " * 300
    assert "解析失败" in _classify_unparseable(noisy, 0)
    assert "凭证" not in _classify_unparseable(noisy, 0)
    # a SHORT output = kimi barely ran → flag as suspected startup/auth failure (read trace),
    # rather than silently calling a real auth death "解析失败"
    assert "疑似" in _classify_unparseable("AuthError occurred, exiting", 0)


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


def test_prefers_informative_answer_over_prompt_not_found_echo():
    # The prompt embeds a "查不到" example with the code filled in (so it shows up in the
    # trace). kimi's REAL answer can legitimately have list_date=null — 未上市 (new IPO not
    # trading yet) / 迁移 / 已退市. The parser must return kimi's real, informative answer,
    # NOT the prompt's echoed "error: not found" example.
    # Regression: 高特电子 301669 = 未上市 was mangled into "查不到 / 名字未知".
    raw = (
        # prompt's echoed fallback example — appears BEFORE kimi's answer
        '{"code":"301669","name":null,"list_date":null,"delist_date":null,'
        '"status":"查不到","new_code":null,"note":"多方查证仍无结果",'
        '"source":null,"error":"not found"}\n'
        "查证结果汇总:高特电子,创业板新股,尚未挂牌交易。\n"
        "```json\n"
        '{"code":"301669","name":"杭州高特电子设备股份有限公司","list_date":null,'
        '"delist_date":null,"status":"未上市","new_code":null,'
        '"note":"创业板新股,已申购缴款但截至今日尚未挂牌交易","source":"http://data.eastmoney.com/x"}\n'
        "```"
    )
    out = parse_kimi_output(raw, "301669")
    assert out is not None
    assert out["name"] == "杭州高特电子设备股份有限公司"
    assert out["status"] == "未上市"
    assert out["list_date"] is None
    assert out.get("error") != "not found"


def test_read_result_file_returns_clean_answer(tmp_path):
    # The file channel: kimi writes ONLY its answer JSON to the file (no prompt-echo),
    # so reading it back is unambiguous — even for a null-list_date 未上市 answer.
    p = tmp_path / "301669.json"
    p.write_text(
        '{"code":"301669","name":"杭州高特电子设备股份有限公司","list_date":null,'
        '"status":"未上市","note":"创业板新股,尚未挂牌","source":"http://x"}',
        encoding="utf-8",
    )
    out = _read_result_file(str(p), "301669")
    assert out is not None
    assert out["name"] == "杭州高特电子设备股份有限公司"
    assert out["status"] == "未上市"
    assert out["list_date"] is None


def test_read_result_file_missing_or_empty_returns_none(tmp_path):
    assert _read_result_file(str(tmp_path / "nope.json"), "301669") is None
    empty = tmp_path / "empty.json"
    empty.write_text("", encoding="utf-8")
    assert _read_result_file(str(empty), "301669") is None


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
    result["new_code"] = "920779"
    f = finding_from_result("830779", result)
    assert f["status"] == "迁移新代码"
    assert f["note"] == "老北交所代码,已迁到新代码"
    assert f["delist_date"] == "2023-09-01"
    assert f["new_code"] == "920779"  # captured for the code-change alias


def test_finding_new_code_rejected_when_bogus():
    # new_code must be a real 6-digit code different from the old one.
    for bogus in (None, "92077", "abcdef", "830779"):
        f = finding_from_result("830779", {"code": "830779", "new_code": bogus})
        assert f["new_code"] is None


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
