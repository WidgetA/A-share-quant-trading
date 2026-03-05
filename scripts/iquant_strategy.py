#encoding:gbk
"""
iQuant 回测脚本 — 动量策略 T+1 回测。

在 iQuant (QMT) 5 分钟 K 线回测模式下运行。
策略逻辑：
  - BUY:  09:40 bar，调用远程 /backtest-scan 获取推荐，按账户 95% 资金买入
  - SELL: 次日 09:35 bar，以当日开盘价（FIX 指定价）清仓
"""
import json
import urllib.request

API_BASE = "http://8.159.150.224:8000/api/iquant"
API_KEY = "20f4b05bed1fe28e3c808dabaa5fa37f"
BUY_AMOUNT = 100000

_bt = {"holding": None, "scanned": {}, "trades": 0, "fails": 0, "no_recs": 0, "first_trade": "", "last_trade": ""}

def _api_call(endpoint, method="GET", body=None):
    url = API_BASE + endpoint
    headers = {"X-API-Key": API_KEY, "Content-Type": "application/json"}
    data = json.dumps(body).encode("utf-8") if body else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    resp = urllib.request.urlopen(req, timeout=30)
    return json.loads(resp.read().decode("utf-8"))

def _iquant_code(code):
    if "." in code:
        return code
    return code + (".SH" if code[0] in "65" else ".SZ")

def _get_open_price(ContextInfo, code, bar_date):
    """取当日开盘价：用 5m bar 的第一根 open。拿不到返回 0。"""
    try:
        dt = bar_date.replace("-", "")
        download_history_data(code, "5m", dt, dt)
        data = ContextInfo.get_market_data_ex(
            ["open"], [code], period="5m",
            start_time=dt, end_time=dt
        )
        if data and code in data:
            df = data[code]
            if hasattr(df, "iloc") and len(df) > 0:
                p = float(df["open"].iloc[0])
                if p > 0:
                    return p
    except Exception as e:
        print("[OPEN ERR] %s %s: %s" % (code, bar_date, e))
    return 0

def _print_summary():
    print("=" * 50)
    print("[SUMMARY] Total BUY trades: %d" % _bt["trades"])
    print("[SUMMARY] Scan fails: %d" % _bt["fails"])
    print("[SUMMARY] No recommendation days: %d" % _bt["no_recs"])
    print("[SUMMARY] Dates scanned: %d" % len(_bt["scanned"]))
    print("[SUMMARY] First trade: %s" % _bt["first_trade"])
    print("[SUMMARY] Last trade: %s" % _bt["last_trade"])
    print("=" * 50)


def init(ContextInfo):
    ContextInfo.set_account("test")
    ContextInfo.accID = "test"

def handlebar(ContextInfo):
    timetag = ContextInfo.get_bar_timetag(ContextInfo.barpos)
    bar_date = timetag_to_datetime(timetag, "%Y-%m-%d")
    bar_time = timetag_to_datetime(timetag, "%H:%M")

    if ContextInfo.barpos < 3:
        print("[BAR #%d] date=%s time=%s" % (ContextInfo.barpos, bar_date, bar_time))

    if bar_date < "2025-01-01" or bar_date > "2025-12-31":
        try:
            total = ContextInfo.time_tick_size
            if ContextInfo.barpos >= total - 1 and _bt["trades"] > 0:
                _print_summary()
        except Exception:
            pass
        return

    # ─── SELL: 09:35 bar，用当日开盘价（FIX）卖出 ───
    if bar_time == "09:35" and _bt["holding"]:
        h = _bt["holding"]
        if h["buy_date"] != bar_date:
            code = _iquant_code(h["code"])
            open_price = _get_open_price(ContextInfo, code, bar_date)
            if open_price > 0:
                order_target_percent(code, 0, "FIX", open_price, ContextInfo, ContextInfo.accID)
                print("[SELL %s] %s @ %.2f (open)" % (bar_date, code, open_price))
                _bt["holding"] = None
            else:
                print("[SELL SKIP %s] %s — 拿不到开盘价，保持持仓" % (bar_date, code))

    # ─── BUY: 09:40 bar，用 order_value 按金额买入 ───
    if bar_time == "09:40" and bar_date not in _bt["scanned"]:
        _bt["scanned"][bar_date] = True
        if _bt["holding"]:
            return

        scan_count = len(_bt["scanned"])
        try:
            result = _api_call("/backtest-scan", "POST", {"trade_date": bar_date, "data_source": "tsanghi"})
        except Exception as e:
            _bt["fails"] += 1
            if scan_count <= 20:
                print("[DIAG #%d] %s scan FAIL: %s" % (scan_count, bar_date, e))
            return

        rec = result.get("recommendation")
        if not rec:
            _bt["no_recs"] += 1
            if scan_count <= 20:
                print("[DIAG #%d] %s no rec: %s" % (scan_count, bar_date, result.get("reason", "?")))
            return

        code = _iquant_code(rec["stock_code"])

        # order_percent: 用当前账户 95% 资金买入，自动利滚利
        order_percent(code, 0.95, ContextInfo, ContextInfo.accID)
        _bt["holding"] = {"code": rec["stock_code"], "name": rec.get("stock_name", ""), "buy_date": bar_date}
        _bt["trades"] += 1
        if not _bt["first_trade"]:
            _bt["first_trade"] = "BUY %s %s" % (bar_date, code)
        _bt["last_trade"] = "BUY %s %s" % (bar_date, code)
        print("[BUY %s] %s 95%%" % (bar_date, code))

    # 最后一个 bar 打印汇总
    try:
        total = ContextInfo.time_tick_size
        if ContextInfo.barpos >= total - 1:
            _print_summary()
    except Exception:
        pass
