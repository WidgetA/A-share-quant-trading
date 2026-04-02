#encoding:gbk
"""
iQuant 实盘下单脚本 — 轮询服务端 /pending-signals, 用 passorder() 执行。

在 iQuant (QMT) 1 分钟 K 线实盘模式下运行。

两个独立循环:
  - 状态线程 (30s): 心跳 + 把 _state 中的持仓/余额发到服务端 (纯 HTTP, 不调 QMT API)
  - handlebar (盘中): 更新持仓/余额到 _state + 轮询信号 + 下单

QMT API (get_trade_detail_data / passorder) 只能在主线程调用。
"""
import json
import threading
import time
import urllib.request

API_BASE = "http://8.159.150.224:8000/api/iquant"
API_KEY = "20f4b05bed1fe28e3c808dabaa5fa37f"

_state = {
    "accID": "",
    "last_poll_ts": 0,
    "startup_reported": False,
    "fields_printed": False,
    "pos_fields_printed": False,
    # Broker state — written by main thread (init/handlebar), read by state thread
    "positions": [],
    "cash": 0,
}


# ── HTTP helpers ──────────────────────────────────────────────

def _api_call(endpoint, method="GET", body=None):
    url = API_BASE + endpoint
    headers = {"X-API-Key": API_KEY, "Content-Type": "application/json"}
    data = json.dumps(body).encode("utf-8") if body else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    resp = urllib.request.urlopen(req, timeout=30)
    return json.loads(resp.read().decode("utf-8"))


# ── Broker query helpers (main thread only) ───────────────────

def _iquant_code(code):
    """转为 iQuant 格式: 601398 -> 601398.SH"""
    if "." in code:
        return code
    return code + (".SH" if code[0] in "65" else ".SZ")


def _get_available_cash(accID):
    """查询可用资金。柜台未连接时返回 0。"""
    try:
        acct = get_trade_detail_data(accID, 'stock', 'account')
        if acct:
            obj = acct[0]
            if not _state["fields_printed"]:
                _state["fields_printed"] = True
                attrs = [a for a in dir(obj) if a.startswith('m_')]
                print("[ACCT] fields: %s" % attrs)
            return obj.m_dAvailable
        else:
            print("[ACCT] empty — 柜台可能未连接")
    except Exception as e:
        print("[ACCT ERR] %s" % e)
    return 0


def _get_position_volume(accID, code):
    """查询某只股票的可卖数量。"""
    try:
        positions = get_trade_detail_data(accID, 'stock', 'position')
        for pos in (positions or []):
            pos_code = pos.m_strInstrumentID
            if pos_code == code or _iquant_code(pos_code.split(".")[0]) == code:
                return int(pos.m_nCanUseVolume)
    except Exception as e:
        print("[POS ERR] %s" % e)
    return 0


def _get_all_positions(accID):
    """查询券商全部持仓, 返回 [{code, volume}]。柜台未连接时返回空列表。"""
    try:
        positions = get_trade_detail_data(accID, 'stock', 'position')
        if not positions:
            return []
        result = []
        for pos in positions:
            if not _state["pos_fields_printed"]:
                _state["pos_fields_printed"] = True
                attrs = [a for a in dir(pos) if a.startswith('m_')]
                print("[POS] fields: %s" % attrs)
                for a in attrs:
                    print("[POS]   %s = %s" % (a, getattr(pos, a, '?')))
            code = pos.m_strInstrumentID.split(".")[0]
            volume = int(getattr(pos, 'm_nVolume', 0) or pos.m_nCanUseVolume)
            result.append({"code": code, "volume": volume})
        return result
    except Exception as e:
        print("[POS ALL ERR] %s" % e)
    return []


def _refresh_broker_state():
    """Query broker for positions + cash, store in _state. Main thread only."""
    accID = _state["accID"]
    positions = _get_all_positions(accID)
    cash = _get_available_cash(accID)
    # Only update if broker returned data; don't overwrite with empty
    if positions or cash > 0:
        _state["positions"] = positions
        _state["cash"] = cash


# ── State sync thread (pure HTTP, no QMT API) ────────────────

def _state_sync_loop():
    """Every 30s: send heartbeat + current _state to server."""
    while True:
        body = {
            "positions": _state["positions"],
            "available_cash": _state["cash"],
        }
        try:
            _api_call("/heartbeat", "POST", body)
        except Exception as e:
            print("[STATE SYNC ERR] %s" % e)

        # One-time startup Feishu notification
        if not _state["startup_reported"]:
            _state["startup_reported"] = True
            cash = _state["cash"]
            if cash > 0:
                try:
                    _api_call("/report-status", "POST", {"available_cash": cash})
                    print("[STATUS] startup reported, cash=%.2f" % cash)
                except Exception as e:
                    print("[STATUS ERR] %s" % e)
            else:
                try:
                    _api_call("/report-trade", "POST", {
                        "message": "iQuant脚本已启动，当前券商柜台未连接（非交易时段或柜台尚未开启），余额暂时无法查询。柜台连通后将自动推送余额。",
                        "stock_code": "", "stock_name": "", "reason": "",
                    })
                    print("[STATUS] startup notified (broker not connected)")
                except Exception as e:
                    print("[STATUS NOTIFY ERR] %s" % e)

        time.sleep(30)


# ── Trade execution helpers ───────────────────────────────────

def _report_trade(msg, signal):
    """上报成交到服务端 (触发飞书通知)。"""
    try:
        _api_call("/report-trade", "POST", {
            "message": msg,
            "stock_code": signal.get("stock_code", ""),
            "stock_name": signal.get("stock_name", ""),
            "reason": signal.get("reason", ""),
        })
    except Exception as e:
        print("[REPORT TRADE ERR] %s" % e)


def _report_error(msg):
    """上报错误到服务端 (触发飞书通知)。"""
    try:
        _api_call("/report-error", "POST", {"error": msg})
    except Exception as e:
        print("[REPORT ERR] %s" % e)


def _execute_signal(ContextInfo, signal):
    """根据信号类型执行下单, 统一使用 passorder。"""
    code = _iquant_code(signal["stock_code"])
    sig_type = signal["type"]
    is_manual = signal.get("manual", False)
    accID = ContextInfo.accID

    if sig_type == "buy":
        if is_manual:
            quantity = signal.get("quantity", 100)
        else:
            cash = _get_available_cash(accID)
            price = signal.get("latest_price", 0)
            if cash <= 0 or price <= 0:
                msg = "V15买入失败: cash=%.2f price=%.2f" % (cash, price)
                print("[V15 BUY ERR] %s" % msg)
                _report_error(msg)
                return False
            quantity = int(cash * 0.95 / price / 100) * 100
            if quantity < 100:
                msg = "余额不足买1手: cash=%.2f price=%.2f 需要>=%.2f" % (cash, price, price * 100)
                print("[V15 BUY ERR] %s" % msg)
                _report_error(msg)
                return False

        price = signal.get("price", 0)
        price_type_str = signal.get("price_type", "market")

        if price_type_str == "limit" and price > 0:
            passorder(23, 11, accID, code, 11, price, quantity, "", 2, "", ContextInfo)
            msg = "买入 %s %d股 限价%.2f" % (code, quantity, price)
        else:
            passorder(23, 1101, accID, code, 5, -1, quantity, "", 2, "", ContextInfo)
            msg = "买入 %s %d股 市价" % (code, quantity)
        print("[BUY] %s" % msg)
        _report_trade(msg, signal)

    elif sig_type == "sell":
        if is_manual:
            quantity = signal.get("quantity", 100)
        else:
            quantity = _get_position_volume(accID, code)
            if quantity <= 0:
                print("[V15 SELL] %s no position, skip" % code)
                return False

        price = signal.get("price", 0)
        price_type_str = signal.get("price_type", "market")

        if price_type_str == "limit" and price > 0:
            passorder(24, 11, accID, code, 11, price, quantity, "", 2, "", ContextInfo)
            msg = "卖出 %s %d股 限价%.2f" % (code, quantity, price)
        else:
            passorder(24, 1101, accID, code, 5, -1, quantity, "", 2, "", ContextInfo)
            msg = "卖出 %s %d股 市价" % (code, quantity)
        print("[SELL] %s" % msg)
        _report_trade(msg, signal)

    return True


def _ack_signal(signal_id):
    """确认信号已执行。"""
    try:
        result = _api_call("/ack-signal", "POST", {"signal_id": signal_id})
        print("[ACK] signal_id=%s success=%s" % (signal_id, result.get("success")))
        return result.get("success", False)
    except Exception as e:
        print("[ACK ERR] signal_id=%s: %s" % (signal_id, e))
        return False


# ── QMT entry points ─────────────────────────────────────────

def init(ContextInfo):
    ContextInfo.set_account("410015160653")
    ContextInfo.accID = "410015160653"
    _state["accID"] = ContextInfo.accID
    print("[INIT] iQuant live trading script started")
    print("[INIT] API_BASE = %s" % API_BASE)

    # Query broker state on main thread before starting sync thread
    _refresh_broker_state()
    print("[INIT] positions=%d cash=%.2f" % (len(_state["positions"]), _state["cash"]))

    t = threading.Thread(target=_state_sync_loop, daemon=True)
    t.start()
    print("[INIT] state sync thread started")


def handlebar(ContextInfo):
    """Update broker state + poll signals and execute orders."""
    now = time.time()

    # 节流: 每30秒最多轮询一次
    if now - _state.get("last_poll_ts", 0) < 30:
        return
    _state["last_poll_ts"] = now

    # Update broker state (main thread — QMT APIs work here)
    _refresh_broker_state()

    try:
        timetag = ContextInfo.get_bar_timetag(ContextInfo.barpos)
        bar_time = timetag_to_datetime(timetag, "%H:%M")
    except Exception as e:
        print("[BAR ERR] %s" % e)
        return

    print("[BAR] time=%s barpos=%d" % (bar_time, ContextInfo.barpos))

    try:
        result = _api_call("/pending-signals")
    except Exception as e:
        print("[POLL ERR] %s: %s" % (bar_time, e))
        return

    signals = result.get("signals", [])
    if not signals:
        print("[POLL %s] no pending signals" % bar_time)
        return

    print("[POLL %s] found %d pending signal(s)" % (bar_time, len(signals)))

    for signal in signals:
        sig_id = signal.get("id", "?")
        try:
            success = _execute_signal(ContextInfo, signal)
            if success:
                _ack_signal(sig_id)
        except Exception as e:
            print("[EXEC ERR] signal %s: %s" % (sig_id, e))
