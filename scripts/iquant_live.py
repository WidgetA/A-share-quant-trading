#encoding:gbk
"""
iQuant 实盘下单脚本 — 轮询服务端 /pending-signals, 用 passorder() 执行。

架构 (v2):
  - init 主线程循环 (5s): 轮询信号 + passorder下单 + 刷新持仓余额
  - 状态线程 (30s): 心跳 + 把 _state 中的持仓/余额发到服务端 (纯 HTTP, 不调 QMT API)
  - handlebar: 空壳 (QMT 框架要求存在)

QMT API (get_trade_detail_data / passorder) 只能在主线程调用。
init 不 return，主线程控制权不交回 QMT。
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
    # Heartbeat thread liveness tracking
    "sync_thread": None,        # threading.Thread reference
    "last_sync_attempt": 0,     # time.time() of last heartbeat attempt
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


def _sync_state_now():
    """Immediately push current _state to server (e.g. after a trade)."""
    body = {
        "positions": _state["positions"],
        "available_cash": _state["cash"],
    }
    try:
        _api_call("/heartbeat", "POST", body)
    except Exception as e:
        print("[SYNC NOW ERR] %s" % e)


# ── State sync thread (pure HTTP, no QMT API) ────────────────

def _state_sync_loop():
    """Every 30s: send heartbeat + current _state to server.

    Wrapped in outer try/except to survive ANY exception — if the inner
    loop somehow breaks, we log and restart after a short delay.
    """
    while True:
        try:
            _state["last_sync_attempt"] = time.time()
            body = {
                "positions": _state["positions"],
                "available_cash": _state["cash"],
            }
            try:
                _api_call("/heartbeat", "POST", body)
                # Log success every ~5 min (every 10th beat) so user can verify in QMT console
                beat_count = _state.get("beat_count", 0) + 1
                _state["beat_count"] = beat_count
                if beat_count % 10 == 1:
                    print("[HEARTBEAT] ok #%d pos=%d cash=%.0f" % (
                        beat_count, len(_state["positions"]), _state["cash"]))
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
        except Exception as e:
            # Catch-all: don't let the thread die
            print("[SYNC THREAD CRASH] %s — retrying in 10s" % e)
            time.sleep(10)


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
                msg = "买入失败: cash=%.2f price=%.2f" % (cash, price)
                print("[BUY ERR] %s" % msg)
                _report_error(msg)
                return False
            quantity = int(cash * 0.95 / price / 100) * 100
            if quantity < 100:
                msg = "余额不足买1手: cash=%.2f price=%.2f 需要>=%.2f" % (cash, price, price * 100)
                print("[BUY ERR] %s" % msg)
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
                print("[SELL] %s no position, skip" % code)
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

def _start_sync_thread():
    """Start (or restart) the state sync thread. Returns the thread."""
    t = threading.Thread(target=_state_sync_loop, daemon=True)
    t.start()
    _state["sync_thread"] = t
    _state["last_sync_attempt"] = time.time()
    return t


POLL_INTERVAL = 5   # seconds between signal polls
BROKER_REFRESH_INTERVAL = 30  # seconds between broker state refreshes


def init(ContextInfo):
    ContextInfo.set_account("410015160653")
    ContextInfo.accID = "410015160653"
    _state["accID"] = ContextInfo.accID
    _state["ContextInfo"] = ContextInfo
    print("[INIT] iQuant live trading script started (v2 — main-thread loop)")
    print("[INIT] API_BASE = %s" % API_BASE)
    print("[INIT] poll_interval = %ds" % POLL_INTERVAL)

    # Query broker state on main thread before starting sync thread
    _refresh_broker_state()
    print("[INIT] positions=%d cash=%.2f" % (len(_state["positions"]), _state["cash"]))

    _start_sync_thread()
    print("[INIT] state sync thread started")

    # Main-thread loop: poll signals + execute + refresh broker state
    print("[INIT] entering main-thread polling loop...")
    _main_loop(ContextInfo)


def _main_loop(ContextInfo):
    """Main-thread loop: poll signals every POLL_INTERVAL, refresh broker state periodically."""
    last_broker_refresh = time.time()
    poll_count = 0

    while True:
        try:
            now = time.time()

            # Watchdog: restart sync thread if dead or stuck
            sync_thread = _state.get("sync_thread")
            last_attempt = _state.get("last_sync_attempt", 0)
            if sync_thread is None or not sync_thread.is_alive() or (now - last_attempt > 120):
                reason = "dead" if (sync_thread and not sync_thread.is_alive()) else "stuck/missing"
                print("[WATCHDOG] sync thread %s — restarting" % reason)
                _start_sync_thread()

            # Refresh broker state every BROKER_REFRESH_INTERVAL
            if now - last_broker_refresh >= BROKER_REFRESH_INTERVAL:
                _refresh_broker_state()
                last_broker_refresh = now

            # Poll for pending signals
            try:
                result = _api_call("/pending-signals")
            except Exception as e:
                print("[POLL ERR] %s" % e)
                time.sleep(POLL_INTERVAL)
                continue

            signals = result.get("signals", [])
            poll_count += 1

            # Log every 60th poll (~5min) to avoid spam
            if poll_count % 60 == 1:
                print("[POLL #%d] signals=%d pos=%d cash=%.0f" % (
                    poll_count, len(signals), len(_state["positions"]), _state["cash"]))

            if signals:
                print("[POLL] found %d pending signal(s)" % len(signals))
                for signal in signals:
                    sig_id = signal.get("id", "?")
                    try:
                        success = _execute_signal(ContextInfo, signal)
                        if success:
                            _ack_signal(sig_id)
                            _refresh_broker_state()
                            last_broker_refresh = time.time()
                            _sync_state_now()
                    except Exception as e:
                        print("[EXEC ERR] signal %s: %s" % (sig_id, e))

            time.sleep(POLL_INTERVAL)

        except Exception as e:
            print("[MAIN LOOP ERR] %s — continuing in %ds" % (e, POLL_INTERVAL))
            time.sleep(POLL_INTERVAL)


