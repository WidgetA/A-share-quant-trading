#encoding:gbk
"""
iQuant 实盘下单脚本 — 轮询服务端 /pending-signals, 用 passorder() 执行。

在 iQuant (QMT) 1 分钟 K 线实盘模式下运行。
流程:
  1. 每根 bar 调用 /pending-signals 获取待执行信号
  2. 按信号调用 passorder() 下单
  3. 下单后调用 /ack-signal 确认

下单方式:
  - manual=True + price_type="market" → passorder 1101 (最优五档即时成交剩余撤销)
  - manual=True + price_type="limit"  → passorder 1101, 指定价
  - V15 买入 (无 quantity) → order_percent(0.95) 满仓
  - V15 卖出 (无 quantity) → order_target_percent(0) 清仓
"""
import json
import time
import urllib.request

API_BASE = "http://8.159.150.224:8000/api/iquant"
API_KEY = "20f4b05bed1fe28e3c808dabaa5fa37f"

_state = {"last_poll": ""}


def _api_call(endpoint, method="GET", body=None):
    url = API_BASE + endpoint
    headers = {"X-API-Key": API_KEY, "Content-Type": "application/json"}
    data = json.dumps(body).encode("utf-8") if body else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    resp = urllib.request.urlopen(req, timeout=30)
    return json.loads(resp.read().decode("utf-8"))


def _iquant_code(code):
    """转为 iQuant 格式: 601398 -> 601398.SH"""
    if "." in code:
        return code
    return code + (".SH" if code[0] in "65" else ".SZ")


def _execute_signal(ContextInfo, signal):
    """根据信号类型执行下单。"""
    code = _iquant_code(signal["stock_code"])
    sig_type = signal["type"]
    is_manual = signal.get("manual", False)

    if is_manual:
        quantity = signal.get("quantity", 100)
        price = signal.get("price", 0)
        price_type_str = signal.get("price_type", "market")

        if sig_type == "buy":
            # 23=买入, 1101=最优五档即时成交剩余撤销
            if price_type_str == "limit" and price > 0:
                # 限价单: 11=指定价
                passorder(23, 11, ContextInfo.accID, code, 11, price, quantity, "", 2, "", ContextInfo)
                print("[MANUAL BUY] %s qty=%d price=%.2f (limit)" % (code, quantity, price))
            else:
                # 市价单: 最优五档即时成交剩余撤销
                passorder(23, 1101, ContextInfo.accID, code, 5, -1, quantity, "", 2, "", ContextInfo)
                print("[MANUAL BUY] %s qty=%d (market)" % (code, quantity))

        elif sig_type == "sell":
            if price_type_str == "limit" and price > 0:
                passorder(24, 11, ContextInfo.accID, code, 11, price, quantity, "", 2, "", ContextInfo)
                print("[MANUAL SELL] %s qty=%d price=%.2f (limit)" % (code, quantity, price))
            else:
                passorder(24, 1101, ContextInfo.accID, code, 5, -1, quantity, "", 2, "", ContextInfo)
                print("[MANUAL SELL] %s qty=%d (market)" % (code, quantity))

    else:
        # V15 策略信号 (无 quantity, 按仓位比例)
        if sig_type == "buy":
            order_percent(code, 0.95, ContextInfo, ContextInfo.accID)
            print("[V15 BUY] %s 95%%" % code)
        elif sig_type == "sell":
            order_target_percent(code, 0, "ANY", -1, ContextInfo, ContextInfo.accID)
            print("[V15 SELL] %s clear" % code)

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


def init(ContextInfo):
    ContextInfo.set_account("test")  # TODO: 替换为实盘账户ID
    ContextInfo.accID = "test"       # TODO: 替换为实盘账户ID
    print("[INIT] iQuant live trading script started")
    print("[INIT] API_BASE = %s" % API_BASE)


def handlebar(ContextInfo):
    timetag = ContextInfo.get_bar_timetag(ContextInfo.barpos)
    bar_time = timetag_to_datetime(timetag, "%H:%M")

    # 只在交易时间轮询 (09:25-15:00)
    if bar_time < "09:25" or bar_time > "15:00":
        return

    try:
        result = _api_call("/pending-signals")
    except Exception as e:
        print("[POLL ERR] %s: %s" % (bar_time, e))
        return

    signals = result.get("signals", [])
    if not signals:
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
