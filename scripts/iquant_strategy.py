# encoding: gbk
"""
iQuant strategy script — live polling + backtest support.

This script runs inside the iQuant (THS) environment on a Windows server.
It supports two modes:

  Live mode:  Polls the remote server for trading signals (buy/sell),
              executes them locally via passorder().
  Backtest:   Detects historical bar replay via ContextInfo.get_bar_timetag(),
              calls the server's /backtest-scan endpoint for each day's
              recommendation, and simulates T+1 buy/sell via passorder().

Architecture (Live):
    Server (Alibaba Cloud)                     iQuant (Windows)
    +----------------------+                   +------------------+
    | 09:30 push SELL      |  GET /pending-    | handlebar() 1min |
    | 09:40 push BUY       |--  signals  --->  |  poll -> execute |
    |                      |                   |  -> ack          |
    | Signal queue (memory)|  POST /ack-signal |                  |
    |                      |<------------------|  passorder()     |
    +----------------------+                   +------------------+

Architecture (Backtest):
    Server                                     iQuant (Backtest)
    +----------------------+                   +------------------+
    | POST /backtest-scan  |<--- date ---------|  09:40 bar:      |
    |  -> MomentumScanner  |                   |   call server    |
    |  -> recommendation   |--- rec ---------->|   buy rec stock  |
    +----------------------+                   |  09:30 bar:      |
                                               |   sell T+1       |
                                               +------------------+

Lifecycle:
    init(ContextInfo)      -- called once at script load
    handlebar(ContextInfo) -- called every bar (1-min frequency)

Configuration:
    Edit API_BASE, API_KEY, BUY_AMOUNT below before deploying.
"""

import json
import urllib.error
import urllib.request

# === CONFIGURATION (edit before deploying) ===
API_BASE = "http://8.159.150.224:8000/api/iquant"
API_KEY = "your-api-key-here"
BUY_AMOUNT = 50000  # yuan per position

# Backtest-specific config
BT_DATA_SOURCE = "akshare"  # "akshare" (recommended) or "ifind"

# === INTERNAL STATE (persists across handlebar calls) ===
_state = {
    "server_ok": False,
    "last_poll_time": "",
    "mode": None,  # "live" or "backtest", detected on first handlebar
}

# Backtest state (separate from live)
_bt_state = {
    "holding": None,  # {code, name, buy_date} or None
    "scanned_dates": {},  # date_str -> True (avoid re-scan)
}


# === API HELPER ===


def _api_call(endpoint, method="GET", body=None):
    """Make API call to the strategy server.

    Args:
        endpoint: e.g., "/ping", "/pending-signals"
        method: "GET" or "POST"
        body: dict for POST body (JSON-encoded)

    Returns:
        dict: parsed JSON response

    Raises:
        Exception: on network error or non-200 status (fail-fast)
    """
    url = API_BASE + endpoint
    headers = {
        "X-API-Key": API_KEY,
        "Content-Type": "application/json",
    }
    if body:
        data = json.dumps(body).encode("utf-8")
    else:
        data = None

    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        resp = urllib.request.urlopen(req, timeout=30)
        return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body_text = e.read().decode("utf-8", errors="replace")[:200]
        raise Exception("HTTP %d: %s" % (e.code, body_text))
    except urllib.error.URLError as e:
        raise Exception("URL error: %s" % e.reason)


# === BAR TIME HELPERS ===


def _get_bar_datetime(ContextInfo):
    """Extract bar date and time from QMT ContextInfo.

    In backtest mode, QMT replays historical bars. The bar's actual
    date/time comes from get_bar_timetag(), not datetime.now().

    Returns:
        (date_str, time_str): e.g., ("2026-02-20", "09:40")
        Returns (None, None) if bar time cannot be determined.
    """
    from datetime import datetime

    try:
        timetag = ContextInfo.get_bar_timetag(ContextInfo.barpos)
        # timetag is milliseconds since epoch
        bar_dt = datetime.fromtimestamp(timetag / 1000)
        return (bar_dt.strftime("%Y-%m-%d"), bar_dt.strftime("%H:%M"))
    except Exception:
        return (None, None)


def _detect_mode(ContextInfo):
    """Detect live vs backtest mode.

    If the bar date differs from today's real date, QMT is replaying
    historical bars (backtest mode).
    """
    from datetime import datetime

    bar_date, _ = _get_bar_datetime(ContextInfo)
    if bar_date is None:
        return "live"  # can't determine bar time -> assume live
    today = datetime.now().strftime("%Y-%m-%d")
    return "backtest" if bar_date != today else "live"


def _get_time_str():
    """Get current real time as HH:MM string (for live mode)."""
    from datetime import datetime

    return datetime.now().strftime("%H:%M")


# === ORDER EXECUTION (shared by live and backtest) ===


def _execute_buy(signal, ContextInfo):
    """Execute a BUY signal via passorder().

    Volume is calculated locally from BUY_AMOUNT and the signal's
    latest_price. Server only provides stock_code + direction.

    Returns:
        True if order placed, False otherwise
    """
    code = signal["stock_code"]
    price = signal.get("latest_price", 0)
    name = signal.get("stock_name", "")

    if price <= 0:
        print("[BUY] Invalid price %.2f for %s, skipping" % (price, code))
        return False

    # Calculate volume: round down to nearest 100 shares (1 lot)
    volume = int(BUY_AMOUNT / price / 100) * 100
    if volume < 100:
        print(
            "[BUY] Price %.2f too high for BUY_AMOUNT=%d, skipping %s" % (price, BUY_AMOUNT, code)
        )
        return False

    try:
        account = ContextInfo.accID
        # passorder(opType, orderType, accountid, orderCode, prType, price, volume, ...)
        # opType: 23=buy stock
        # orderType: 1101=ordinary
        # prType: 11=best 5 levels immediate (market price variant)
        passorder(23, 1101, account, code, 11, -1, volume, "", 1, "", ContextInfo)  # noqa: F821
        print(
            "[BUY] ORDER PLACED: %s %s x%d (est ~%.0f yuan)" % (code, name, volume, price * volume)
        )
        return True
    except Exception as e:
        print("[BUY] ORDER FAILED for %s: %s" % (code, e))
        return False


def _get_position_volume(code, ContextInfo):
    """Query actual sellable volume for a stock from the broker.

    Uses iQuant get_trade_detail_data() to read real position.

    Returns:
        int: sellable volume, or 0 if no position found
    """
    try:
        account = ContextInfo.accID
        positions = get_trade_detail_data(account, "POSITION", "STOCK")  # noqa: F821
        for pos in positions:
            if hasattr(pos, "m_strInstrumentID") and pos.m_strInstrumentID == code:
                return int(pos.m_nCanUseVolume)
    except Exception as e:
        print("[SELL] Failed to query position for %s: %s" % (code, e))
    return 0


def _execute_sell(signal, ContextInfo):
    """Execute a SELL signal via passorder().

    Queries actual sellable volume from broker.

    Returns:
        True if order placed, False otherwise
    """
    code = signal["stock_code"]
    name = signal.get("stock_name", "")

    volume = _get_position_volume(code, ContextInfo)
    if volume < 100:
        print("[SELL] No sellable position for %s (volume=%d), skipping" % (code, volume))
        return False

    try:
        account = ContextInfo.accID
        # opType: 24=sell stock
        passorder(24, 1101, account, code, 11, -1, volume, "", 1, "", ContextInfo)  # noqa: F821
        print("[SELL] ORDER PLACED: %s %s x%d" % (code, name, volume))
        return True
    except Exception as e:
        print("[SELL] ORDER FAILED for %s: %s" % (code, e))
        return False


def _ack_signal(signal_id):
    """Acknowledge a signal after execution (live mode only)."""
    try:
        _api_call("/ack-signal", method="POST", body={"signal_id": signal_id})
    except Exception as e:
        print("[WARN] Failed to ack signal %s: %s" % (signal_id, e))


# === LIVE MODE ===


def _handlebar_live(ContextInfo):
    """Live mode: poll server for signals, execute via passorder().

    Same logic as original handlebar().
    """
    time_str = _get_time_str()

    # Only active during trading hours (09:25 ~ 15:05)
    if time_str < "09:25" or time_str > "15:05":
        return

    # If server was unreachable at init, retry ping
    if not _state["server_ok"]:
        try:
            _api_call("/ping")
            _state["server_ok"] = True
            print("[%s] Server reconnected" % time_str)
        except Exception:
            return

    # Poll for pending signals
    try:
        result = _api_call("/pending-signals")
    except Exception as e:
        print("[%s] Poll failed: %s" % (time_str, e))
        return

    signals = result.get("signals", [])
    if not signals:
        return

    print("[%s] Received %d signal(s)" % (time_str, len(signals)))

    for signal in signals:
        sig_type = signal.get("type", "")
        sig_id = signal.get("id", "")
        code = signal.get("stock_code", "")

        print(
            "[%s] Processing: %s %s %s (id=%s)"
            % (time_str, sig_type.upper(), code, signal.get("stock_name", ""), sig_id)
        )

        executed = False
        if sig_type == "buy":
            executed = _execute_buy(signal, ContextInfo)
        elif sig_type == "sell":
            executed = _execute_sell(signal, ContextInfo)
        else:
            print("[%s] Unknown signal type: %s" % (time_str, sig_type))
            executed = True

        if executed:
            _ack_signal(sig_id)


# === BACKTEST MODE ===


def _handlebar_backtest(ContextInfo):
    """Backtest mode: call server for each day's recommendation, simulate T+1.

    REQUIRES 1-minute bar frequency. Daily bars are ignored to prevent
    incorrect simulation (no intraday price info → wrong fill prices).

    Trading pattern:
      - 09:25~09:35 bar: SELL yesterday's holding (T+1 at open)
      - 09:36~09:50 bar: BUY today's recommendation from server scan
    """
    bar_date, bar_time = _get_bar_datetime(ContextInfo)

    if bar_date is None or bar_time is None:
        return

    # Reject non-minute bars (daily bar_time="00:00" has no intraday info)
    if bar_time == "00:00":
        _bt_state.setdefault("_daily_warned", False)
        if not _bt_state["_daily_warned"]:
            print("[BT ERROR] Daily bars detected — switch to 1-minute frequency!")
            _bt_state["_daily_warned"] = True
        return

    # === SELL window: 09:25~09:35 (T+1 at open) ===
    if "09:25" <= bar_time <= "09:35" and _bt_state["holding"] is not None:
        holding = _bt_state["holding"]
        if holding["buy_date"] != bar_date:
            code = holding["code"]
            name = holding.get("name", "")
            volume = _get_position_volume(code, ContextInfo)
            if volume >= 100:
                try:
                    account = ContextInfo.accID
                    passorder(24, 1101, account, code, 11, -1, volume, "", 1, "", ContextInfo)  # noqa: F821
                    print("[BT %s] SELL %s %s x%d" % (bar_date, code, name, volume))
                except Exception as e:
                    print("[BT %s] SELL FAILED %s: %s" % (bar_date, code, e))
            else:
                print("[BT %s] No position for %s (vol=%d)" % (bar_date, code, volume))
            _bt_state["holding"] = None

    # === BUY window: 09:36~09:50 (scan for today's recommendation) ===
    if "09:36" <= bar_time <= "09:50" and bar_date not in _bt_state["scanned_dates"]:
        _bt_state["scanned_dates"][bar_date] = True

        if _bt_state["holding"] is not None:
            print(
                "[BT %s] Already holding %s, skip scan"
                % (bar_date, _bt_state["holding"]["code"])
            )
            return

        try:
            result = _api_call(
                "/backtest-scan",
                method="POST",
                body={"trade_date": bar_date, "data_source": BT_DATA_SOURCE},
            )
        except Exception as e:
            print("[BT %s] Scan failed: %s" % (bar_date, e))
            return

        rec = result.get("recommendation")
        if not rec:
            reason = result.get("reason", "no recommendation")
            print("[BT %s] No recommendation: %s" % (bar_date, reason))
            return

        code = rec["stock_code"]
        name = rec.get("stock_name", "")
        price = rec.get("latest_price", 0)
        score = rec.get("composite_score", 0)

        print(
            "[BT %s] BUY %s %s @ %.2f (score=%.4f)"
            % (bar_date, code, name, price, score)
        )

        if _execute_buy(rec, ContextInfo):
            _bt_state["holding"] = {
                "code": code,
                "name": name,
                "buy_date": bar_date,
            }


# === IQUANT LIFECYCLE ===


def init(ContextInfo):
    """Called once when script is loaded in iQuant."""
    print("=" * 50)
    print("  iQuant Momentum Strategy")
    print("  Server: %s" % API_BASE)
    print("=" * 50)

    # Verify server connectivity (also triggers lazy resource init)
    try:
        result = _api_call("/ping")
        _state["server_ok"] = True
        print(
            "[OK] Server connected: %s (time=%s)"
            % (result.get("status"), result.get("server_time"))
        )
        print(
            "[OK] Pending signals: %d, Holdings: %d"
            % (result.get("pending_count", 0), result.get("holdings_count", 0))
        )
    except Exception as e:
        print("[ERROR] Cannot reach server: %s" % e)
        print("[ERROR] Will retry on first handlebar call.")


def handlebar(ContextInfo):
    """Called every bar (1-minute frequency).

    Dispatches to live mode or backtest mode based on bar timetag.
    """
    # Detect mode on first call
    if _state["mode"] is None:
        _state["mode"] = _detect_mode(ContextInfo)
        print("[MODE] Detected: %s" % _state["mode"])

    if _state["mode"] == "backtest":
        _handlebar_backtest(ContextInfo)
    else:
        _handlebar_live(ContextInfo)
