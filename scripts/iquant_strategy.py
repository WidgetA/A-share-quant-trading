# encoding: gbk
"""
iQuant strategy script — polling-based signal executor.

This script runs inside the iQuant (THS) environment on a Windows server.
It polls the remote strategy server for trading signals, then executes
them locally via passorder().

Architecture:
    Server (Alibaba Cloud)                     iQuant (Windows)
    ┌──────────────────────┐                   ┌──────────────────┐
    │ 09:30 push SELL      │  GET /pending-    │ handlebar() 每分钟│
    │ 09:40 push BUY       │──  signals  ──►   │  poll → execute  │
    │                      │                   │  → ack           │
    │ Signal queue (memory)│  POST /ack-signal │                  │
    │                      │◄──────────────────│ passorder()      │
    └──────────────────────┘                   └──────────────────┘

Lifecycle:
    init(ContextInfo)      -- called once at script load
    handlebar(ContextInfo) -- called every bar (1-min frequency)

Workflow:
    Every bar:
      1. Poll GET /pending-signals
      2. For each signal: passorder() (buy or sell)
      3. POST /ack-signal to confirm execution

Network:
    Uses urllib only (available in iQuant Python env).
    requests library may not be available.

Configuration:
    Edit API_BASE and API_KEY below before deploying.
"""

import json
import urllib.error
import urllib.request

# === CONFIGURATION (edit before deploying) ===
API_BASE = "http://8.159.150.224:8000/api/iquant"
API_KEY = "your-api-key-here"

# === INTERNAL STATE (persists across handlebar calls) ===
_state = {
    "server_ok": False,
    "last_poll_time": "",
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


def _get_time_str():
    """Get current time as HH:MM string."""
    from datetime import datetime

    return datetime.now().strftime("%H:%M")


# === ORDER EXECUTION ===


def _execute_buy(signal, ContextInfo):
    """Execute a BUY signal via passorder().

    Args:
        signal: dict with stock_code, stock_name, volume, latest_price
        ContextInfo: iQuant context object

    Returns:
        True if order placed, False otherwise
    """
    code = signal["stock_code"]
    volume = signal.get("volume", 0)
    price = signal.get("latest_price", 0)
    name = signal.get("stock_name", "")

    if volume < 100:
        print("[BUY] Volume < 100 for %s, skipping" % code)
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


def _execute_sell(signal, ContextInfo):
    """Execute a SELL signal via passorder().

    Args:
        signal: dict with stock_code, stock_name, volume
        ContextInfo: iQuant context object

    Returns:
        True if order placed, False otherwise
    """
    code = signal["stock_code"]
    volume = signal.get("volume", 0)
    name = signal.get("stock_name", "")

    if volume < 100:
        print("[SELL] Volume < 100 for %s, skipping" % code)
        return False

    try:
        account = ContextInfo.accID
        # opType: 24=sell stock
        # prType: 11=best 5 levels immediate (market price variant)
        passorder(24, 1101, account, code, 11, -1, volume, "", 1, "", ContextInfo)  # noqa: F821
        print("[SELL] ORDER PLACED: %s %s x%d" % (code, name, volume))
        return True
    except Exception as e:
        print("[SELL] ORDER FAILED for %s: %s" % (code, e))
        return False


def _ack_signal(signal_id):
    """Acknowledge a signal after execution.

    Tells the server the signal has been executed so it won't be
    returned again on next poll.
    """
    try:
        _api_call("/ack-signal", method="POST", body={"signal_id": signal_id})
    except Exception as e:
        # Ack failure is non-fatal — worst case, signal appears again next poll
        # and passorder() might place a duplicate. But that's better than
        # failing to ack and never clearing the signal.
        print("[WARN] Failed to ack signal %s: %s" % (signal_id, e))


# === IQUANT LIFECYCLE ===


def init(ContextInfo):
    """Called once when script is loaded in iQuant."""
    print("=" * 50)
    print("  iQuant Momentum Strategy (Polling Mode)")
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

    Simple loop:
      1. Poll /pending-signals for new signals
      2. Execute each signal via passorder()
      3. Ack each executed signal
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
            return  # still unreachable, skip this bar

    # Poll for pending signals
    try:
        result = _api_call("/pending-signals")
    except Exception as e:
        print("[%s] Poll failed: %s" % (time_str, e))
        return

    signals = result.get("signals", [])
    if not signals:
        return  # nothing to do

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
            # Ack unknown signals to clear them from queue
            executed = True

        if executed:
            _ack_signal(sig_id)
