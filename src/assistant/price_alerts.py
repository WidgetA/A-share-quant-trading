"""Persistent one-shot holding price alerts and their background monitor.

Kimi only creates/cancels rules through the bundled deterministic skill script.
This module owns the durable rule state, evaluates the broker's existing 30-second
position cache, and sends triggered alerts through the normal Feishu relay.

It never submits or changes an order.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
import time as wall_time
import uuid
from datetime import date, datetime, time, timedelta
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Awaitable, Callable
from zoneinfo import ZoneInfo

from src.common.config import PROJECT_ROOT

logger = logging.getLogger(__name__)

BEIJING_TZ = ZoneInfo("Asia/Shanghai")
DEFAULT_ALERT_DB = PROJECT_ROOT / "data" / "price_alerts.sqlite3"
DEFAULT_POLL_SECONDS = 5.0
DEFAULT_BROKER_MAX_AGE_SECONDS = 90.0

_ACTIVE = "active"
_TRIGGERED = "triggered"
_CANCELLED = "cancelled"
_NOTIFICATION_NONE = "none"
_NOTIFICATION_PENDING = "pending"
_NOTIFICATION_SENT = "sent"


def normalize_stock_code(value: str) -> str:
    """Return the bare six-digit part of a broker/Tushare stock code."""
    return (value or "").strip().upper().split(".", 1)[0]


def price_to_fen(value: object) -> int:
    """Convert a positive price to integer fen without binary-float drift."""
    try:
        number = Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("价格必须是大于 0 的数字") from exc
    if not number.is_finite() or number <= 0:
        raise ValueError("价格必须是大于 0 的数字")
    return int(number * 100)


def _now_iso(now: datetime | None = None) -> str:
    current = now or datetime.now(BEIJING_TZ)
    if current.tzinfo is None:
        current = current.replace(tzinfo=BEIJING_TZ)
    return current.astimezone(BEIJING_TZ).isoformat(timespec="seconds")


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    result = dict(row)
    result["threshold"] = result.pop("threshold_fen") / 100
    last_price_fen = result.pop("last_price_fen")
    result["last_price"] = last_price_fen / 100 if last_price_fen is not None else None
    return result


class PriceAlertStore:
    """Small SQLite store shared safely by the web process and Kimi helper."""

    def __init__(self, path: str | Path | None = None) -> None:
        configured = os.environ.get("PRICE_ALERT_DB_PATH", "").strip()
        self.path = Path(path or configured or DEFAULT_ALERT_DB)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=10.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 10000")
        return conn

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS price_alerts (
                    alert_id TEXT PRIMARY KEY,
                    stock_code TEXT NOT NULL,
                    stock_name TEXT NOT NULL,
                    direction TEXT NOT NULL CHECK (direction IN ('below', 'above')),
                    threshold_fen INTEGER NOT NULL CHECK (threshold_fen > 0),
                    status TEXT NOT NULL CHECK (status IN ('active', 'triggered', 'cancelled')),
                    created_at TEXT NOT NULL,
                    triggered_at TEXT,
                    cancelled_at TEXT,
                    last_price_fen INTEGER,
                    last_checked_at TEXT,
                    notification_status TEXT NOT NULL DEFAULT 'none'
                        CHECK (notification_status IN ('none', 'pending', 'sent')),
                    notification_attempts INTEGER NOT NULL DEFAULT 0,
                    notification_last_attempt_at TEXT,
                    notification_error TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_price_alerts_status
                    ON price_alerts(status, notification_status);

                CREATE UNIQUE INDEX IF NOT EXISTS uq_price_alerts_active_rule
                    ON price_alerts(stock_code, direction, threshold_fen)
                    WHERE status = 'active';
                """
            )

    def create_alert(
        self,
        *,
        stock_code: str,
        stock_name: str,
        direction: str,
        threshold: object,
        now: datetime | None = None,
    ) -> tuple[dict[str, Any], bool]:
        code = normalize_stock_code(stock_code)
        name = (stock_name or "").strip()
        if len(code) != 6 or not code.isdigit():
            raise ValueError("股票代码必须是 6 位数字")
        if not name:
            raise ValueError("股票名称不能为空")
        if direction not in ("below", "above"):
            raise ValueError("方向只支持 below 或 above")
        threshold_fen = price_to_fen(threshold)
        created_at = _now_iso(now)

        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                """
                SELECT * FROM price_alerts
                WHERE stock_code = ? AND direction = ? AND threshold_fen = ?
                  AND status = 'active'
                """,
                (code, direction, threshold_fen),
            ).fetchone()
            if existing is not None:
                conn.commit()
                return _row_to_dict(existing), False

            alert_id = uuid.uuid4().hex[:12]
            conn.execute(
                """
                INSERT INTO price_alerts (
                    alert_id, stock_code, stock_name, direction, threshold_fen,
                    status, created_at, notification_status
                ) VALUES (?, ?, ?, ?, ?, 'active', ?, 'none')
                """,
                (alert_id, code, name, direction, threshold_fen, created_at),
            )
            row = conn.execute(
                "SELECT * FROM price_alerts WHERE alert_id = ?", (alert_id,)
            ).fetchone()
            conn.commit()

        assert row is not None
        return _row_to_dict(row), True

    def list_alerts(self, *, status: str | None = None) -> list[dict[str, Any]]:
        sql = "SELECT * FROM price_alerts"
        params: tuple[object, ...] = ()
        if status is not None:
            if status not in (_ACTIVE, _TRIGGERED, _CANCELLED):
                raise ValueError("未知预警状态")
            sql += " WHERE status = ?"
            params = (status,)
        sql += " ORDER BY created_at DESC, alert_id DESC"
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [_row_to_dict(row) for row in rows]

    def cancel_alerts(
        self,
        *,
        alert_id: str | None = None,
        stock_code: str | None = None,
        now: datetime | None = None,
    ) -> int:
        if not alert_id and not stock_code:
            raise ValueError("取消预警必须指定预警编号或股票")
        clauses = ["status = 'active'"]
        params: list[object] = []
        if alert_id:
            clauses.append("alert_id = ?")
            params.append(alert_id.strip())
        if stock_code:
            clauses.append("stock_code = ?")
            params.append(normalize_stock_code(stock_code))
        params.append(_now_iso(now))
        sql = (
            "UPDATE price_alerts SET status = 'cancelled', cancelled_at = ? WHERE "
            + " AND ".join(clauses)
        )
        # cancelled_at is the first SQL placeholder.
        ordered_params = [params[-1], *params[:-1]]
        with self._connect() as conn:
            cursor = conn.execute(sql, ordered_params)
            conn.commit()
            return max(cursor.rowcount, 0)

    def claim_triggered(
        self,
        positions: list[dict[str, Any]],
        *,
        now: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """Update fresh prices and atomically claim active rules that now match."""
        prices: dict[str, int] = {}
        for position in positions:
            code = normalize_stock_code(str(position.get("code") or ""))
            if len(code) != 6:
                continue
            try:
                prices[code] = price_to_fen(position.get("last_price"))
            except ValueError:
                continue
        if not prices:
            return []

        checked_at = _now_iso(now)
        claimed_ids: list[str] = []
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            active_rows = conn.execute(
                "SELECT * FROM price_alerts WHERE status = 'active'"
            ).fetchall()
            for row in active_rows:
                price_fen = prices.get(row["stock_code"])
                if price_fen is None:
                    continue
                conn.execute(
                    """
                    UPDATE price_alerts
                    SET last_price_fen = ?, last_checked_at = ?
                    WHERE alert_id = ? AND status = 'active'
                    """,
                    (price_fen, checked_at, row["alert_id"]),
                )
                hit = (row["direction"] == "below" and price_fen <= row["threshold_fen"]) or (
                    row["direction"] == "above" and price_fen >= row["threshold_fen"]
                )
                if not hit:
                    continue
                cursor = conn.execute(
                    """
                    UPDATE price_alerts
                    SET status = 'triggered', triggered_at = ?,
                        notification_status = 'pending', notification_error = NULL
                    WHERE alert_id = ? AND status = 'active'
                    """,
                    (checked_at, row["alert_id"]),
                )
                if cursor.rowcount == 1:
                    claimed_ids.append(row["alert_id"])
            conn.commit()

        if not claimed_ids:
            return []
        placeholders = ",".join("?" for _ in claimed_ids)
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM price_alerts WHERE alert_id IN ({placeholders})",
                claimed_ids,
            ).fetchall()
        return [_row_to_dict(row) for row in rows]

    def pending_notifications(self, *, now: datetime | None = None) -> list[dict[str, Any]]:
        current = now or datetime.now(BEIJING_TZ)
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM price_alerts
                WHERE status = 'triggered' AND notification_status = 'pending'
                ORDER BY triggered_at, alert_id
                """
            ).fetchall()

        due: list[dict[str, Any]] = []
        for row in rows:
            last_attempt = row["notification_last_attempt_at"]
            attempts = int(row["notification_attempts"] or 0)
            if not last_attempt:
                due.append(_row_to_dict(row))
                continue
            try:
                last_dt = datetime.fromisoformat(last_attempt)
                if last_dt.tzinfo is None:
                    last_dt = last_dt.replace(tzinfo=BEIJING_TZ)
            except ValueError:
                due.append(_row_to_dict(row))
                continue
            exponent = min(max(attempts - 1, 0), 4)
            retry_after = timedelta(seconds=min(60 * (2**exponent), 900))
            if current.astimezone(BEIJING_TZ) >= last_dt.astimezone(BEIJING_TZ) + retry_after:
                due.append(_row_to_dict(row))
        return due

    def record_notification(
        self,
        alert_id: str,
        *,
        sent: bool,
        error: str | None = None,
        now: datetime | None = None,
    ) -> None:
        attempted_at = _now_iso(now)
        status = _NOTIFICATION_SENT if sent else _NOTIFICATION_PENDING
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE price_alerts
                SET notification_status = ?,
                    notification_attempts = notification_attempts + 1,
                    notification_last_attempt_at = ?, notification_error = ?
                WHERE alert_id = ? AND status = 'triggered'
                """,
                (status, attempted_at, None if sent else (error or "发送失败")[:500], alert_id),
            )
            conn.commit()


def format_trigger_message(alert: dict[str, Any]) -> str:
    direction_text = "跌破" if alert["direction"] == "below" else "突破"
    last_price = alert.get("last_price")
    current_text = f"{last_price:.2f} 元" if isinstance(last_price, (int, float)) else "未知"
    return (
        "🚨 持仓价格预警\n\n"
        f"{alert['stock_code']} {alert['stock_name']} 已{direction_text} "
        f"{alert['threshold']:.2f} 元\n"
        f"当前价: {current_text}\n"
        f"触发时间: {alert.get('triggered_at') or '-'}\n"
        f"预警编号: {alert['alert_id']}\n\n"
        "这是一次性预警，触发后已自动停止；未执行任何交易。\n"
        "数据源: 券商持仓行情缓存（约 30 秒刷新）"
    )


async def _default_send_notification(message: str) -> bool:
    from src.common.feishu_bot import FeishuBot

    bot = FeishuBot()
    if not bot.is_configured():
        return False
    return await bot.send_message(message, max_retries=2)


async def _default_trading_day_checker(target: date) -> bool:
    """Use the exchange calendar when available; weekday is a logged fallback."""
    try:
        from src.data.clients.tushare_realtime import get_tushare_trade_calendar

        value = target.strftime("%Y-%m-%d")
        days = await get_tushare_trade_calendar(value, value)
        return value in set(days or [])
    except Exception as exc:
        logger.warning(
            "Price alert trade-calendar lookup failed for %s: %s; using weekday fallback",
            target,
            exc,
        )
        return target.weekday() < 5


class PriceAlertMonitor:
    """Evaluate active alerts against fresh broker position snapshots."""

    def __init__(
        self,
        app_state: Any,
        *,
        store: PriceAlertStore | None = None,
        poll_seconds: float = DEFAULT_POLL_SECONDS,
        broker_max_age_seconds: float = DEFAULT_BROKER_MAX_AGE_SECONDS,
        send_notification: Callable[[str], Awaitable[bool]] | None = None,
        trading_day_checker: Callable[[date], Awaitable[bool]] | None = None,
    ) -> None:
        self._app_state = app_state
        self.store = store or PriceAlertStore()
        self.poll_seconds = poll_seconds
        self.broker_max_age_seconds = broker_max_age_seconds
        self._send_notification = send_notification or _default_send_notification
        self._trading_day_checker = trading_day_checker or _default_trading_day_checker
        self._last_snapshot_timestamp: float | None = None
        self._calendar_date: date | None = None
        self._calendar_is_open = False

    @staticmethod
    def _is_market_time(now: datetime) -> bool:
        local = now.astimezone(BEIJING_TZ)
        current = local.time().replace(tzinfo=None)
        return (time(9, 30) <= current < time(11, 30)) or (time(13, 0) <= current < time(15, 0))

    async def _is_open_day(self, target: date) -> bool:
        if target != self._calendar_date:
            self._calendar_is_open = await self._trading_day_checker(target)
            self._calendar_date = target
        return self._calendar_is_open

    async def _send_pending(self, now: datetime) -> int:
        sent_count = 0
        for alert in self.store.pending_notifications(now=now):
            message = format_trigger_message(alert)
            try:
                sent = await self._send_notification(message)
                error = None if sent else "飞书机器人未配置或发送接口返回失败"
            except Exception as exc:  # pragma: no cover - defensive integration boundary
                sent = False
                error = f"{type(exc).__name__}: {exc}"
                logger.warning("Price alert Feishu send failed", exc_info=True)
            self.store.record_notification(alert["alert_id"], sent=sent, error=error, now=now)
            if sent:
                sent_count += 1
                logger.info(
                    "Price alert notified: %s %s @ %.2f",
                    alert["stock_code"],
                    alert["direction"],
                    alert["threshold"],
                )
        return sent_count

    async def check_once(self, *, now: datetime | None = None) -> dict[str, int | str]:
        current = (now or datetime.now(BEIJING_TZ)).astimezone(BEIJING_TZ)
        notified = await self._send_pending(current)

        if not self._is_market_time(current):
            return {"result": "outside_market", "triggered": 0, "notified": notified}
        if not await self._is_open_day(current.date()):
            return {"result": "non_trading_day", "triggered": 0, "notified": notified}

        updated_at = getattr(self._app_state, "broker_positions_updated_at", None)
        if not isinstance(updated_at, (int, float)):
            return {"result": "no_broker_snapshot", "triggered": 0, "notified": notified}
        if wall_time.time() - float(updated_at) > self.broker_max_age_seconds:
            return {"result": "stale_broker_snapshot", "triggered": 0, "notified": notified}
        if self._last_snapshot_timestamp == float(updated_at):
            return {"result": "unchanged_snapshot", "triggered": 0, "notified": notified}

        self._last_snapshot_timestamp = float(updated_at)
        positions = list(getattr(self._app_state, "broker_positions", []) or [])
        triggered = self.store.claim_triggered(positions, now=current)
        notified += await self._send_pending(current)
        return {"result": "checked", "triggered": len(triggered), "notified": notified}

    async def run(self) -> None:
        logger.info("Holding price alert monitor started")
        try:
            while True:
                try:
                    await self.check_once()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Holding price alert monitor check failed")
                await asyncio.sleep(self.poll_seconds)
        except asyncio.CancelledError:
            logger.info("Holding price alert monitor cancelled")
            raise
