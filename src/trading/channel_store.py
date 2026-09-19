"""Durable channel configuration and request ownership for the Web broker switch."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from src.trading.broker_client import BrokerError


def canonical(value) -> str:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    )


class ChannelStore:
    def __init__(self, path: Path):
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        # Exclusive creation sets restrictive permissions before any credentials are written.
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            os.close(fd)
        except FileExistsError:
            pass
        with self.db() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS profiles (id TEXT PRIMARY KEY, spec TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS preferences (name TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS requests (
                    id TEXT PRIMARY KEY, route TEXT NOT NULL, kind TEXT NOT NULL,
                    intent TEXT NOT NULL, result TEXT, created REAL NOT NULL DEFAULT (unixepoch()),
                    original_error TEXT
                );
            """)
            columns = {row[1] for row in db.execute("PRAGMA table_info(requests)")}
            if "original_error" not in columns:
                db.execute("ALTER TABLE requests ADD COLUMN original_error TEXT")

    @contextmanager
    def db(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    def preference(self, name: str, default=None):
        with self.db() as db:
            row = db.execute("SELECT value FROM preferences WHERE name=?", (name,)).fetchone()
        return row[0] if row else default

    @property
    def backend(self) -> str:
        return self.preference("backend", "miniqmt")

    def save_profile(self, spec: dict) -> str:
        raw = canonical(spec)
        route = spec["backend"] + "-" + hashlib.sha256(raw.encode()).hexdigest()[:24]
        with self.db() as db:
            db.execute("INSERT OR IGNORE INTO profiles VALUES (?, ?)", (route, raw))
            db.execute(
                "INSERT OR REPLACE INTO preferences VALUES (?, ?)",
                (spec["backend"], route),
            )
        return route

    def select(self, backend: str):
        if backend not in ("miniqmt", "qmt") or not self.preference(backend):
            raise BrokerError("NOT_CONFIGURED", "请先配置所选交易通道")
        with self.db() as db:
            db.execute("INSERT OR REPLACE INTO preferences VALUES ('backend', ?)", (backend,))
            db.execute(
                "INSERT OR REPLACE INTO preferences VALUES ('active_route', ?)",
                (self.preference(backend),),
            )

    def profile(self, route: str) -> dict:
        with self.db() as db:
            row = db.execute("SELECT spec FROM profiles WHERE id=?", (route,)).fetchone()
        if row is None:
            raise BrokerError("UNKNOWN_CHANNEL", "找不到订单所属通道的配置")
        return json.loads(row[0])

    def reserve(self, key: str, route: str, kind: str, intent: dict) -> tuple[dict, bool]:
        raw = canonical(intent)
        with self.db() as db:
            cur = db.execute(
                "INSERT OR IGNORE INTO requests(id,route,kind,intent) VALUES (?,?,?,?)",
                (key, route, kind, raw),
            )
            first = cur.rowcount == 1
            row = dict(db.execute("SELECT * FROM requests WHERE id=?", (key,)).fetchone())
            if row["kind"] != kind or row["intent"] != raw:
                raise BrokerError("ID_CONFLICT", "同一请求编号的交易内容不同，请核对原请求")
        row["intent"] = json.loads(row["intent"])
        row["result"] = json.loads(row["result"]) if row["result"] else None
        return row, first

    def complete(self, key: str, result: dict):
        with self.db() as db:
            db.execute("UPDATE requests SET result=? WHERE id=?", (canonical(result), key))

    def record_error(self, key: str, error: str):
        with self.db() as db:
            db.execute(
                "UPDATE requests SET original_error=COALESCE(original_error, ?) WHERE id=?",
                (error, key),
            )

    def orders(self, route: str) -> list[dict]:
        with self.db() as db:
            rows = db.execute(
                "SELECT * FROM requests WHERE route=? AND kind='order' ORDER BY created DESC",
                (route,),
            ).fetchall()
        return [
            {
                **dict(row),
                "intent": json.loads(row["intent"]),
                "result": json.loads(row["result"]) if row["result"] else None,
            }
            for row in rows
        ]


def default_store() -> ChannelStore:
    from src.common import config

    return ChannelStore(config.PROJECT_ROOT / "data" / "trading-channels.sqlite3")
