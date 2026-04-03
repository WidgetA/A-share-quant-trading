# === MODULE PURPOSE ===
# PostgreSQL persistence for daily scan top-N scored stocks.
# Stores the highest-scoring candidates each day for post-hoc analysis.
# Does NOT record whether a stock was actually traded.

# === KEY CONCEPTS ===
# - Saves top-N (default 5) by v3_score each trading day
# - Primary key: (trade_date, rank) — one row per rank per day
# - save_top_n replaces all rows for a date (clean upsert)

import asyncio
import logging
import os
from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING, Any

import asyncpg

if TYPE_CHECKING:
    from src.strategy.strategies.momentum_scanner import MomentumScoredStock

logger = logging.getLogger(__name__)


@dataclass
class MomentumScanDBConfig:
    """Configuration for momentum scan database."""

    host: str = "localhost"
    port: int = 5432
    database: str = "messages"
    user: str = "reader"
    password: str = ""
    pool_min_size: int = 2
    pool_max_size: int = 5
    schema: str = "trading"


SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS {schema};
"""

TABLES_SQL = """
CREATE TABLE IF NOT EXISTS {schema}.v15_scan_stocks (
    trade_date          DATE NOT NULL,
    rank                SMALLINT NOT NULL,
    stock_code          VARCHAR(20) NOT NULL,
    stock_name          VARCHAR(50) NOT NULL,
    board_name          VARCHAR(100) NOT NULL,
    v3_score            DECIMAL(10, 6) NOT NULL,
    open_price          DECIMAL(12, 4),
    prev_close          DECIMAL(12, 4),
    latest_price        DECIMAL(12, 4),
    gain_from_open_pct  DECIMAL(8, 4),
    turnover_amp        DECIMAL(10, 4),
    consecutive_up_days SMALLINT,
    trend_10d           DECIMAL(10, 6),
    final_candidates    INTEGER,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trade_date, rank)
);

CREATE INDEX IF NOT EXISTS idx_v15_scan_date
ON {schema}.v15_scan_stocks(trade_date DESC);
"""


class MomentumScanDB:
    """Async PostgreSQL store for daily scan top-N stocks."""

    def __init__(self, config: MomentumScanDBConfig):
        self._config = config
        self._pool: asyncpg.Pool | None = None
        self._is_connected = False
        self._schema = config.schema
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        if self._is_connected:
            return
        self._pool = await asyncpg.create_pool(
            host=self._config.host,
            port=self._config.port,
            database=self._config.database,
            user=self._config.user,
            password=self._config.password,
            min_size=self._config.pool_min_size,
            max_size=self._config.pool_max_size,
        )
        await self._create_tables()
        self._is_connected = True
        logger.info("MomentumScanDB connected to PostgreSQL")

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None
            self._is_connected = False

    async def _create_tables(self) -> None:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            await conn.execute(SCHEMA_SQL.format(schema=self._schema))
            await conn.execute(TABLES_SQL.format(schema=self._schema))

    @property
    def _db_pool(self) -> asyncpg.Pool:
        if not self._is_connected or not self._pool:
            raise RuntimeError("MomentumScanDB is not connected. Call connect() first.")
        return self._pool

    async def save_top_n(
        self,
        trade_date: date,
        scored_stocks: list["MomentumScoredStock"],
        final_candidates: int,
        n: int = 5,
    ) -> int:
        """Save top-N scored stocks for a trading day. Replaces existing data."""
        top = scored_stocks[:n]
        if not top:
            return 0

        async with self._lock:
            async with self._db_pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute(
                        f"DELETE FROM {self._schema}.v15_scan_stocks WHERE trade_date = $1",
                        trade_date,
                    )
                    for rank, s in enumerate(top, 1):
                        await conn.execute(
                            f"""
                            INSERT INTO {self._schema}.v15_scan_stocks
                            (trade_date, rank, stock_code, stock_name, board_name,
                             v3_score, open_price, prev_close, latest_price,
                             gain_from_open_pct, turnover_amp,
                             consecutive_up_days, trend_10d, final_candidates)
                            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
                            """,
                            trade_date,
                            rank,
                            s.stock_code,
                            s.stock_name,
                            s.board_name,
                            s.v3_score,
                            s.open_price,
                            s.prev_close,
                            s.latest_price,
                            s.gain_from_open_pct,
                            s.turnover_amp,
                            s.consecutive_up_days,
                            s.trend_10d,
                            final_candidates,
                        )

        logger.info(f"MomentumScanDB: saved top-{len(top)} for {trade_date}")
        return len(top)

    async def query(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int = 1000,
    ) -> list[dict]:
        """Query scan history. Returns list of dicts."""
        conditions: list[str] = []
        params: list[Any] = []
        idx = 1

        if start_date:
            conditions.append(f"trade_date >= ${idx}")
            params.append(date.fromisoformat(start_date))
            idx += 1
        if end_date:
            conditions.append(f"trade_date <= ${idx}")
            params.append(date.fromisoformat(end_date))
            idx += 1

        where = " AND ".join(conditions) if conditions else "1=1"
        params.append(limit)

        sql = f"""
            SELECT trade_date, rank, stock_code, stock_name, board_name,
                   v3_score, open_price, prev_close, latest_price,
                   gain_from_open_pct, turnover_amp,
                   consecutive_up_days, trend_10d, final_candidates
            FROM {self._schema}.v15_scan_stocks
            WHERE {where}
            ORDER BY trade_date DESC, rank
            LIMIT ${idx}
        """

        async with self._db_pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)

        def _f(val):
            return round(float(val), 4) if val is not None else None

        return [
            {
                "trade_date": row["trade_date"].isoformat(),
                "rank": row["rank"],
                "stock_code": row["stock_code"],
                "stock_name": row["stock_name"],
                "board_name": row["board_name"],
                "v3_score": _f(row["v3_score"]),
                "open_price": _f(row["open_price"]),
                "prev_close": _f(row["prev_close"]),
                "latest_price": _f(row["latest_price"]),
                "gain_from_open_pct": _f(row["gain_from_open_pct"]),
                "turnover_amp": _f(row["turnover_amp"]),
                "consecutive_up_days": row["consecutive_up_days"],
                "trend_10d": _f(row["trend_10d"]),
                "final_candidates": row["final_candidates"],
            }
            for row in rows
        ]


def create_momentum_scan_db_from_config() -> MomentumScanDB:
    """Create MomentumScanDB from database-config.yaml."""
    from src.common.config import load_config

    config = load_config("config/database-config.yaml")
    db_config = config.get_dict("database.trading", {})

    if not db_config:
        raise ValueError("Trading database configuration not found")

    def resolve_env(value: Any) -> Any:
        if isinstance(value, str) and value.startswith("${"):
            inner = value[2:-1]
            if ":" in inner:
                var_name, default = inner.split(":", 1)
            else:
                var_name, default = inner, ""
            return os.environ.get(var_name, default)
        return value

    return MomentumScanDB(
        MomentumScanDBConfig(
            host=resolve_env(db_config.get("host", "localhost")),
            port=int(resolve_env(db_config.get("port", 5432))),
            database=resolve_env(db_config.get("database", "messages")),
            user=resolve_env(db_config.get("user", "reader")),
            password=resolve_env(db_config.get("password", "")),
            pool_min_size=db_config.get("pool_min_size", 2),
            pool_max_size=db_config.get("pool_max_size", 5),
            schema=db_config.get("schema", "trading"),
        )
    )
