#!/usr/bin/env python3
"""
Standalone script to update position data to PostgreSQL.
No dependencies on project modules - can run directly on server.

Usage:
    apt install python3-psycopg2
    python3 update_positions_standalone.py
"""

import psycopg2

# Database configuration (same as docker-compose.prod.yml)
DB_CONFIG = {
    "host": "pc-uf691yl22372t36ej.pg.polardb.rds.aliyuncs.com",
    "port": 5432,
    "user": "widgeta",
    "password": "!a9HtYsI-C!1PkCt!Q",
    "dbname": "_supabase",
}

# Account state
TOTAL_ASSETS = 10025998.0  # 总资产
TOTAL_CAPITAL = 10000000.0  # 初始本金

# Position data from local position_state.json
POSITION_CONFIG = {
    "total_capital": TOTAL_CAPITAL,
    "num_slots": 5,
    "premarket_slots": 3,
    "intraday_slots": 2,
    "min_order_amount": 10000.0,
    "lot_size": 100,
}

SLOTS = [
    {
        "slot_id": 0,
        "slot_type": "premarket",
        "state": "filled",
        "entry_time": "2026-01-27T09:30:00",
        "entry_reason": "光通信板块利好",
        "sector_name": "光通信",
        "holdings": [
            {
                "stock_code": "600498.SH", "stock_name": "烽火通信",
                "quantity": 25000, "entry_price": 39.92,
            },
            {
                "stock_code": "600487.SH", "stock_name": "亨通光电",
                "quantity": 32500, "entry_price": 30.70,
            },
        ],
    },
    {
        "slot_id": 1,
        "slot_type": "premarket",
        "state": "filled",
        "entry_time": "2026-01-27T09:30:00",
        "entry_reason": "盘前公告",
        "sector_name": "化工",
        "holdings": [
            {
                "stock_code": "601117.SH", "stock_name": "中国化学",
                "quantity": 222700, "entry_price": 8.98,
            },
        ],
    },
    {
        "slot_id": 2,
        "slot_type": "premarket",
        "state": "filled",
        "entry_time": "2026-01-27T09:30:00",
        "entry_reason": "盘前公告",
        "sector_name": "黄金",
        "holdings": [
            {
                "stock_code": "000506.SZ", "stock_name": "招金矿业",
                "quantity": 30900, "entry_price": 21.4,
            },
            {
                "stock_code": "002155.SZ", "stock_name": "湖南黄金",
                "quantity": 35900, "entry_price": 27.8,
            },
        ],
    },
    {
        "slot_id": 3,
        "slot_type": "intraday",
        "state": "empty",
        "entry_time": None,
        "entry_reason": "",
        "sector_name": None,
        "holdings": [],
    },
    {
        "slot_id": 4,
        "slot_type": "intraday",
        "state": "empty",
        "entry_time": None,
        "entry_reason": "",
        "sector_name": None,
        "holdings": [],
    },
]

# SQL to create tables
CREATE_TABLES_SQL = """
CREATE SCHEMA IF NOT EXISTS trading;

CREATE TABLE IF NOT EXISTS trading.position_slots (
    id SERIAL PRIMARY KEY,
    slot_id INTEGER NOT NULL UNIQUE,
    slot_type VARCHAR(20) NOT NULL,
    state VARCHAR(20) NOT NULL DEFAULT 'empty',
    entry_time TIMESTAMP,
    entry_reason TEXT,
    sector_name VARCHAR(100),
    pending_order_id VARCHAR(50),
    exit_price DECIMAL(12, 4),
    exit_time TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS trading.stock_holdings (
    id SERIAL PRIMARY KEY,
    slot_id INTEGER NOT NULL,
    stock_code VARCHAR(20) NOT NULL,
    stock_name VARCHAR(50),
    quantity INTEGER NOT NULL DEFAULT 0,
    entry_price DECIMAL(12, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS trading.account_state (
    id SERIAL PRIMARY KEY,
    total_capital DECIMAL(14, 2) NOT NULL,
    total_assets DECIMAL(14, 2) NOT NULL,
    cash_balance DECIMAL(14, 2) NOT NULL,
    realized_pnl DECIMAL(14, 2) DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


def calculate_holdings_cost() -> float:
    """Calculate total cost of all holdings."""
    total = 0.0
    for slot in SLOTS:
        for h in slot.get("holdings", []):
            total += h["quantity"] * h["entry_price"]
    return total


def update_positions() -> None:
    """Update positions to PostgreSQL."""

    holdings_cost = calculate_holdings_cost()
    cash_balance = TOTAL_ASSETS - holdings_cost
    realized_pnl = TOTAL_ASSETS - TOTAL_CAPITAL

    print("=" * 60)
    print("Account Summary")
    print("=" * 60)
    print(f"初始本金:     {TOTAL_CAPITAL:>14,.2f} 元")
    print(f"总资产:       {TOTAL_ASSETS:>14,.2f} 元")
    print(f"持仓成本:     {holdings_cost:>14,.2f} 元")
    print(f"现金余额:     {cash_balance:>14,.2f} 元")
    print(f"累计盈亏:     {realized_pnl:>+14,.2f} 元")
    print("=" * 60)

    # Connect to database
    print("\nConnecting to PostgreSQL...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    print("Connected!")

    try:
        # Create tables
        cur.execute(CREATE_TABLES_SQL)
        conn.commit()
        print("Tables created/verified")

        # Clear existing data
        cur.execute("DELETE FROM trading.stock_holdings")
        cur.execute("DELETE FROM trading.position_slots")
        cur.execute("DELETE FROM trading.account_state")
        conn.commit()
        print("Cleared existing data")

        # Insert account state
        cur.execute(
            """
            INSERT INTO trading.account_state
            (total_capital, total_assets, cash_balance, realized_pnl, updated_at)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
            """,
            (TOTAL_CAPITAL, TOTAL_ASSETS, cash_balance, realized_pnl),
        )
        print(f"Account state inserted: total_assets={TOTAL_ASSETS:,.2f}")

        # Insert all slots
        for slot in SLOTS:
            cur.execute(
                """
                INSERT INTO trading.position_slots
                (slot_id, slot_type, state, entry_time, entry_reason, sector_name)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (slot_id) DO UPDATE SET
                    slot_type = EXCLUDED.slot_type,
                    state = EXCLUDED.state,
                    entry_time = EXCLUDED.entry_time,
                    entry_reason = EXCLUDED.entry_reason,
                    sector_name = EXCLUDED.sector_name,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    slot["slot_id"],
                    slot["slot_type"],
                    slot["state"],
                    slot.get("entry_time"),
                    slot.get("entry_reason", ""),
                    slot.get("sector_name"),
                ),
            )

            # Insert holdings
            holdings = slot.get("holdings", [])
            for h in holdings:
                cur.execute(
                    """
                    INSERT INTO trading.stock_holdings
                    (slot_id, stock_code, stock_name, quantity, entry_price)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        slot["slot_id"],
                        h["stock_code"],
                        h.get("stock_name"),
                        h.get("quantity", 0),
                        h.get("entry_price"),
                    ),
                )

            if holdings:
                slot_id = slot["slot_id"]
                slot_type = slot["slot_type"]
                sector = slot["sector_name"]
                print(f"  Slot {slot_id} ({slot_type}, {sector}): {len(holdings)} holdings")
                for h in holdings:
                    code, name = h["stock_code"], h.get("stock_name")
                    qty, price = h.get("quantity"), h.get("entry_price")
                    cost = qty * price
                    print(f"    - {code} {name}: {qty:,} @ {price:.2f} = {cost:,.2f}")

        conn.commit()
        print(f"\nUpdated {len(SLOTS)} slots successfully!")

        # Verify
        cur.execute("SELECT COUNT(*) FROM trading.position_slots WHERE state = 'filled'")
        count = cur.fetchone()[0]
        print(f"Verification: {count} filled slots in database")

        cur.execute("SELECT total_assets, cash_balance, realized_pnl FROM trading.account_state")
        row = cur.fetchone()
        if row:
            print(f"Account: total_assets={row[0]:,.2f}, cash={row[1]:,.2f}, pnl={row[2]:+,.2f}")

    finally:
        cur.close()
        conn.close()
        print("\nConnection closed")


if __name__ == "__main__":
    update_positions()
