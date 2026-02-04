#!/usr/bin/env python3
"""
Standalone script to migrate position data from JSON to PostgreSQL.
No dependencies on project modules - can run directly on server.

Usage:
    apt install python3-psycopg2
    python3 migrate_positions_standalone.py data.json
"""

import json
import sys
from pathlib import Path

import psycopg2

# Database configuration (same as docker-compose.prod.yml)
DB_CONFIG = {
    "host": "pc-uf691yl22372t36ej.pg.polardb.rds.aliyuncs.com",
    "port": 5432,
    "user": "widgeta",
    "password": "!a9HtYsI-C!1PkCt!Q",
    "dbname": "_supabase",
}

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
"""


def migrate(json_file: str) -> None:
    """Migrate positions from JSON file to PostgreSQL."""

    # Load JSON
    path = Path(json_file)
    if not path.exists():
        print(f"Error: File not found: {json_file}")
        sys.exit(1)

    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    print(f"Loaded data from {json_file}")
    print(f"Config: {data.get('config', {})}")

    # Filter filled slots
    slots = data.get("slots", [])
    filled_slots = [s for s in slots if s.get("state") == "filled"]
    print(f"Found {len(filled_slots)} filled positions to migrate")

    if not filled_slots:
        print("No positions to migrate")
        return

    # Connect to database
    print("Connecting to PostgreSQL...")
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
        conn.commit()
        print("Cleared existing data")

        # Insert all slots
        for slot in slots:
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
                print(f"  Slot {slot['slot_id']} ({slot['slot_type']}): {len(holdings)} holdings")
                for h in holdings:
                    print(f"    - {h['stock_code']} {h.get('stock_name')}: {h.get('quantity')} @ {h.get('entry_price')}")

        conn.commit()
        print(f"\nMigrated {len(slots)} slots successfully!")

        # Verify
        cur.execute("SELECT COUNT(*) FROM trading.position_slots WHERE state = 'filled'")
        count = cur.fetchone()[0]
        print(f"Verification: {count} filled slots in database")

    finally:
        cur.close()
        conn.close()
        print("Connection closed")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        json_file = "data.json"
    else:
        json_file = sys.argv[1]

    migrate(json_file)
