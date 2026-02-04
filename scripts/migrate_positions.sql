-- Migration SQL for position data
-- Run this in PostgreSQL to import position data from local JSON

-- First, ensure schema and tables exist
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
    slot_id INTEGER NOT NULL REFERENCES trading.position_slots(slot_id) ON DELETE CASCADE,
    stock_code VARCHAR(20) NOT NULL,
    stock_name VARCHAR(50),
    quantity INTEGER NOT NULL DEFAULT 0,
    entry_price DECIMAL(12, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Clear existing data (if re-running)
DELETE FROM trading.stock_holdings;
DELETE FROM trading.position_slots;

-- Insert position slots
INSERT INTO trading.position_slots (slot_id, slot_type, state, entry_time, entry_reason, sector_name)
VALUES
    (0, 'premarket', 'filled', '2026-01-27 09:30:00', '光通信板块利好', '光通信'),
    (1, 'premarket', 'filled', '2026-01-27 09:30:00', '盘前公告', '化工'),
    (2, 'premarket', 'filled', '2026-01-27 09:30:00', '盘前公告', '黄金'),
    (3, 'intraday', 'empty', NULL, '', NULL),
    (4, 'intraday', 'empty', NULL, '', NULL);

-- Insert stock holdings
-- Slot 0: 光通信板块
INSERT INTO trading.stock_holdings (slot_id, stock_code, stock_name, quantity, entry_price)
VALUES
    (0, '600498.SH', '烽火通信', 25000, 39.92),
    (0, '600487.SH', '亨通光电', 32500, 30.70);

-- Slot 1: 中国化学
INSERT INTO trading.stock_holdings (slot_id, stock_code, stock_name, quantity, entry_price)
VALUES
    (1, '601117.SH', '中国化学', 222700, 8.98);

-- Slot 2: 黄金板块
INSERT INTO trading.stock_holdings (slot_id, stock_code, stock_name, quantity, entry_price)
VALUES
    (2, '000506.SZ', '招金矿业', 30900, 21.40),
    (2, '002155.SZ', '湖南黄金', 35900, 27.80);

-- Verify
SELECT 'Slots:' as info;
SELECT slot_id, slot_type, state, sector_name FROM trading.position_slots ORDER BY slot_id;

SELECT 'Holdings:' as info;
SELECT h.slot_id, h.stock_code, h.stock_name, h.quantity, h.entry_price
FROM trading.stock_holdings h
ORDER BY h.slot_id, h.stock_code;
