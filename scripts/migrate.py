# scripts/migrate.py
"""
Ensure SQLite schema for ToolTally.

- Creates required tables if missing.
- Migrates existing tables to include missing columns (e.g., offers.created_at).
- Ensures helpful indexes.
- Enables WAL for better read/write behavior.

Usage:
  py scripts\migrate.py
"""

from __future__ import annotations

import os
import sqlite3
from typing import Set

DB_PATH = os.environ.get("DB_PATH", os.path.join("data", "tooltally.db"))

REQUIRED_TABLES = {
    "raw_offers",
    "vendors",
    "products",
    "offers",
    "meta",
}

def _get_columns(con: sqlite3.Connection, table: str) -> Set[str]:
    cur = con.execute(f"PRAGMA table_info('{table}')")
    cols = {row[1] for row in cur.fetchall()}
    cur.close()
    return cols

def _table_exists(con: sqlite3.Connection, table: str) -> bool:
    cur = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
    row = cur.fetchone()
    cur.close()
    return row is not None

def ensure_schema(con: sqlite3.Connection) -> None:
    cur = con.cursor()

    # --- Create tables if missing ---
    # raw_offers: staging from scrapers
    cur.execute("""
    CREATE TABLE IF NOT EXISTS raw_offers (
        id INTEGER PRIMARY KEY,
        vendor TEXT NOT NULL,
        title TEXT,
        price_pounds REAL NOT NULL,
        url TEXT NOT NULL,
        vendor_sku TEXT,
        category_name TEXT,
        scraped_at TEXT,
        processed INTEGER DEFAULT 0
    )
    """)

    # vendors: canonical vendors
    cur.execute("""
    CREATE TABLE IF NOT EXISTS vendors (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE
    )
    """)

    # products: canonical, unique products
    cur.execute("""
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        category TEXT
    )
    """)

    # offers: vendor offers for a product
    cur.execute("""
    CREATE TABLE IF NOT EXISTS offers (
        id INTEGER PRIMARY KEY,
        product_id INTEGER NOT NULL,
        vendor_id INTEGER NOT NULL,
        price_pounds REAL NOT NULL,
        url TEXT NOT NULL,
        created_at TEXT,
        FOREIGN KEY(product_id) REFERENCES products(id),
        FOREIGN KEY(vendor_id) REFERENCES vendors(id)
    )
    """)

    # meta: optional key/value store
    cur.execute("""
    CREATE TABLE IF NOT EXISTS meta (
        key TEXT PRIMARY KEY,
        value TEXT
    )
    """)

    con.commit()

    # --- Migrate existing tables: add missing columns ---

    # offers.created_at (needed by resolver v2)
    if _table_exists(con, "offers"):
        cols = _get_columns(con, "offers")
        if "created_at" not in cols:
            cur.execute("ALTER TABLE offers ADD COLUMN created_at TEXT")
            print("Migrated: added offers.created_at")

    # raw_offers.processed (used to track staging state)
    if _table_exists(con, "raw_offers"):
        cols = _get_columns(con, "raw_offers")
        if "processed" not in cols:
            cur.execute("ALTER TABLE raw_offers ADD COLUMN processed INTEGER DEFAULT 0")
            print("Migrated: added raw_offers.processed")

    con.commit()

    # --- Indexes ---
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_vendors_name ON vendors(name)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_products_name ON products(name)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_offers_product ON offers(product_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_offers_vendor ON offers(vendor_id)")
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_offers_url ON offers(url)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_raw_offers_vendor ON raw_offers(vendor)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_raw_offers_url ON raw_offers(url)")
    con.commit()

    cur.close()

def main() -> None:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    try:
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=NORMAL;")
        ensure_schema(con)
        print(f"Schema ensured at {os.path.abspath(DB_PATH)}")
    finally:
        con.close()

if __name__ == "__main__":
    main()
