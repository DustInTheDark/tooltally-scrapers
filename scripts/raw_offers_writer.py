# scripts/raw_offers_writer.py
import sqlite3
from pathlib import Path
from typing import Dict, List, Any

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "tooltally.db"

CREATE_RAW_SQL = """
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS raw_offers (
  id            INTEGER PRIMARY KEY,
  vendor        TEXT    NOT NULL,
  title         TEXT    NOT NULL,
  price_pounds  REAL    NOT NULL,
  url           TEXT    NOT NULL,
  vendor_sku    TEXT,
  category_name TEXT,
  scraped_at    TEXT,
  processed     INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_unique ON raw_offers(vendor, url);
CREATE INDEX IF NOT EXISTS idx_raw_processed ON raw_offers(processed);
"""

UPSERT_SQL = """
INSERT INTO raw_offers (
  vendor, title, price_pounds, url, vendor_sku, category_name, scraped_at, processed
) VALUES (
  :vendor, :title, :price_pounds, :url, :vendor_sku, :category_name, :scraped_at, 0
)
ON CONFLICT(vendor, url) DO UPDATE SET
  title        = excluded.title,
  price_pounds = excluded.price_pounds,
  vendor_sku   = COALESCE(excluded.vendor_sku, raw_offers.vendor_sku),
  category_name= COALESCE(excluded.category_name, raw_offers.category_name),
  scraped_at   = excluded.scraped_at,
  processed    = 0;
"""

def _ensure_table(con: sqlite3.Connection) -> None:
    con.executescript(CREATE_RAW_SQL)

def save_many_raw_offers(rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    con = sqlite3.connect(DB_PATH)
    try:
        _ensure_table(con)
        con.executemany(UPSERT_SQL, rows)
        con.commit()
        # sqlite3 rowcount is unreliable for executemany; just report input length
        return len(rows)
    finally:
        con.close()
