# scripts/migrate.py
import os
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "tooltally.db"

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

-- Vendors catalog
CREATE TABLE IF NOT EXISTS vendors (
  id   INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE
);

-- Canonical products
CREATE TABLE IF NOT EXISTS products (
  id       INTEGER PRIMARY KEY,
  name     TEXT NOT NULL,
  category TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_products_unique
  ON products(name, COALESCE(category, ''));

-- Offers (vendor-level prices pointing to canonical products)
CREATE TABLE IF NOT EXISTS offers (
  id           INTEGER PRIMARY KEY,
  product_id   INTEGER NOT NULL,
  vendor_id    INTEGER NOT NULL,
  price_pounds REAL    NOT NULL,
  url          TEXT    NOT NULL,
  vendor_sku   TEXT,
  scraped_at   TEXT,
  UNIQUE(product_id, vendor_id, url) ON CONFLICT REPLACE,
  FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE,
  FOREIGN KEY(vendor_id)  REFERENCES vendors(id)  ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_offers_product ON offers(product_id);
CREATE INDEX IF NOT EXISTS idx_offers_vendor  ON offers(vendor_id);

-- Raw staging area for scrapers
CREATE TABLE IF NOT EXISTS raw_offers (
  id           INTEGER PRIMARY KEY,
  vendor       TEXT    NOT NULL,
  title        TEXT    NOT NULL,
  price_pounds REAL    NOT NULL,
  url          TEXT    NOT NULL,
  vendor_sku   TEXT,
  category_name TEXT,
  scraped_at   TEXT,
  processed    INTEGER NOT NULL DEFAULT 0
);
-- Prevent exact duplicate vendor+url rows piling up; allow updates via UPSERT
CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_unique ON raw_offers(vendor, url);
CREATE INDEX IF NOT EXISTS idx_raw_processed ON raw_offers(processed);

-- Tiny meta table for sanity (optional)
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT
);
INSERT OR IGNORE INTO meta(key, value) VALUES ('schema_version', '1');
"""

def ensure_parent_dir():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

def migrate():
    ensure_parent_dir()
    con = sqlite3.connect(DB_PATH)
    try:
        con.executescript(SCHEMA_SQL)
        con.commit()
        print(f"Schema ensured at {DB_PATH}")
    finally:
        con.close()

if __name__ == "__main__":
    migrate()
