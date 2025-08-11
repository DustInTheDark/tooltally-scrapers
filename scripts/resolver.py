# scripts/resolver.py
import re
import sqlite3
from pathlib import Path
from typing import Optional

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "tooltally.db"

ENSURE_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS vendors (
  id   INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS products (
  id       INTEGER PRIMARY KEY,
  name     TEXT NOT NULL,
  category TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_products_unique
  ON products(name, COALESCE(category, ''));

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

def _norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def _norm_category(cat: Optional[str]) -> Optional[str]:
    if not cat:
        return None
    c = _norm_space(cat)
    return c if c else None

def _get_or_create_vendor_id(cur: sqlite3.Cursor, name: str) -> int:
    cur.execute("SELECT id FROM vendors WHERE name = ?", (name,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO vendors(name) VALUES (?)", (name,))
    return cur.lastrowid

def _get_or_create_product_id(cur: sqlite3.Cursor, name: str, category: Optional[str]) -> int:
    if category is None:
        cur.execute("SELECT id FROM products WHERE name = ? AND category IS NULL", (name,))
        row = cur.fetchone()
        if row:
            return row[0]
        cur.execute("INSERT INTO products(name, category) VALUES (?, NULL)", (name,))
        return cur.lastrowid
    else:
        cur.execute("SELECT id FROM products WHERE name = ? AND category = ?", (name, category))
        row = cur.fetchone()
        if row:
            return row[0]
        cur.execute("INSERT INTO products(name, category) VALUES (?, ?)", (name, category))
        return cur.lastrowid

def resolve_batch(limit: int = 1000) -> tuple[int, int, int]:
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA foreign_keys = ON;")
    try:
        con.executescript(ENSURE_SQL)
        cur = con.cursor()

        cur.execute("""
            SELECT id, vendor, title, price_pounds, url, vendor_sku, category_name, scraped_at
            FROM raw_offers
            WHERE processed = 0
            ORDER BY id
            LIMIT ?
        """, (limit,))
        rows = cur.fetchall()
        if not rows:
            return (0, 0, 0)

        vendors_created = 0
        products_created = 0
        offers_upserted = 0

        for (raw_id, vendor, title, price, url, vendor_sku, category_name, scraped_at) in rows:
            vendor = _norm_space(vendor)
            title_norm = _norm_space(title)
            category_norm = _norm_category(category_name)

            # vendors
            cur.execute("SELECT id FROM vendors WHERE name = ?", (vendor,))
            vrow = cur.fetchone()
            if vrow:
                vendor_id = vrow[0]
            else:
                cur.execute("INSERT INTO vendors(name) VALUES (?)", (vendor,))
                vendor_id = cur.lastrowid
                vendors_created += 1

            # products
            if category_norm is None:
                cur.execute("SELECT id FROM products WHERE name = ? AND category IS NULL", (title_norm,))
                prow = cur.fetchone()
                if prow:
                    product_id = prow[0]
                else:
                    cur.execute("INSERT INTO products(name, category) VALUES (?, NULL)", (title_norm,))
                    product_id = cur.lastrowid
                    products_created += 1
            else:
                cur.execute("SELECT id FROM products WHERE name = ? AND category = ?", (title_norm, category_norm))
                prow = cur.fetchone()
                if prow:
                    product_id = prow[0]
                else:
                    cur.execute("INSERT INTO products(name, category) VALUES (?, ?)", (title_norm, category_norm))
                    product_id = cur.lastrowid
                    products_created += 1

            # offers (UPSERT by product_id + vendor_id + url)
            cur.execute("""
                INSERT INTO offers(product_id, vendor_id, price_pounds, url, vendor_sku, scraped_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(product_id, vendor_id, url) DO UPDATE SET
                  price_pounds = excluded.price_pounds,
                  vendor_sku   = COALESCE(excluded.vendor_sku, offers.vendor_sku),
                  scraped_at   = excluded.scraped_at
            """, (product_id, vendor_id, price, url, vendor_sku, scraped_at))
            offers_upserted += 1

            # mark raw row processed
            cur.execute("UPDATE raw_offers SET processed = 1 WHERE id = ?", (raw_id,))

        con.commit()
        return (vendors_created, products_created, offers_upserted)
    finally:
        con.close()

if __name__ == "__main__":
    total_offers = 0
    while True:
        v, p, o = resolve_batch(2000)
        total_offers += o
        print(f"Batch: vendors+{v}, products+{p}, offers upserted {o}")
        if o == 0:
            break
    print(f"Done. Total offers upserted: {total_offers}")
