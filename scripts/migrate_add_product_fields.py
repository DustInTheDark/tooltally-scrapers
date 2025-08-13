# scripts/migrate_add_product_fields.py
"""
Idempotent migration to add optional columns to `products` and indexes we use
for robust cross-vendor matching. Safe to run multiple times.

Adds:
- products.fingerprint (TEXT, UNIQUE INDEX)
- products.brand, products.model, products.power_source, products.voltage,
  products.kit, products.chuck, products.ean_gtin (all nullable)

This does NOT drop anything and won't break existing queries.
"""

import os
import sqlite3
from datetime import datetime

DB_PATH = os.environ.get("DB_PATH") or os.path.join(os.path.dirname(__file__), "..", "data", "tooltally.db")
DB_PATH = os.path.abspath(DB_PATH)

def column_exists(cur, table, column):
    cur.execute(f"PRAGMA table_info({table})")
    return any(row[1].lower() == column.lower() for row in cur.fetchall())

def index_exists(cur, name):
    cur.execute("PRAGMA index_list(products)")
    return any(row[1].lower() == name.lower() for row in cur.fetchall())

def main():
    con = sqlite3.connect(DB_PATH)
    con.isolation_level = None
    cur = con.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA foreign_keys=ON;")

    try:
        cur.execute("BEGIN;")
        # Add columns if missing
        cols = [
            ("fingerprint", "TEXT"),
            ("brand", "TEXT"),
            ("model", "TEXT"),
            ("power_source", "TEXT"),
            ("voltage", "INTEGER"),
            ("kit", "TEXT"),
            ("chuck", "TEXT"),
            ("ean_gtin", "TEXT"),
        ]
        for name, typ in cols:
            if not column_exists(cur, "products", name):
                cur.execute(f"ALTER TABLE products ADD COLUMN {name} {typ};")

        # Unique index on fingerprint (if present)
        if not index_exists(cur, "idx_products_fingerprint"):
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_products_fingerprint ON products(fingerprint);")

        # Helpful indexes on offers
        cur.execute("CREATE INDEX IF NOT EXISTS idx_offers_product ON offers(product_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_offers_vendor ON offers(vendor_id);")

        cur.execute("COMMIT;")
        print(f"[{datetime.utcnow().isoformat()}Z] Migration completed OK on {DB_PATH}")
    except Exception:
        cur.execute("ROLLBACK;")
        raise
    finally:
        con.close()

if __name__ == "__main__":
    main()
