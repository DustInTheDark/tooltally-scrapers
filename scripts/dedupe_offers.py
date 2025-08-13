# scripts/dedupe_offers.py
"""
Keep exactly 1 offer per (product_id, vendor_id):
- Choose the lowest price
- If prices tie, choose the most recent scraped_at
- As a final tiebreaker, choose the largest id
Prints how many rows were removed.
"""

import os
import sqlite3

DB_PATH = os.environ.get("DB_PATH") or os.path.join(os.path.dirname(__file__), "..", "data", "tooltally.db")
DB_PATH = os.path.abspath(DB_PATH)

def main():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("PRAGMA foreign_keys=ON;")
    cur.execute("PRAGMA journal_mode=WAL;")

    # Count how many duplicates we plan to delete
    cur.executescript("""
    DROP TABLE IF EXISTS _to_delete;
    CREATE TEMP TABLE _to_delete AS
    WITH ranked AS (
      SELECT id, product_id, vendor_id, price_pounds, scraped_at,
             ROW_NUMBER() OVER (
               PARTITION BY product_id, vendor_id
               ORDER BY price_pounds ASC, datetime(scraped_at) DESC, id DESC
             ) AS rn
      FROM offers
    )
    SELECT id FROM ranked WHERE rn > 1;
    """)
    cur.execute("SELECT COUNT(*) FROM _to_delete;")
    (dupes_count,) = cur.fetchone()

    # Delete the duplicates
    cur.execute("DELETE FROM offers WHERE id IN (SELECT id FROM _to_delete);")
    con.commit()

    print(f"Deduped offers: removed {dupes_count} extra row(s).")
    con.close()

if __name__ == "__main__":
    print(f"DB: {DB_PATH}")
    main()
