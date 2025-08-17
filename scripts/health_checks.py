#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Health checks for ToolTally SQLite DB.

Run:
  py scripts\\health_checks.py
"""

import sqlite3
from textwrap import dedent

DB_PATH = "data/tooltally.db"

def one(cur, q: str) -> int:
    cur.execute(q)
    row = cur.fetchone()
    return 0 if row is None else (row[0] or 0)

def main():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    print("=== Basic counts ===")
    stats = [
        ("raw_offers rows", "SELECT COUNT(*) FROM raw_offers"),
        ("raw_offers unprocessed", "SELECT COUNT(*) FROM raw_offers WHERE processed=0"),
        ("products rows", "SELECT COUNT(*) FROM products"),
        ("offers rows", "SELECT COUNT(*) FROM offers"),
        ("vendors rows", "SELECT COUNT(*) FROM vendors"),
    ]
    for label, q in stats:
        print(f"{label:24s}: {one(cur, q)}")

    print("\n=== Cross-vendor MPN overlap in raw_offers ===")
    q_overlap = dedent(r"""
        SELECT COUNT(*) FROM (
          SELECT UPPER(REPLACE(REPLACE(mpn,'-',''),' ','')) AS k,
                 COUNT(DISTINCT vendor) AS vendor_cnt, COUNT(*) AS n
          FROM raw_offers
          WHERE mpn IS NOT NULL AND TRIM(mpn) <> ''
          GROUP BY k
          HAVING vendor_cnt >= 2
        )
    """).strip()
    print("Cross-vendor MPN keys:", one(cur, q_overlap))

    print("\n=== Multi-vendor products (post-resolve) ===")
    q_multivendor = dedent(r"""
        SELECT COUNT(*) FROM (
          SELECT product_id, COUNT(DISTINCT vendor_id) AS vc
          FROM offers
          GROUP BY product_id
          HAVING vc > 1
        )
    """).strip()
    print("Products with >1 vendor:", one(cur, q_multivendor))

    print("\n=== Products by fingerprint type ===")
    by_fp = [
        ("ean",   "SELECT COUNT(*) FROM products WHERE fingerprint LIKE 'ean:%'"),
        ("mpn",   "SELECT COUNT(*) FROM products WHERE fingerprint LIKE 'mpn:%'"),
        ("model", "SELECT COUNT(*) FROM products WHERE fingerprint LIKE 'model:%'"),
        ("other/NULL", "SELECT COUNT(*) FROM products WHERE fingerprint IS NULL OR fingerprint=''"),
    ]
    for label, q in by_fp:
        print(f"{label:11s}: {one(cur, q)}")

    print("\n=== Top 25 biggest vendor clusters ===")
    q_top = dedent(r"""
        SELECT p.id, substr(p.name,1,80) AS title, COUNT(DISTINCT o.vendor_id) AS vendors, p.fingerprint
        FROM products p
        JOIN offers o ON o.product_id = p.id
        GROUP BY p.id
        ORDER BY vendors DESC, p.id
        LIMIT 25
    """).strip()
    for row in cur.execute(q_top):
        pid, title, vendors, fp = row
        print(f"#{pid:>6}  v={vendors}  fp={fp}  {title}")

    print("\n=== Sample split-by-key suspects (same MPN across multiple products) ===")
    # Helps catch cases where MPN overlaps but you've still got many product_ids
    q_split = dedent(r"""
        WITH mpn_keys AS (
          SELECT p.id AS product_id, p.fingerprint,
                 UPPER(REPLACE(REPLACE(ro.mpn,'-',''),' ','')) AS mpn_norm
          FROM products p
          JOIN offers o ON o.product_id = p.id
          JOIN raw_offers ro ON ro.url = o.url
          WHERE ro.mpn IS NOT NULL AND TRIM(ro.mpn) <> ''
        ),
        agg AS (
          SELECT mpn_norm, COUNT(DISTINCT product_id) AS prod_cnt
          FROM mpn_keys
          WHERE mpn_norm IS NOT NULL
          GROUP BY mpn_norm
          HAVING prod_cnt >= 2
          ORDER BY prod_cnt DESC
          LIMIT 25
        )
        SELECT * FROM agg
    """).strip()
    for row in cur.execute(q_split):
        print("MPN split suspect:", row)

    con.close()


if __name__ == "__main__":
    main()
