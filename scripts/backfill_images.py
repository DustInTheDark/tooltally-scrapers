#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Backfill product images by visiting vendor product pages and extracting og:image / twitter:image.
- Chooses one best image per product (first vendor with a valid absolute image).
- Writes to products.image_url.
- Skips products that already have a non-empty image_url unless --force is used.

Usage:
  pip install requests beautifulsoup4 lxml
  py scripts\\backfill_images.py --limit 500
"""

from __future__ import annotations
import argparse
import re
import sqlite3
import sys
from typing import Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

DB_PATH = "data/tooltally.db"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

def open_db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

ABS_URL_RE = re.compile(r"^https?://", re.I)

def is_abs(url: str) -> bool:
    return bool(url and ABS_URL_RE.match(url))

def clean_img_src(src: Optional[str]) -> Optional[str]:
    if not src:
        return None
    s = src.strip()
    # common tracking/query junk could be removed here if needed
    return s or None

def pick_from_meta(soup: BeautifulSoup, base_url: str) -> Optional[str]:
    # Priority: og:image, twitter:image, meta[name=image], link[rel=image_src]
    # then common product image selectors as fallback
    # Gather candidates in order
    metas = [
        ('meta[property="og:image"]', 'content'),
        ('meta[name="og:image"]', 'content'),
        ('meta[name="twitter:image:src"]', 'content'),
        ('meta[name="twitter:image"]', 'content'),
        ('meta[name="image"]', 'content'),
        ('link[rel="image_src"]', 'href'),
    ]
    for sel, attr in metas:
        el = soup.select_one(sel)
        if el:
            src = clean_img_src(el.get(attr))
            if src:
                return src if is_abs(src) else urljoin(base_url, src)

    # Fallbacks: look for product gallery-ish selectors
    candidates = []
    for sel in [
        'img#main-image',
        'img[itemprop="image"]',
        'img.product-image',
        'img.product__image',
        'img[src*="/product/"]',
        'img[src*="catalog"]',
        'img[class*="product"]',
        'img[class*="gallery"]',
    ]:
        for img in soup.select(sel):
            src = clean_img_src(img.get("src") or img.get("data-src") or img.get("data-original"))
            if src:
                full = src if is_abs(src) else urljoin(base_url, src)
                candidates.append(full)

    # Prefer https and larger-looking files (very naive heuristic)
    def score(u: str) -> Tuple[int, int]:
        https = u.lower().startswith("https")
        # crude "size" hint: presence of common size tokens
        big_hint = any(x in u.lower() for x in ["1200", "1000", "1024", "800", "large", "xl"])
        return (1 if https else 0, 1 if big_hint else 0)

    if candidates:
        candidates.sort(key=score, reverse=True)
        return candidates[0]

    return None

def fetch_image_from_page(url: str, timeout: int = 15) -> Optional[str]:
    try:
        resp = requests.get(url, headers={"User-Agent": UA}, timeout=timeout)
    except requests.RequestException:
        return None
    if resp.status_code >= 400 or "text/html" not in resp.headers.get("Content-Type", ""):
        return None
    soup = BeautifulSoup(resp.text, "lxml")
    return pick_from_meta(soup, base_url=url)

def pick_offer_url_for_product(cur: sqlite3.Cursor, pid_text: str) -> Optional[str]:
    # Prefer offers that already have a URL and a non-zero price
    cur.execute("""
        SELECT o.url
        FROM offers o
        WHERE CAST(o.product_id AS TEXT) = ?
          AND o.url IS NOT NULL AND o.url <> ''
        ORDER BY
          CASE WHEN (o.price_pounds IS NULL OR o.price_pounds=0) THEN 1 ELSE 0 END ASC,
          o.vendor_id ASC,
          o.id ASC
        LIMIT 5
    """, (pid_text,))
    for row in cur.fetchall():
        url = (row["url"] or "").strip()
        if url and url.lower().startswith(("http://", "https://")):
            return url
    return None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=500, help="max products to process")
    parser.add_argument("--force", action="store_true", help="overwrite existing image_url")
    args = parser.parse_args()

    con = open_db()
    cur = con.cursor()

    # Pick products lacking image (or all with --force)
    if args.force:
        cur.execute("""
            SELECT CAST(p.id AS TEXT) AS id
            FROM products p
            ORDER BY p.id ASC
            LIMIT ?
        """, (args.limit,))
    else:
        cur.execute("""
            SELECT CAST(p.id AS TEXT) AS id
            FROM products p
            WHERE p.image_url IS NULL OR TRIM(p.image_url) = ''
            ORDER BY p.id ASC
            LIMIT ?
        """, (args.limit,))

    rows = cur.fetchall()
    processed = 0
    updated = 0

    for r in rows:
        pid = r["id"]
        processed += 1

        # If not forcing, skip if already has image_url (defensive double-check)
        if not args.force:
            existing = cur.execute(
                "SELECT image_url FROM products WHERE CAST(id AS TEXT)=?", (pid,)
            ).fetchone()
            if existing and existing["image_url"]:
                continue

        offer_url = pick_offer_url_for_product(cur, pid)
        if not offer_url:
            continue

        img = fetch_image_from_page(offer_url)
        if not img:
            continue

        # Basic sanity: absolute URL and looks like an image
        p = urlparse(img)
        if not p.scheme or not p.netloc:
            continue
        if not re.search(r"\.(jpg|jpeg|png|webp|gif)(\?|$)", img, re.I):
            # Still allow if og:image without extension; most CDNs OK.
            pass

        cur.execute("UPDATE products SET image_url=? WHERE CAST(id AS TEXT)=?", (img, pid))
        updated += 1
        if updated % 20 == 0:
            con.commit()
            print(f"Updated {updated} images…")

    con.commit()
    con.close()
    print(f"Done. processed={processed}, updated={updated}")

if __name__ == "__main__":
    sys.exit(main())
