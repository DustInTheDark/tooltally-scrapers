# scripts/resolver.py
"""
Enhanced resolver for ToolTally:
- Reads new/changed rows from raw_offers
- Builds a robust fingerprint per product (EAN/GTIN > brand+model+voltage+kit+category)
- Groups offers from multiple vendors into one canonical product
- Upserts products and offers without breaking existing logic
- Marks processed rows when done

Backwards-compatible with existing DB; works with optional extra columns
from migration script.
"""

import os
import re
import sqlite3
from datetime import datetime

DB_PATH = os.environ.get("DB_PATH") or os.path.join(os.path.dirname(__file__), "..", "data", "tooltally.db")
DB_PATH = os.path.abspath(DB_PATH)

# --- Heuristics for parsing ---------------------------------------------------

BRANDS = r"(makita|dewalt|bosch|milwaukee|einhell|ryobi|black\+?decker|hikoki|stanley|metabo|titan|parkside)"
MODEL_PATTERNS = [
    r"\b([A-Z]{2,4}\d{2,4}[A-Z]?)\b",
    r"\b([A-Z]{2,4}\d{2,4})\b",
    r"\b(\d{3,4}[A-Z]{1,2})\b",
]
VOLT_RE = r"(10\.8|10v|12v|14\.4|18v|20v|36v|40v|max|110v|115v|220v|230v|240v)"
KIT_PATTERNS = [
    ("bare", r"\b(bare unit|body only|tool only)\b"),
    ("1x1.5Ah", r"\b1\s*x\s*1\.?5\s*ah\b"),
    ("1x2Ah", r"\b1\s*x\s*2\s*ah\b"),
    ("2x3Ah", r"\b2\s*x\s*3\s*ah\b"),
    ("2x4Ah", r"\b2\s*x\s*4\s*ah\b"),
    ("2x5Ah", r"\b2\s*x\s*5\s*ah\b"),
]

def norm_space(s: str) -> str:
    return re.sub(r"[\s/_-]+", " ", (s or "")).strip()

def norm_lower(s: str) -> str:
    return norm_space(s).lower()

def extract_brand(title: str) -> str:
    m = re.search(BRANDS, title or "", re.I)
    return m.group(1).lower() if m else ""

def extract_model(title: str) -> str:
    t = (title or "").upper()
    for pat in MODEL_PATTERNS:
        m = re.search(pat, t)
        if m:
            return m.group(1)
    return ""

def extract_voltage(title: str) -> int | None:
    t = (title or "").lower()
    m = re.search(VOLT_RE, t)
    if not m:
        return None
    g = m.group(1).lower().replace("max", "")
    g = g.replace("v", "")
    g = g.replace("10.8", "12")
    try:
        return int(re.sub(r"\D", "", g))
    except Exception:
        return None

def extract_kit(title: str) -> str:
    t = (title or "").lower()
    for label, pat in KIT_PATTERNS:
        if re.search(pat, t):
            return label
    return ""

def build_fingerprint(title: str, category: str, vendor_sku: str | None = None, ean_gtin: str | None = None) -> str:
    if ean_gtin:
        return f"ean:{ean_gtin}"
    brand = extract_brand(title)
    model = extract_model(title)
    volt = extract_voltage(title)
    kit = extract_kit(title)
    cat = norm_lower(category or "")
    parts = [brand, model, str(volt or ""), kit, cat]
    key = " | ".join([p for p in parts if p])
    if not key and vendor_sku:
        key = f"sku:{norm_lower(vendor_sku)}"
    return key

# --- DB helpers ---------------------------------------------------------------

def dict_factory(cursor, row):
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}

def ensure_vendor_id(cur, row) -> int | None:
    vid = row.get("vendor_id")
    if isinstance(vid, int):
        return vid
    vname = norm_space(row.get("vendor") or row.get("vendor_name") or "")
    vdom = norm_lower(row.get("vendor_domain") or row.get("domain") or "")
    if vname:
        cur.execute("SELECT id FROM vendors WHERE lower(name)=lower(?)", (vname,))
        r = cur.fetchone()
        if r:
            return r[0]
    if vdom:
        cur.execute("SELECT id FROM vendors WHERE lower(domain)=?", (vdom,))
        r = cur.fetchone()
        if r:
            return r[0]
    if vname:
        cur.execute("INSERT INTO vendors(name, domain) VALUES(?, ?)", (vname, vdom or None))
        return cur.lastrowid
    return None

def upsert_product(cur, name, category, fp, brand=None, model=None, voltage=None, kit=None, ean_gtin=None):
    if fp:
        cur.execute("SELECT id FROM products WHERE fingerprint = ?", (fp,))
        row = cur.fetchone()
        if row:
            pid = row[0]
            cur.execute("""
                UPDATE products
                   SET name = COALESCE(name, ?),
                       category = COALESCE(category, ?),
                       brand = COALESCE(brand, ?),
                       model = COALESCE(model, ?),
                       voltage = COALESCE(voltage, ?),
                       kit = COALESCE(kit, ?),
                       ean_gtin = COALESCE(ean_gtin, ?)
                 WHERE id = ?
            """, (name, category, brand, model, voltage, kit, ean_gtin, pid))
            return pid
    cur.execute("""
        INSERT INTO products(name, category, fingerprint, brand, model, voltage, kit, ean_gtin)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?)
    """, (name, category, fp, brand, model, voltage, kit, ean_gtin))
    return cur.lastrowid

def insert_or_replace_offer(cur, product_id, vendor_id, price, url, vendor_sku, scraped_at):
    cur.execute("""
        SELECT id FROM offers
        WHERE product_id=? AND vendor_id=?
        ORDER BY datetime(scraped_at) DESC
        LIMIT 1
    """, (product_id, vendor_id))
    row = cur.fetchone()
    if row:
        cur.execute("""
            UPDATE offers
               SET price_pounds=?,
                   url=?,
                   vendor_sku=?,
                   scraped_at=?
             WHERE id=?
        """, (price, url, vendor_sku, scraped_at, row[0]))
    else:
        cur.execute("""
            INSERT INTO offers(product_id, vendor_id, price_pounds, url, vendor_sku, scraped_at, created_at)
            VALUES(?, ?, ?, ?, ?, ?, ?)
        """, (product_id, vendor_id, price, url, vendor_sku, scraped_at, datetime.utcnow().isoformat() + "Z"))

# --- Main ---------------------------------------------------------------------

def main():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = dict_factory
    cur = con.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA foreign_keys=ON;")
    try:
        cur.execute("SELECT * FROM raw_offers WHERE COALESCE(processed, 0) = 0")
        raw_rows = cur.fetchall()
    except sqlite3.OperationalError:
        cur.execute("SELECT * FROM raw_offers")
        raw_rows = cur.fetchall()
    if not raw_rows:
        print("No new raw_offers to process.")
        con.close()
        return
    print(f"Processing {len(raw_rows)} raw_offers rows…")
    batch = 0
    try:
        cur.execute("BEGIN;")
        for row in raw_rows:
            title = row.get("title") or row.get("name") or ""
            category = row.get("category") or row.get("category_name") or ""
            price = row.get("price_pounds") or row.get("price") or None
            try:
                price = float(price) if price is not None else None
            except Exception:
                price = None
            url = row.get("url") or row.get("buy_url") or None
            vendor_sku = row.get("vendor_sku") or row.get("sku") or None
            ean_gtin = row.get("ean_gtin") or row.get("ean") or row.get("gtin") or None
            scraped_at = row.get("scraped_at") or datetime.utcnow().isoformat() + "Z"
            if price is None or url is None:
                continue
            vendor_id = ensure_vendor_id(cur, row)
            if not vendor_id:
                continue
            fp = build_fingerprint(title, category, vendor_sku, ean_gtin)
            brand = extract_brand(title)
            model = extract_model(title)
            voltage = extract_voltage(title)
            kit = extract_kit(title)
            pid = upsert_product(
                cur,
                name=norm_space(title),
                category=norm_space(category) or None,
                fp=fp or None,
                brand=brand or None,
                model=model or None,
                voltage=voltage,
                kit=kit or None,
                ean_gtin=ean_gtin or None,
            )
            insert_or_replace_offer(
                cur,
                product_id=pid,
                vendor_id=vendor_id,
                price=price,
                url=url,
                vendor_sku=vendor_sku,
                scraped_at=scraped_at,
            )
            if "processed" in row:
                cur.execute("UPDATE raw_offers SET processed=1 WHERE id=?", (row["id"],))
            batch += 1
            if batch % 500 == 0:
                con.commit()
                cur.execute("BEGIN;")
        con.commit()
        print(f"Processed {batch} raw rows into canonical products/offers.")
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()

if __name__ == "__main__":
    print(f"DB: {DB_PATH}")
    main()
