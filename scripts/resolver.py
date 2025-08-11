# scripts/resolver.py
import hashlib
import json
import re
import sqlite3
from datetime import datetime
from typing import Optional, Tuple

# Try to use your project's connection helper if it exists.
try:
    from data.db import get_conn  # type: ignore
except Exception:
    # Fallback: local get_conn using data/tooltally.db
    import os
    def get_conn():
        db_path = os.environ.get(
            "DB_PATH",
            os.path.join(os.path.dirname(__file__), "..", "data", "tooltally.db")
        )
        conn = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn

# --------- heuristics / parsing helpers ---------

KNOWN_BRANDS = [
    "Bosch", "Makita", "DeWalt", "BLACK + DECKER", "Black & Decker", "Batavia",
    "Milwaukee", "Ryobi", "Einhell", "Hitachi", "Hikoki", "Metabo", "Stanley"
]

MPN_PATTERNS = [
    r"\b0?\d{2}[A-Z0-9]{1,}[A-Z0-9-]{3,}\b",   # Bosch-like: 06019H4000 etc.
    r"\b[A-Z]{2,}\d[A-Z0-9-]{2,}\b",           # DCD796P1, GSB18V55, etc.
    r"\b[0-9]{6,}[A-Z0-9-]*\b",                # numeric-heavy SKUs
]

EAN_PATTERNS = [r"\b\d{13}\b"]

def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def detect_brand(title: str) -> Optional[str]:
    t = title.lower()
    for b in KNOWN_BRANDS:
        if b.lower() in t:
            return b
    return None

def find_first(patterns, text) -> Optional[str]:
    for pat in patterns:
        m = re.search(pat, text, re.I)
        if m:
            return m.group(0)
    return None

def parse_voltage(title: str) -> Optional[str]:
    m = re.search(r"\b(10\.8|12|14\.4|18|20|24|36|40)\s*v\b", title, re.I)
    return (m.group(1) + "V") if m else None

def parse_battery_info(title: str) -> Tuple[bool, Optional[int], Optional[float]]:
    t = title.lower()
    if "bare" in t or "body only" in t or "bare unit" in t:
        return (False, None, None)
    qty = None
    cap = None
    m_qty = re.search(r"\b(\d+)\s*x\b", t)
    if m_qty:
        qty = int(m_qty.group(1))
    m_cap = re.search(r"\b(\d+(?:\.\d+)?)\s*ah\b", t)
    if m_cap:
        cap = float(m_cap.group(1))
    includes = ("battery" in t or "batteries" in t or "ah" in t) and "bare" not in t
    return (includes, qty, cap)

def variant_signature_from_title(title: str) -> str:
    vol = parse_voltage(title) or ""
    inc, qty, cap = parse_battery_info(title)
    if not inc:
        variant = "bare"
    elif qty and cap:
        variant = f"kit-{qty}x{cap}Ah"
    elif qty:
        variant = f"kit-{qty}x"
    elif cap:
        variant = f"kit-?x{cap}Ah"
    else:
        variant = "kit"
    parts = [p for p in [vol, variant] if p]
    return "|".join(parts) if parts else "base"

def normalized_key(ean: Optional[str], brand: Optional[str], mpn: Optional[str], variant_sig: str) -> str:
    if ean:
        return f"ean:{ean}"
    basis = norm_space(f"{brand or ''}|{mpn or ''}|{variant_sig or 'base'}").lower()
    return "sig:" + hashlib.sha1(basis.encode("utf-8")).hexdigest()

# --------- upserts ---------

def upsert_vendor(conn, name: str) -> int:
    name = norm_space(name)
    cur = conn.execute("SELECT id FROM vendors WHERE name = ?;", (name,))
    row = cur.fetchone()
    if row:
        return row["id"]
    conn.execute("INSERT INTO vendors (name) VALUES (?);", (name,))
    return conn.execute("SELECT last_insert_rowid() AS id;").fetchone()["id"]

def upsert_category(conn, name: Optional[str]) -> Optional[int]:
    if not name:
        return None
    name = norm_space(name)
    row = conn.execute("SELECT id FROM categories WHERE name = ?;", (name,)).fetchone()
    if row:
        return row["id"]
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    conn.execute("INSERT INTO categories (name, slug) VALUES (?, ?);", (name, slug))
    return conn.execute("SELECT last_insert_rowid() AS id;").fetchone()["id"]

def upsert_product(conn, brand: str, mpn: Optional[str], ean: Optional[str],
                   name: str, category_id: Optional[int], variant_sig: str,
                   normalized: str, specs: dict) -> int:
    row = conn.execute("SELECT id FROM products WHERE normalized_key = ?;", (normalized,)).fetchone()
    if row:
        pid = row["id"]
        conn.execute(
            """UPDATE products
               SET brand=?, mpn=?, ean=COALESCE(?, ean), name=?, category_id=?, variant_signature=?,
                   specs_json=?, updated_at=CURRENT_TIMESTAMP
               WHERE id=?;""",
            (brand, mpn, ean, name, category_id, variant_sig, json.dumps(specs or {}), pid)
        )
        return pid

    if ean:
        row = conn.execute("SELECT id FROM products WHERE ean = ?;", (ean,)).fetchone()
        if row:
            pid = row["id"]
            conn.execute(
                """UPDATE products
                   SET normalized_key=?, brand=?, mpn=?, name=?, category_id=?, variant_signature=?,
                       specs_json=?, updated_at=CURRENT_TIMESTAMP
                   WHERE id=?;""",
                (normalized, brand, mpn, name, category_id, variant_sig, json.dumps(specs or {}), pid)
            )
            return pid

    conn.execute(
        """INSERT INTO products
           (brand, mpn, ean, name, category_id, variant_signature, normalized_key, specs_json)
           VALUES (?,?,?,?,?,?,?,?);""",
        (brand, mpn, ean, name, category_id, variant_sig, normalized, json.dumps(specs or {}))
    )
    return conn.execute("SELECT last_insert_rowid() AS id;").fetchone()["id"]

def upsert_offer(conn, product_id: int, vendor_id: int, vendor_sku: Optional[str],
                 price_cents: int, currency: str, buy_url: str,
                 in_stock: Optional[int], shipping_cents: Optional[int],
                 scraped_at: str):
    if vendor_sku:
        conn.execute(
            """
            INSERT INTO offers (product_id, vendor_id, vendor_sku, price_cents, currency, buy_url, in_stock, shipping_cents, scraped_at)
            VALUES (?,?,?,?,?,?,?,?,?)
            ON CONFLICT(vendor_id, vendor_sku) DO UPDATE SET
              product_id=excluded.product_id,
              price_cents=excluded.price_cents,
              currency=excluded.currency,
              buy_url=excluded.buy_url,
              in_stock=excluded.in_stock,
              shipping_cents=excluded.shipping_cents,
              scraped_at=excluded.scraped_at;
            """,
            (product_id, vendor_id, vendor_sku, price_cents, currency, buy_url, in_stock, shipping_cents, scraped_at)
        )
    else:
        # Fallback uniqueness by (vendor_id, buy_url)
        row = conn.execute("SELECT id FROM offers WHERE vendor_id=? AND buy_url=?;", (vendor_id, buy_url)).fetchone()
        if row:
            conn.execute(
                """UPDATE offers
                   SET product_id=?, price_cents=?, currency=?, in_stock=?, shipping_cents=?, scraped_at=?
                   WHERE id=?;""",
                (product_id, price_cents, currency, in_stock, shipping_cents, scraped_at, row["id"])
            )
        else:
            conn.execute(
                """INSERT INTO offers
                   (product_id, vendor_id, vendor_sku, price_cents, currency, buy_url, in_stock, shipping_cents, scraped_at)
                   VALUES (?,?,?,?,?,?,?,?,?);""",
                (product_id, vendor_id, vendor_sku, price_cents, currency, buy_url, in_stock, shipping_cents, scraped_at)
            )

# --------- main resolution pipeline ---------

def resolve_one(conn, ro):
    raw_title = ro["raw_title"]
    vendor = ro["vendor"]
    price_cents = int(ro["price_cents"])
    currency = ro["currency"] or "GBP"
    buy_url = ro["buy_url"]
    vendor_sku = ro["vendor_sku"]
    category_name = ro["category_name"]
    scraped_at = ro["scraped_at"] or datetime.utcnow().isoformat()

    brand = detect_brand(raw_title) or "Unknown"
    ean = find_first(EAN_PATTERNS, raw_title)
    mpn = find_first(MPN_PATTERNS, raw_title)
    variant_sig = variant_signature_from_title(raw_title)

    cat_id = upsert_category(conn, category_name)
    nkey = normalized_key(ean, brand, mpn, variant_sig)

    specs = {
        "voltage": parse_voltage(raw_title),
        "battery_variant": variant_sig,
    }

    product_id = upsert_product(
        conn,
        brand=brand,
        mpn=mpn,
        ean=ean,
        name=norm_space(raw_title),  # you can later replace with a cleaner canonical title
        category_id=cat_id,
        variant_sig=variant_sig,
        normalized=nkey,
        specs=specs
    )

    vendor_id = upsert_vendor(conn, vendor)
    upsert_offer(
        conn,
        product_id=product_id,
        vendor_id=vendor_id,
        vendor_sku=vendor_sku,
        price_cents=price_cents,
        currency=currency,
        buy_url=buy_url,
        in_stock=None,
        shipping_cents=None,
        scraped_at=scraped_at
    )

    # Alias for auditability / future fuzzy matches
    conn.execute(
        "INSERT OR IGNORE INTO product_aliases (product_id, alias_name, source_vendor_id, confidence) VALUES (?,?,?,?);",
        (product_id, raw_title, vendor_id, 0.6 if (mpn or ean) else 0.4)
    )

    conn.execute(
        "UPDATE raw_offers SET processed=1, resolved_product_id=? WHERE id=?;",
        (product_id, ro["id"])
    )

def process_unresolved(batch_size: int = 2000) -> int:
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM raw_offers WHERE processed=0 ORDER BY id LIMIT ?;",
            (batch_size,)
        ).fetchall()
        for ro in rows:
            resolve_one(conn, ro)
        conn.commit()
        return len(rows)
    finally:
        conn.close()

if __name__ == "__main__":
    n = process_unresolved(5000)
    print(f"Resolved {n} raw rows.")
