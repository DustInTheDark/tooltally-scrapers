# scripts/resolver.py
"""
Resolver / Canonicalizer for ToolTally

- Reads unprocessed rows from raw_offers
- Normalizes into vendors, products, offers
- Matching priority:
    1) EAN/GTIN (exact)
    2) MPN (normalized)
    3) Fuzzy Name (brand+model+voltage+category; SequenceMatcher >= threshold)
    4) Rich power-tool fingerprint: brand+model+voltage+kit+bundle+charger+case+category
    5) Hand-tool fingerprint: brand + base_category + sizes/subtypes
    6) Vendor SKU fingerprint
    7) Title-token fingerprint
- Product upsert is COLLISION-SAFE even with a composite UNIQUE index.
- Offer upsert honors UNIQUE(url) and reassigns to the canonical product.
"""

from __future__ import annotations
import os
import re
import sqlite3
from datetime import datetime, timezone
from urllib.parse import urlparse
from difflib import SequenceMatcher

# --------------------------------------------------------------------------------------
# DB path
# --------------------------------------------------------------------------------------

DB_PATH = os.environ.get("DB_PATH") or os.path.join(os.path.dirname(__file__), "..", "data", "tooltally.db")
DB_PATH = os.path.abspath(DB_PATH)

# --------------------------------------------------------------------------------------
# Utilities
# --------------------------------------------------------------------------------------

def dict_factory(cursor, row):
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def norm_space(s: str) -> str:
    return re.sub(r"[\s/_-]+", " ", (s or "")).strip()

def norm_lower(s: str) -> str:
    return norm_space(s).lower()

def normalise_mpn(mpn: str | None) -> str | None:
    if not mpn:
        return None
    return re.sub(r"[\s\-]", "", mpn).lower()

def _row_id(rec):
    if rec is None:
        return None
    if isinstance(rec, dict):
        return rec.get("id")
    return rec[0] if len(rec) > 0 else None

# --------------------------------------------------------------------------------------
# Extractors & fingerprint building
# --------------------------------------------------------------------------------------

BRANDS = r"(makita|dewalt|bosch|milwaukee|einhell|ryobi|black\+?decker|hikoki|stanley|metabo|titan|parkside|festool|fein)"
MODEL_PATTERNS = [
    r"\b([A-Z]{1,5}\d{1,4}[A-Z]?(?:-[A-Z0-9]+)*)\b",   # DHP453Z, M12BIW12-0, DCD709, GSB18V-55
    r"\b([A-Z]{2,4}\d{2,4})\b",                        # fallback
    r"\b(\d{3,4}[A-Z]{1,2})\b",                        # fallback
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
BATTERY_BUNDLE = re.compile(r"\b(\d+)\s*x\s*(\d+(?:\.\d+)?)\s*ah\b", re.I)
CHARGER_TOKEN  = re.compile(r"\b(charger|fast charger|dc\d{2,})\b", re.I)
CASE_TOKEN     = re.compile(r"\b(case|makpac|t-stak|tstak|stack|box|bag)\b", re.I)

SUBTOKENS = [
    # hammers
    "claw", "framing", "ball", "ball-pein", "ball pein", "club", "lump", "sledge", "deadblow", "dead-blow",
    # saws
    "handsaw", "panel", "tenon", "rip", "bow", "hacksaw", "junior", "coping",
]
CAT_MAP = {
    "hammer": "hammer", "hammers": "hammer",
    "saw": "saw", "saws": "saw", "hand saw": "saw", "handsaw": "saw", "hacksaw": "saw",
    "drill": "drill", "drills": "drill", "drill driver": "drill", "drill drivers": "drill",
}
SIZE_PATTERNS = [
    (r"\b(\d+)\s*oz\b",  lambda m: f"oz{m.group(1)}"),
    (r"\b(\d+)\s*g\b",   lambda m: f"g{m.group(1)}"),
    (r"\b(\d+)\s*mm\b",  lambda m: f"mm{m.group(1)}"),
    (r"\b(\d+)\s*cm\b",  lambda m: f"cm{m.group(1)}"),
    (r"\b(\d+)\s*in(ch)?\b", lambda m: f"in{m.group(1)}"),
    (r"\b(\d+)\s*tpi\b", lambda m: f"tpi{m.group(1)}"),
]
STOPWORDS = {"unit","bare","body","only","kit","set","refurb","electric","corded","cordless","combi",
             "plus","brushless","li-ion","lithium","max","with","and","the","tool"}

# ---- basic extractors ----

def base_category(cat: str) -> str:
    c = norm_lower(cat)
    return CAT_MAP.get(c, CAT_MAP.get(c.rstrip("s"), c))

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
    g = m.group(1).lower().replace("max", "").replace("v", "").replace("10.8", "12")
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

def extract_battery_bundle(title: str) -> str:
    t = (title or "").lower()
    m = BATTERY_BUNDLE.search(t)
    if not m:
        return ""
    count, cap = m.group(1), m.group(2)
    cap = cap.rstrip("0").rstrip(".") if "." in cap else cap
    return f"{count}x{cap}Ah"

def has_charger(title: str) -> bool:
    return bool(CHARGER_TOKEN.search(title or ""))

def has_case(title: str) -> bool:
    return bool(CASE_TOKEN.search(title or ""))

def extract_sizes(title: str) -> list[str]:
    t = norm_lower(title)
    out = set()
    for pat, tokey in SIZE_PATTERNS:
        for m in re.finditer(pat, t):
            out.add(tokey(m))
    return sorted(out)

def extract_subtokens(title: str) -> list[str]:
    t = norm_lower(title)
    found = set()
    for tok in SUBTOKENS:
        if re.search(rf"\b{re.escape(tok)}\b", t):
            found.add(tok.replace(" ", "-"))
    return sorted(found)

# ---- name signature & fingerprints ----

def name_signature(title: str, category: str) -> str:
    """Compact, comparable signature for fuzzy match: brand + model + volt + base_category."""
    brand = extract_brand(title)
    model = extract_model(title)
    volt  = extract_voltage(title)
    cat   = base_category(category or "")
    parts = [p for p in [brand, model, str(volt or ""), cat] if p]
    return " ".join(parts).lower()

def build_fingerprint(title: str, category: str, vendor_sku: str | None,
                      ean_gtin: str | None, mpn: str | None) -> str:
    # 1) EAN/GTIN
    if ean_gtin:
        return f"ean:{ean_gtin}"

    # 2) MPN (normalized)
    nmpn = normalise_mpn(mpn)
    if nmpn:
        return f"mpn:{nmpn}"

    # ---- MODEL-LEVEL CANONICALISATION (the important change) ----
    # Power tools: canonicalise on brand + model + voltage only.
    brand = extract_brand(title)
    model = extract_model(title)
    volt  = extract_voltage(title)
    if brand or model or volt:
        b = (brand or "").lower()
        m = (model or "").upper()   # models are usually upper-case codes
        v = str(volt or "")
        if b or m or v:
            return f"model:{b}|{m}|{v}"

    # Hand tools: keep previous behavior (brand + base_category + sizes/subtypes)
    cat = base_category(category or "")
    sizes = extract_sizes(title)
    subs  = extract_subtokens(title)
    if brand or sizes or subs or cat:
        parts = [brand, cat] + sizes + subs
        key   = " | ".join([p for p in parts if p])
        if key:
            return key

    # Vendor SKU
    if vendor_sku:
        return f"sku:{norm_lower(vendor_sku)}"

    # Title tokens fallback
    tokens = [t for t in re.findall(r"[a-z0-9]+", norm_lower(title)) if t not in STOPWORDS]
    key = " ".join(tokens[:8])
    return f"title:{key}" if key else ""

# --------------------------------------------------------------------------------------
# DB helpers (vendors, products, offers)
# --------------------------------------------------------------------------------------

def ensure_vendor_id(cur, row) -> int | None:
    """
    Resolve vendor by name first (preferred), else by URL domain. Create if missing.
    """
    vname = norm_space(row.get("vendor") or row.get("vendor_name") or "")
    if vname:
        cur.execute("SELECT id FROM vendors WHERE lower(name)=lower(?)", (vname,))
        rid = _row_id(cur.fetchone())
        if rid:
            return rid

    url = row.get("url") or ""
    domain = ""
    try:
        domain = urlparse(url).netloc.lower()
    except Exception:
        pass

    if domain:
        cur.execute("SELECT id FROM vendors WHERE lower(domain)=?", (domain,))
        rid = _row_id(cur.fetchone())
        if rid:
            if vname:
                cur.execute("UPDATE vendors SET name = COALESCE(name, ?) WHERE id = ?", (vname, rid))
            return rid

    if vname or domain:
        cur.execute("INSERT INTO vendors(name, domain) VALUES(?, ?)", (vname or None, domain or None))
        return cur.lastrowid

    return None

def normalize_existing_fingerprints(cur):
    """Lower/trim all existing fingerprints to avoid future collisions."""
    cur.execute("UPDATE products SET fingerprint = lower(trim(fingerprint)) WHERE fingerprint IS NOT NULL;")

def try_update_product_fields(cur, pid: int, *, name, category, brand, model, voltage, kit, ean_gtin):
    """
    Try to backfill product fields. If a UNIQUE constraint would be violated
    (e.g., composite unique on name/category), fall back to updating only
    non-unique fields to avoid crashing.
    """
    try:
        cur.execute("""
            UPDATE products
               SET name      = COALESCE(name, ?),
                   category  = COALESCE(category, ?),
                   brand     = COALESCE(brand, ?),
                   model     = COALESCE(model, ?),
                   voltage   = COALESCE(voltage, ?),
                   kit       = COALESCE(kit, ?),
                   ean_gtin  = COALESCE(ean_gtin, ?)
             WHERE id = ?
        """, (name, category, brand, model, voltage, kit, ean_gtin, pid))
    except sqlite3.IntegrityError:
        # Retry a narrower update that cannot violate typical unique(name,category)
        cur.execute("""
            UPDATE products
               SET brand     = COALESCE(brand, ?),
                   model     = COALESCE(model, ?),
                   voltage   = COALESCE(voltage, ?),
                   kit       = COALESCE(kit, ?),
                   ean_gtin  = COALESCE(ean_gtin, ?)
             WHERE id = ?
        """, (brand, model, voltage, kit, ean_gtin, pid))

def select_product_id_by_name_category(cur, name, category):
    if not name or not category:
        return None
    cur.execute("""
        SELECT id FROM products
        WHERE lower(trim(name)) = lower(trim(?))
          AND lower(trim(category)) = lower(trim(?))
        LIMIT 1
    """, (name, category))
    return _row_id(cur.fetchone())

# ---------- fuzzy matching helpers ----------

def product_min_price(cur, product_id: int) -> float | None:
    cur.execute("SELECT MIN(price_pounds) as min_price FROM offers WHERE product_id = ?", (product_id,))
    r = cur.fetchone()
    if not r:
        return None
    return r.get("min_price")

def candidate_rows_for_fuzzy(cur, brand: str, model: str, cat: str, name_like: str, limit: int = 160):
    params = []
    where = []

    # Prefer same category but don't require it — we’ll score via SequenceMatcher
    if brand:
        where.append("(lower(brand) = lower(?))")
        params.append(brand)
    if model:
        where.append("(upper(model) = upper(?))")
        params.append(model)
        where.append("(lower(name) LIKE lower(?))")
        params.append(f"%{model.lower()}%")
    elif name_like:
        where.append("(lower(name) LIKE lower(?))")
        params.append(f"%{name_like.lower()}%")

    sql = f"""
        SELECT id, name, category, brand, model
        FROM products
        {"WHERE " + " AND ".join(where) if where else ""}
        LIMIT {int(limit)}
    """
    cur.execute(sql, tuple(params))
    return cur.fetchall()

def fuzzy_match_product(cur, title: str, category: str, price: float, ratio_threshold: float = 0.90):
    """Return product_id if a strong fuzzy match is found; else None."""
    sig = name_signature(title, category)
    if not sig:
        return None

    brand = extract_brand(title)
    model = extract_model(title)
    cat   = base_category(category or "")
    like_hint = model or brand or ""  # hint for LIKE when we have very little

    candidates = candidate_rows_for_fuzzy(cur, brand, model, cat, like_hint, limit=120)
    if not candidates:
        return None

    best_id = None
    best_ratio = 0.0

    for c in candidates:
        c_sig = name_signature(c.get("name") or "", c.get("category") or "")
        if not c_sig:
            continue
        r = SequenceMatcher(None, sig, c_sig).ratio()
        if r > best_ratio:
            best_ratio = r
            best_id = c["id"]

    if best_id is None or best_ratio < ratio_threshold:
        return None

    # sanity gate: price ratio check to avoid merging bare units with big kits etc.
    min_p = product_min_price(cur, best_id)
    if min_p is None:
        return best_id
    low, high = min(price, min_p), max(price, min_p)
    ratio = (low / high) if high > 0 else 1.0
    if ratio < 0.20:  # too far apart, likely different bundles
        return None

    return best_id

# --------------------------------------------------------------------------------------
# Upserts (collision-safe)
# --------------------------------------------------------------------------------------

def upsert_product(cur, *, name, category, fingerprint,
                   brand=None, model=None, voltage=None, kit=None, ean_gtin=None) -> int:
    """
    Collision-safe upsert by fingerprint; also consult (name,category).
    - Normalize `fingerprint` to lower(trim()).
    - If a row exists (by fingerprint or by name/category), update fields safely.
    - Else, INSERT OR IGNORE, then SELECT id.
    """
    fprint = (fingerprint or "").strip().lower() or None

    # 1) Existing by fingerprint?
    if fprint:
        cur.execute("SELECT id FROM products WHERE lower(trim(fingerprint)) = ?", (fprint,))
        pid = _row_id(cur.fetchone())
        if pid:
            try_update_product_fields(cur, pid,
                                      name=name, category=category,
                                      brand=brand, model=model, voltage=voltage,
                                      kit=kit, ean_gtin=ean_gtin)
            return pid

    # 2) Existing by (name, category)?
    pid2 = select_product_id_by_name_category(cur, name, category)
    if pid2:
        if fprint:
            try:
                cur.execute("""
                    UPDATE products
                       SET fingerprint = COALESCE(fingerprint, ?)
                     WHERE id = ?
                """, (fprint, pid2))
            except sqlite3.IntegrityError:
                pass
        try_update_product_fields(cur, pid2,
                                  name=name, category=category,
                                  brand=brand, model=model, voltage=voltage,
                                  kit=kit, ean_gtin=ean_gtin)
        return pid2

    # 3) Insert new canonical row (ignore duplicates)
    cur.execute("""
        INSERT OR IGNORE INTO products(name, category, fingerprint, brand, model, voltage, kit, ean_gtin)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?)
    """, (name, category, fprint, brand, model, voltage, kit, ean_gtin))

    if cur.lastrowid:
        return cur.lastrowid

    # 4) If insert ignored, fetch existing by fingerprint or name/category
    if fprint:
        cur.execute("SELECT id FROM products WHERE lower(trim(fingerprint)) = ?", (fprint,))
        pid3 = _row_id(cur.fetchone())
        if pid3:
            try_update_product_fields(cur, pid3,
                                      name=name, category=category,
                                      brand=brand, model=model, voltage=voltage,
                                      kit=kit, ean_gtin=ean_gtin)
            return pid3

    pid4 = select_product_id_by_name_category(cur, name, category)
    if pid4:
        try_update_product_fields(cur, pid4,
                                  name=name, category=category,
                                  brand=brand, model=model, voltage=voltage,
                                  kit=kit, ean_gtin=ean_gtin)
        return pid4

    # 5) Last-chance insert (very rare)
    cur.execute("""
        INSERT INTO products(name, category, fingerprint, brand, model, voltage, kit, ean_gtin)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?)
    """, (name, category, fprint, brand, model, voltage, kit, ean_gtin))
    return cur.lastrowid

def insert_or_replace_offer(cur, *, product_id, vendor_id, price, url, vendor_sku, scraped_at):
    """
    Offer upsert honoring UNIQUE(url):
      - If URL exists → reassign/overwrite that row.
      - Else update the latest existing offer for (product,vendor).
      - Else insert new.
    """
    # 1) by URL
    cur.execute("SELECT id FROM offers WHERE url = ?", (url,))
    oid = _row_id(cur.fetchone())
    if oid:
        cur.execute("""
            UPDATE offers
               SET product_id   = ?,
                   vendor_id    = ?,
                   price_pounds = ?,
                   vendor_sku   = ?,
                   scraped_at   = ?
             WHERE id = ?
        """, (product_id, vendor_id, price, vendor_sku, scraped_at, oid))
        return

    # 2) latest by (product, vendor)
    cur.execute("""
        SELECT id FROM offers
         WHERE product_id = ? AND vendor_id = ?
         ORDER BY datetime(scraped_at) DESC, id DESC
         LIMIT 1
    """, (product_id, vendor_id))
    oid = _row_id(cur.fetchone())
    if oid:
        cur.execute("""
            UPDATE offers
               SET price_pounds = ?,
                   url = ?,
                   vendor_sku = ?,
                   scraped_at = ?
             WHERE id = ?
        """, (price, url, vendor_sku, scraped_at, oid))
        return

    # 3) insert new
    cur.execute("""
        INSERT INTO offers(product_id, vendor_id, price_pounds, url, vendor_sku, scraped_at, created_at)
        VALUES(?, ?, ?, ?, ?, ?, ?)
    """, (product_id, vendor_id, price, url, vendor_sku, scraped_at, now_utc_iso()))

# --------------------------------------------------------------------------------------
# Main processing
# --------------------------------------------------------------------------------------

def main():
    print(f"DB: {DB_PATH}")
    con = sqlite3.connect(DB_PATH)
    con.row_factory = dict_factory
    cur = con.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA foreign_keys=ON;")

    # Normalize legacy fingerprints to avoid collisions
    normalize_existing_fingerprints(cur)
    con.commit()

    # Load unprocessed rows
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

    processed = 0
    try:
        cur.execute("BEGIN;")
        for row in raw_rows:
            title       = row.get("title") or row.get("name") or ""
            category    = row.get("category") or row.get("category_name") or ""
            price       = row.get("price_pounds") or row.get("price") or None
            url         = row.get("url") or row.get("buy_url") or None
            vendor_sku  = row.get("vendor_sku") or row.get("sku") or None
            ean_gtin    = (row.get("ean_gtin") or "").strip() or None
            mpn         = (row.get("mpn") or "").strip() or None
            scraped_at  = row.get("scraped_at") or now_utc_iso()

            try:
                price = float(price) if price is not None else None
            except Exception:
                price = None

            if price is None or not url:
                if "id" in row:
                    cur.execute("UPDATE raw_offers SET processed=1 WHERE id=?", (row["id"],))
                continue

            vendor_id = ensure_vendor_id(cur, row)
            if not vendor_id:
                if "id" in row:
                    cur.execute("UPDATE raw_offers SET processed=1 WHERE id=?", (row["id"],))
                continue

            # --- matching priority ---
            # 1) EAN/GTIN or 2) MPN via fingerprints/upsert
            pid = None
            if ean_gtin or mpn:
                fingerprint = build_fingerprint(title, category, vendor_sku, ean_gtin, mpn)
                brand   = extract_brand(title) or None
                model   = extract_model(title) or None
                voltage = extract_voltage(title)
                kit     = extract_kit(title) or None

                pid = upsert_product(
                    cur,
                    name=norm_space(title) or None,
                    category=norm_space(category) or None,
                    fingerprint=fingerprint,
                    brand=brand,
                    model=model,
                    voltage=voltage,
                    kit=kit,
                    ean_gtin=ean_gtin or None,
                )
            else:
                # 3) Fuzzy name match BEFORE inserting a new product
                p_fuzzy = fuzzy_match_product(cur, title, category, price, ratio_threshold=0.90)
                if p_fuzzy:
                    brand   = extract_brand(title) or None
                    model   = extract_model(title) or None
                    voltage = extract_voltage(title)
                    kit     = extract_kit(title) or None
                    # Backfill non-unique fields on the existing candidate
                    try_update_product_fields(cur, p_fuzzy,
                                              name=norm_space(title) or None,
                                              category=norm_space(category) or None,
                                              brand=brand, model=model, voltage=voltage, kit=kit,
                                              ean_gtin=ean_gtin or None)
                    pid = p_fuzzy

            # 4) If still not found, fall back to deterministic fingerprint upsert
            if not pid:
                fingerprint = build_fingerprint(title, category, vendor_sku, ean_gtin, mpn)
                brand   = extract_brand(title) or None
                model   = extract_model(title) or None
                voltage = extract_voltage(title)
                kit     = extract_kit(title) or None

                pid = upsert_product(
                    cur,
                    name=norm_space(title) or None,
                    category=norm_space(category) or None,
                    fingerprint=fingerprint,
                    brand=brand,
                    model=model,
                    voltage=voltage,
                    kit=kit,
                    ean_gtin=ean_gtin or None,
                )

            # Offers upsert
            insert_or_replace_offer(
                cur,
                product_id=pid,
                vendor_id=vendor_id,
                price=price,
                url=url,
                vendor_sku=vendor_sku,
                scraped_at=scraped_at,
            )

            # mark processed
            if "id" in row:
                cur.execute("UPDATE raw_offers SET processed=1 WHERE id=?", (row["id"],))

            processed += 1
            if processed % 500 == 0:
                con.commit()
                cur.execute("BEGIN;")

        con.commit()
        print(f"Processed {processed} raw rows into canonical products/offers.")
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()

if __name__ == "__main__":
    main()
