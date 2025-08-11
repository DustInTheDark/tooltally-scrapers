# scripts/resolver.py
"""
Resolver v2.1: collapse vendor listings in raw_offers into canonical products (+ offers)
using (brand, model/MPN, voltage, kit_signature) as the product key.

Upgrades vs v2.0:
- Brand-specific model extraction (Makita/DeWalt/Bosch/Milwaukee/Einhell/Ryobi/B+D).
- Brand-specific kit inference (e.g., Makita 'Z' bare, DeWalt 'N' bare, Ryobi '-0' bare).
- Voltage inference from model (e.g., Milwaukee M12/M18).
- Better name composition (less 'XCHANGE' / 'DRILL18V' noise).

No schema changes. Idempotent: clears products/offers and rebuilds them from raw_offers.
"""

from __future__ import annotations

import os
import re
import sys
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

DB_PATH = os.environ.get("DB_PATH", os.path.join("data", "tooltally.db"))

# ---------------- Brand normalization ----------------

BRAND_ALIASES = {
    "DeWalt": ["dewalt", "de-walt", "de walt", "d e w a l t"],
    "Bosch": ["bosch", "bosch professional"],
    "Makita": ["makita"],
    "Milwaukee": ["milwaukee", "milwaukee tool"],
    "Black+Decker": ["black & decker", "black + decker", "black and decker", "b+d", "b & d", "black+decker"],
    "Ryobi": ["ryobi"],
    "Einhell": ["einhell", "power x-change", "power x change", "x-change", "x change"],
    "Metabo": ["metabo"],
    "HiKOKI": ["hikoki", "hitachi hikoki", "hitachi-koki", "hitachi koki", "hitachi"],
    "Draper": ["draper"],
    "Batavia": ["batavia"],
    "Erbauer": ["erbauer"],
    "Stanley": ["stanley"],
    "Irwin": ["irwin"],
    "Bahco": ["bahco"],
}

BRAND_LUT = []
for canon, alts in BRAND_ALIASES.items():
    BRAND_LUT.append((canon, re.compile(r"\b(" + r"|".join(map(re.escape, [canon] + alts)) + r")\b", re.I)))

# ---------------- Regexes ----------------

# Generic helpful bits
RE_VOLT = re.compile(r"\b(\d{2,3})\s*V\b", re.I)  # 12V / 18V / 36V / 40V / 54V

# Kit/bare signatures (generic)
RE_BARE = re.compile(r"\b(bare unit|body only|tool only|solo|naked)\b", re.I)
RE_CHARGER = re.compile(r"\bcharger\b", re.I)
RE_BATT_COUNT_AH = re.compile(r"\b(\d+)\s*[xX]\s*(\d+(?:\.\d+)?)\s*Ah\b", re.I)
RE_BATT_AH = re.compile(r"\b(\d+(?:\.\d+)?)\s*Ah\b", re.I)
RE_KIT_WORD = re.compile(r"\b(kit|set|bundle|twin pack|combo)\b", re.I)

# Noise to strip in categories / friendly names
RE_NOISE = re.compile(
    r"\b(bare unit|body only|tool only|solo|naked|with|w/|inc\.?|includes?|and|plus|case|l-?boxx|bag|"
    r"batter(y|ies)|fast charger|charger|accessor(y|ies)|bit set|drill bit set|"
    r"brushless|cordless|li-?ion|professional|amp(?:share)?|coolpack|procore18v|fuel)\b",
    re.I,
)

# Vendor-agnostic model patterns
RE_BOSCH_CODE = re.compile(r"\b0[0-9]{5}[A-Z0-9]{4,6}\b", re.I)  # 06019G030A / 06019J5101
RE_G_SERIES = re.compile(r"\bG[BSRDXH][A-Z]?\s*(?:\d{2}\s*V|\d+V)[-\s]?\d{1,3}[A-Z]?\b", re.I)  # GSB 18V-55, GSR 12V-15
RE_ALNUM_BLOCK = re.compile(r"\b[A-Z]{2,5}\d{2,4}[A-Z]{0,3}\b")  # DCD796N / DTD155Z etc.

# Brand-specific model patterns
RE_MAKITA = re.compile(r"\bD[ABCDGHILMPRSTVWXZ]{1,2}[A-Z]\d{3}[A-Z]{0,2}\b", re.I)  # DHP482Z, DTD155Z, DHR202Z
RE_DEWALT = re.compile(r"\bD[BCDFHLMPS][A-Z]{1,2}\d{3}[A-Z]?(?:[NP]\d{0,2})?\b", re.I)  # DCD796N, DCF887P2, DCH273
RE_MILW = re.compile(r"\bM(?:12|18)[A-Z]+[A-Z]?\d{0,4}(?:-[0-9A-Z]{1,5})?\b", re.I)  # M12FID2-0, M18FPD2-502X
RE_RYOBI = re.compile(r"\bR\d{2}[A-Z]{2,3}-\d\b", re.I)  # R18PD3-0
RE_EINHELL = re.compile(r"\bT[CE]-[A-Z]{2,3}\s*18(?:[/\-]\d+)?\s*Li(?:-?[\w]+){0,2}\b", re.I)  # TE-CD 18/2 Li, TC-CD 18-2 Li
RE_BD = re.compile(r"\bB[CD][CD]\w{2,4}\d{1,4}[A-Z-]*\b", re.I)  # BDCDD18N, BCD700S2KA

# ---------------- Data classes ----------------

@dataclass(frozen=True)
class ProductKey:
    brand: str
    model_canon: str
    voltage: Optional[str]
    kit_signature: str  # "bare", "batteries_2x5.0Ah", "kit", "tool"

# ---------------- Helpers ----------------

def _norm_brand(title: str) -> Optional[str]:
    for canon, rx in BRAND_LUT:
        if rx.search(title):
            return canon
    return None

def _match_any(*patterns: re.Pattern, text: str) -> Optional[str]:
    for rx in patterns:
        m_all = list(rx.finditer(text))
        if m_all:
            # pick the longest token
            best = max(m_all, key=lambda m: len(m.group(0)))
            return best.group(0)
    return None

def _extract_model_brand_aware(brand: Optional[str], title: str) -> Optional[str]:
    # Order matters: the first successful brand-specific hit wins.
    if brand == "Makita":
        hit = _match_any(RE_MAKITA, RE_ALNUM_BLOCK, text=title)
        if hit: return hit
    elif brand == "DeWalt":
        hit = _match_any(RE_DEWALT, RE_ALNUM_BLOCK, text=title)
        if hit: return hit
    elif brand == "Bosch":
        hit = _match_any(RE_BOSCH_CODE, RE_G_SERIES, RE_ALNUM_BLOCK, text=title)
        if hit: return hit
    elif brand == "Milwaukee":
        hit = _match_any(RE_MILW, RE_ALNUM_BLOCK, text=title)
        if hit: return hit
    elif brand == "Ryobi":
        hit = _match_any(RE_RYOBI, RE_ALNUM_BLOCK, text=title)
        if hit: return hit
    elif brand == "Einhell":
        hit = _match_any(RE_EINHELL, RE_ALNUM_BLOCK, text=title)
        if hit: return hit
    elif brand == "Black+Decker":
        hit = _match_any(RE_BD, RE_ALNUM_BLOCK, text=title)
        if hit: return hit
    # Fallbacks: generic useful patterns
    hit = _match_any(RE_BOSCH_CODE, RE_G_SERIES, RE_ALNUM_BLOCK, text=title)
    if hit: 
        return hit
    return None

def _extract_voltage(title: str) -> Optional[str]:
    m = RE_VOLT.search(title)
    if m:
        try:
            return f"{int(m.group(1))}V"
        except Exception:
            return f"{m.group(1)}V"
    return None

def _infer_voltage_from_model(brand: Optional[str], model_raw: str) -> Optional[str]:
    m = model_raw.upper()
    if brand == "Milwaukee":
        if m.startswith("M12"):
            return "12V"
        if m.startswith("M18"):
            return "18V"
    if brand == "Ryobi":
        # many Ryobi are R18.. = 18V platform
        if m.startswith("R18"):
            return "18V"
    return None

def _brand_specific_kit(brand: Optional[str], model_raw: str) -> Optional[str]:
    m = model_raw.upper()
    # Makita: trailing Z often indicates bare unit (Z == body only)
    if brand == "Makita" and m.endswith("Z"):
        return "bare"
    # DeWalt: trailing N is commonly bare
    if brand == "DeWalt" and m.endswith("N"):
        return "bare"
    # Ryobi: '-0' means bare (no battery/charger)
    if brand == "Ryobi" and re.search(r"-0\b", model_raw):
        return "bare"
    # Milwaukee: '-0' is commonly bare
    if brand == "Milwaukee" and re.search(r"-0\b", model_raw):
        return "bare"
    return None

def _kit_signature(title: str, brand: Optional[str], model_raw: Optional[str]) -> str:
    # Brand-specific hint from model
    if model_raw:
        spec = _brand_specific_kit(brand, model_raw)
        if spec:
            return spec

    # Title-based generic detection
    if RE_BARE.search(title):
        return "bare"
    m = RE_BATT_COUNT_AH.search(title)
    if m:
        count = int(m.group(1))
        ah = m.group(2)
        return f"batteries_{count}x{ah}Ah"
    if RE_BATT_AH.search(title):
        if RE_CHARGER.search(title) or RE_KIT_WORD.search(title):
            ah = RE_BATT_AH.search(title).group(1)
            return f"batteries_1x{ah}Ah"
        return "kit"
    if RE_CHARGER.search(title) or RE_KIT_WORD.search(title):
        return "kit"
    return "tool"

def _canonize_model(model: str) -> str:
    # Remove whitespace/hyphens, uppercase
    return re.sub(r"[\s\-]+", "", model).upper()

def _compose_name(brand: str, model_canon: str, voltage: Optional[str], kit_sig: str) -> str:
    parts = [brand, model_canon]
    if voltage:
        parts.append(voltage)
    suffix = None
    if kit_sig == "bare":
        suffix = "Bare Unit"
    elif kit_sig.startswith("batteries_"):
        tmp = kit_sig[len("batteries_"):]
        try:
            count_str, ah_str = tmp.split("x", 1)
            if not ah_str.lower().endswith("ah"):
                ah_str = f"{ah_str}Ah"
            suffix = f"{count_str} x {ah_str} Kit"
        except Exception:
            suffix = "Kit"
    elif kit_sig == "kit":
        suffix = "Kit"
    name = " ".join(p for p in parts if p)
    if suffix:
        name = f"{name} {suffix}"
    return re.sub(r"\s{2,}", " ", name).strip()

def _clean_for_category(s: str) -> str:
    return RE_NOISE.sub(" ", s).strip()

# ---------------- DB helpers ----------------

def connect_db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con

def ensure_indexes(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS vendors (id INTEGER PRIMARY KEY, name TEXT UNIQUE)")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        category TEXT
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS offers (
        id INTEGER PRIMARY KEY,
        product_id INTEGER NOT NULL,
        vendor_id INTEGER NOT NULL,
        price_pounds REAL NOT NULL,
        url TEXT NOT NULL,
        created_at TEXT,
        FOREIGN KEY(product_id) REFERENCES products(id),
        FOREIGN KEY(vendor_id) REFERENCES vendors(id)
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS raw_offers (
        id INTEGER PRIMARY KEY,
        vendor TEXT,
        title TEXT,
        price_pounds REAL,
        url TEXT,
        vendor_sku TEXT,
        category_name TEXT,
        scraped_at TEXT,
        processed INTEGER DEFAULT 0
    )""")
    # Indexes
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_vendors_name ON vendors(name)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_products_name ON products(name)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_offers_product ON offers(product_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_offers_vendor ON offers(vendor_id)")
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_offers_url ON offers(url)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_raw_offers_vendor ON raw_offers(vendor)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_raw_offers_url ON raw_offers(url)")
    con.commit()
    cur.close()

def get_vendor_id(con: sqlite3.Connection, vendor_name: str) -> int:
    cur = con.cursor()
    cur.execute("INSERT OR IGNORE INTO vendors(name) VALUES (?)", (vendor_name,))
    cur.execute("SELECT id FROM vendors WHERE name = ?", (vendor_name,))
    vid = cur.fetchone()[0]
    cur.close()
    return vid

# ---------------- Main ----------------

def main(dry_run: bool = False) -> None:
    con = connect_db()
    ensure_indexes(con)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    rows = list(con.execute(
        "SELECT id, vendor, COALESCE(title,'') as title, price_pounds, url, vendor_sku, category_name "
        "FROM raw_offers"
    ))

    product_map: Dict[ProductKey, Dict[str, str]] = {}
    grouped_offers: Dict[ProductKey, List[sqlite3.Row]] = {}

    for r in rows:
        title = (r["title"] or "").strip()
        vendor = (r["vendor"] or "").strip()
        url = (r["url"] or "").strip()
        price = r["price_pounds"]

        if not vendor or not url or price is None:
            continue

        brand = _norm_brand(title)
        model_raw = _extract_model_brand_aware(brand, title)  # brand-aware
        voltage = _extract_voltage(title) or (model_raw and _infer_voltage_from_model(brand, model_raw))
        kit_sig = _kit_signature(title, brand, model_raw or "")

        # Fallbacks if still missing brand/model
        if not brand:
            for canon, rx in BRAND_LUT:
                if rx.search(title):
                    brand = canon
                    break

        if not brand:
            brand = vendor  # vendor name as a fence to avoid cross-vendor merges

        if not model_raw:
            # Fallback to a long alnum token; last resort: use URL to avoid wrong merges
            m = re.findall(r"\b[A-Z0-9][A-Z0-9\-]{2,}\b", title.upper())
            model_raw = max(m, key=len) if m else url

        model_canon = _canonize_model(model_raw)
        key = ProductKey(brand=brand, model_canon=model_canon, voltage=voltage, kit_signature=kit_sig)

        category = None
        if r["category_name"]:
            category = _clean_for_category(str(r["category_name"]))

        if key not in product_map:
            product_map[key] = {
                "name": _compose_name(brand, model_canon, voltage, kit_sig),
                "category": category,
            }
            grouped_offers[key] = []
        else:
            if not product_map[key]["category"] and category:
                product_map[key]["category"] = category

        grouped_offers[key].append(r)

    print(f"Resolver: planned {len(product_map)} unique products from {len(rows)} raw rows.")

    if dry_run:
        con.close()
        return

    print("Resolver: clearing previous products/offers …")
    cur.execute("DELETE FROM offers")
    cur.execute("DELETE FROM products")
    con.commit()

    # Vendor cache
    vendor_cache: Dict[str, int] = {}

    def vid_for(vname: str) -> int:
        if vname in vendor_cache:
            return vendor_cache[vname]
        vid = get_vendor_id(con, vname)
        vendor_cache[vname] = vid
        return vid

    # Insert products
    product_id_for: Dict[ProductKey, int] = {}
    for key, meta in product_map.items():
        cur.execute("INSERT INTO products(name, category) VALUES (?, ?)", (meta["name"], meta["category"]))
        product_id_for[key] = cur.lastrowid
    con.commit()

    # Insert offers (dedupe URL within product)
    now = datetime.now(timezone.utc).isoformat()
    inserted = 0
    for key, offs in grouped_offers.items():
        pid = product_id_for[key]
        seen_urls: set[str] = set()
        for r in offs:
            u = r["url"]
            if u in seen_urls:
                continue
            seen_urls.add(u)
            vid = vid_for(r["vendor"])
            try:
                cur.execute(
                    "INSERT OR IGNORE INTO offers(product_id, vendor_id, price_pounds, url, created_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (pid, vid, float(r["price_pounds"]), u, now),
                )
                inserted += cur.rowcount
            except Exception:
                # fallback without IGNORE (e.g., if schema/index differs)
                cur.execute(
                    "INSERT INTO offers(product_id, vendor_id, price_pounds, url, created_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (pid, vid, float(r["price_pounds"]), u, now),
                )
                inserted += 1
    con.commit()

    print(f"Resolver: inserted {len(product_map)} products and {inserted} offers.")
    cur.close()
    con.close()

if __name__ == "__main__":
    dry = "--dry-run" in sys.argv
    main(dry_run=dry)
