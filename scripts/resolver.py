#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Resolver: raw_offers → products + offers (with robust cross-key grouping + safe upsert)

What this does
--------------
1) Loads unprocessed rows from raw_offers.
2) Normalises identifiers and titles; extracts brand/model/voltage/kit.
3) Builds candidate keys per row: EAN, Brand|MPN, Brand|ModelBase|Volt|Kit,
   and a relaxed Brand|ModelBase key (even if voltage is missing).
4) Unions rows that share ANY key (transitively) → merges identifier-only and title-only islands.
5) Safely get-or-create a product per cluster (no UNIQUE-index crashes) and add offers.
6) Marks raw_offers.processed = 1.

Notes
-----
- Works with an existing UNIQUE index on products (e.g., on name or other columns).
- products.fingerprint is kept (unique within this run).
- If an insert is ignored by a UNIQUE index, we detect the existing product and continue.
"""

from __future__ import annotations
import sqlite3
import re
import hashlib
from collections import defaultdict, Counter
from typing import Dict, List, Tuple, Optional

# ----------------------------
# PRAGMA helpers
# ----------------------------
def set_pragmas(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    cur.execute("PRAGMA foreign_keys = ON;")
    cur.execute("PRAGMA journal_mode = WAL;")
    cur.execute("PRAGMA synchronous = NORMAL;")
    cur.execute("PRAGMA temp_store = MEMORY;")
    try:
        cur.execute("PRAGMA mmap_size = 30000000000;")
    except Exception:
        pass
    cur.close()

# ----------------------------
# Normalisation & parsing
# ----------------------------
ALNUM_UPPER = re.compile(r'[^A-Z0-9]')

def norm_mpn(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = ALNUM_UPPER.sub('', s.upper())
    return s or None

def norm_ean(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    digits = re.sub(r'\D', '', s)
    return digits if len(digits) in (8, 12, 13, 14) else None

def norm_voltage(title: str) -> Optional[str]:
    t = title.lower()
    t = t.replace('20v max', '18v').replace('10.8v', '12v')
    m = re.search(r'(\d{2})(?:\.\d)?\s*v', t)
    return f"{m.group(1)}v" if m else None

def kit_signature(title: str, mpn: Optional[str]) -> str:
    t = title.lower()
    m = (mpn or '').upper()
    if any(k in t for k in ('body only','tool only','bare unit','naked')) \
       or any(suf in m for suf in ('-0','Z','N')):
        return 'bare'
    if any(k in t for k in ('makpac','tstak','case','carry case','inlay','box')) \
       and not any(k in t for k in ('battery','batteries','charger')):
        return 'case-only'
    if re.search(r'\b[12]x\s*\d(?:\.\d)?\s*ah\b', t) or 'with battery' in t or 'with charger' in t or ' 1 x ' in t or ' 2 x ' in t:
        return '2-batt kit' if ('2x' in t or ' 2 x ' in t) else 'starter kit'
    return 'unknown'

BRAND_PATTS = [
    ('Makita',    re.compile(r'\b(D[A-Z]{2,3}\d{3}[A-Z0-9]*)\b', re.I)),
    ('DeWalt',    re.compile(r'\b(DC[DFGH]\d{3}[A-Z0-9]*)\b', re.I)),
    ('Bosch',     re.compile(r'\b(G[SB][A-Z0-9 -]*\d{2}[-\w]*)\b', re.I)),
    ('Milwaukee', re.compile(r'\b(M1[28][A-Z0-9-]+)\b', re.I)),
    ('Ryobi',     re.compile(r'\b(R[0-9A-Z-]+)\b', re.I)),
    ('Einhell',   re.compile(r'\b(TE-[A-Z]{2}\w*)\b', re.I)),
    ('Metabo',    re.compile(r'\b(SSD|SSW|SB|SBP|BS|BSB|LTX|LT|PowerMaxx)[-\w]*\b', re.I)),
]
HEAD_BRAND_MAP = {
    'makita': 'Makita',
    'dewalt': 'DeWalt',
    'bosch': 'Bosch',
    'milwaukee': 'Milwaukee',
    'ryobi': 'Ryobi',
    'einhell': 'Einhell',
    'metabo': 'Metabo',
}

def extract_brand_model_base(title: str) -> Tuple[Optional[str], Optional[str]]:
    for brand, patt in BRAND_PATTS:
        m = patt.search(title)
        if m:
            code = re.sub(r'\s+', '', m.group(1).upper())
            base = code
            if brand == 'Makita':
                base = re.sub(r'(Z|J|TJ|RTJ|RJ|RFJ|RMJ|RTE?J|S?J)$', '', base)
            if brand == 'DeWalt':
                base = re.sub(r'(N|NT|P1|P2|PS)$', '', base)
            return brand, base
    head = (title.strip().split() or [''])[0].lower()
    return (HEAD_BRAND_MAP.get(head), None)

CATEGORY_MAP = {
    'drills': 'Drills',
    'combi drill': 'Drills',
    'hammer drill': 'Drills',
    'impact driver': 'Impact Drivers',
    'impact wrench': 'Impact Wrenches',
    'grinder': 'Grinders',
    'angle grinder': 'Grinders',
    'circular saw': 'Saws',
    'jigsaw': 'Saws',
    'reciprocating saw': 'Saws',
    'rotary hammer': 'Rotary Hammers',
    'sds drill': 'Rotary Hammers',
    'multitool': 'Multi-Tools',
}
def canon_category(s: Optional[str], fallback_title: Optional[str] = None) -> Optional[str]:
    if not s and not fallback_title:
        return None
    t = (s or fallback_title or '').strip().lower()
    for k, v in CATEGORY_MAP.items():
        if k in t:
            return v
    if fallback_title and not s:
        ft = fallback_title.lower()
        if 'drill' in ft: return 'Drills'
        if 'driver' in ft and 'impact' in ft: return 'Impact Drivers'
        if 'grinder' in ft: return 'Grinders'
        if 'saw' in ft: return 'Saws'
    return s

# ----------------------------
# Candidate keys & union-find
# ----------------------------
def candidate_keys(row: Dict) -> List[Tuple[str, str]]:
    """
    Emit keys for a raw_offers row:
      - ean:    EAN/GTIN digits
      - mpn:    Brand|MPN (normalized)
      - model:  Brand|ModelBase|Voltage|Kit (strict)
      - modelb: Brand|ModelBase (relaxed, no voltage/kit)
    """
    title = row.get('title') or ''
    brand, model_base = extract_brand_model_base(title)
    ean = norm_ean(row.get('ean_gtin'))
    mpn = norm_mpn(row.get('mpn'))
    volt = norm_voltage(title)
    kit  = kit_signature(title, mpn)

    keys: List[Tuple[str,str]] = []
    if ean:
        keys.append(('ean', ean))
    if mpn and brand:
        keys.append(('mpn', f'{brand}|{mpn}'))
    if brand and model_base and volt:
        keys.append(('model', f'{brand}|{model_base}|{volt}|{kit}'))
    if brand and model_base:
        keys.append(('modelb', f'{brand}|{model_base}'))
    return keys

class DSU:
    def __init__(self, n: int):
        self.p = list(range(n))
        self.r = [0] * n
    def find(self, x: int) -> int:
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x
    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self.r[ra] < self.r[rb]:
            self.p[ra] = rb
        elif self.r[ra] > self.r[rb]:
            self.p[rb] = ra
        else:
            self.p[rb] = ra
            self.r[ra] += 1

def choose_fingerprint(cluster_rows: List[Dict]) -> str:
    """Priority: ean > mpn > model > modelb; include one representative for transparency."""
    eans, mpns, models, modelbs = set(), set(), set(), set()
    for r in cluster_rows:
        for typ, key in candidate_keys(r):
            if   typ == 'ean':    eans.add(key)
            elif typ == 'mpn':    mpns.add(key)
            elif typ == 'model':  models.add(key)
            elif typ == 'modelb': modelbs.add(key)
    if eans:    return f"ean:{sorted(eans)[0]}"
    if mpns:    return f"mpn:{sorted(mpns)[0]}"
    if models:  return f"model:{sorted(models)[0]}"
    if modelbs: return f"modelb:{sorted(modelbs)[0]}"
    return "model:unknown"

def majority_or_first(values: List[Optional[str]]) -> Optional[str]:
    vals = [v for v in values if v]
    if not vals:
        return None
    cnt = Counter(vals)
    return cnt.most_common(1)[0][0]

def cluster_signature(cluster_rows: List[Dict]) -> str:
    ids = sorted(str(r['id']) for r in cluster_rows)
    vds = sorted((r.get('vendor') or '').lower() for r in cluster_rows)
    basis = "|".join(ids) + "||" + "|".join(vds)
    return hashlib.sha1(basis.encode('utf-8')).hexdigest()[:10]

def uniquify_fingerprint(base_fp: str, cluster_rows: List[Dict], used: set[str]) -> str:
    fp = base_fp
    if fp not in used:
        used.add(fp)
        return fp
    sig = cluster_signature(cluster_rows)
    fp2 = f"{base_fp}|c:{sig}"
    if fp2 not in used:
        used.add(fp2)
        return fp2
    i = 2
    while True:
        fpn = f"{base_fp}|dup{i}"
        if fpn not in used:
            used.add(fpn)
            return fpn
        i += 1

# ----------------------------
# DB helpers
# ----------------------------
def get_vendor_id(cur: sqlite3.Cursor, name: str) -> int:
    cur.execute("SELECT id FROM vendors WHERE lower(name)=lower(?)", (name,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO vendors(name) VALUES(?)", (name,))
    return cur.lastrowid

def insert_offer(cur: sqlite3.Cursor,
                 product_id: int,
                 vendor_id: int,
                 price_pounds: Optional[float],
                 url: Optional[str],
                 vendor_sku: Optional[str],
                 scraped_at: Optional[str]) -> None:
    cur.execute("""
        INSERT INTO offers (product_id, vendor_id, price_pounds, url, vendor_sku, scraped_at, created_at)
        VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    """, (product_id, vendor_id, price_pounds, url, vendor_sku, scraped_at))

def build_product_display_name(brand: Optional[str],
                               model: Optional[str],
                               voltage: Optional[str],
                               kit: Optional[str],
                               fallback_title: str) -> str:
    parts = []
    if brand: parts.append(brand)
    if model: parts.append(model)
    if voltage: parts.append(voltage.upper())
    if kit and kit != 'unknown': parts.append(f'({kit})')
    name = " ".join(parts).strip()
    return name if len(name) >= 5 else fallback_title

def get_or_create_product(cur: sqlite3.Cursor,
                          name: str,
                          category: Optional[str],
                          fingerprint: str,
                          brand: Optional[str],
                          model: Optional[str],
                          voltage: Optional[str],
                          kit: Optional[str],
                          ean_gtin: Optional[str]) -> int:
    """Robust upsert that respects existing UNIQUE indexes."""
    # Convert voltage "18v" -> 18
    v_int = None
    if voltage and isinstance(voltage, str) and voltage.endswith('v'):
        try: v_int = int(voltage[:-1])
        except ValueError: v_int = None

    # 1) Fast path: try to insert (ignored if UNIQUE hits)
    cur.execute("""
        INSERT OR IGNORE INTO products (name, category, fingerprint, brand, model, power_source, voltage, kit, chuck, ean_gtin)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        name,
        category,
        fingerprint,
        (brand or '').lower() or None,
        (model or '') or None,
        None,
        v_int,
        kit or None,
        None,
        ean_gtin or None,
    ))

    # If row inserted, fetch id by fingerprint or name
    cur.execute("SELECT id FROM products WHERE fingerprint = ?", (fingerprint,))
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute("SELECT id FROM products WHERE lower(name) = lower(?)", (name,))
    row = cur.fetchone()
    if row:
        return row[0]

    # 2) Last resort: adjust name with a tiny suffix and insert again
    alt_name = f"{name} • {fingerprint.split(':',1)[0]}"
    cur.execute("""
        INSERT OR IGNORE INTO products (name, category, fingerprint, brand, model, power_source, voltage, kit, chuck, ean_gtin)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        alt_name,
        category,
        fingerprint,
        (brand or '').lower() or None,
        (model or '') or None,
        None,
        v_int,
        kit or None,
        None,
        ean_gtin or None,
    ))
    cur.execute("SELECT id FROM products WHERE fingerprint = ?", (fingerprint,))
    row = cur.fetchone()
    if row:
        return row[0]

    # 3) If still not found, try selecting by (brand, model, voltage) if present
    if (brand or model or v_int is not None):
        cur.execute("""
            SELECT id FROM products
            WHERE (brand IS ? OR brand = ?)
              AND (model IS ? OR model = ?)
              AND (voltage IS ? OR voltage = ?)
            ORDER BY id ASC LIMIT 1
        """, ((brand or '').lower() or None, (brand or '').lower() or None,
              (model or '') or None, (model or '') or None,
              v_int, v_int))
        row = cur.fetchone()
        if row:
            return row[0]

    # 4) Absolute fallback: create with a guaranteed unique name
    uniq_name = f"{name} [{hashlib.sha1(name.encode('utf-8')).hexdigest()[:6]}]"
    cur.execute("""
        INSERT INTO products (name, category, fingerprint, brand, model, power_source, voltage, kit, chuck, ean_gtin)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        uniq_name,
        category,
        fingerprint,
        (brand or '').lower() or None,
        (model or '') or None,
        None,
        v_int,
        kit or None,
        None,
        ean_gtin or None,
    ))
    return cur.lastrowid

# ----------------------------
# Main resolver
# ----------------------------
def resolve(db_path: str = "data/tooltally.db", batch_commit_every: int = 500) -> None:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    set_pragmas(con)
    cur = con.cursor()

    cur.execute("""
        SELECT id, vendor, title, price_pounds, url, vendor_sku,
               category_name, scraped_at, ean_gtin, mpn
        FROM raw_offers
        WHERE processed=0
    """)
    raw = cur.fetchall()
    if not raw:
        print("No unprocessed raw_offers found. Nothing to do.")
        con.close()
        return

    rows: List[Dict] = []
    for r in raw:
        rows.append({
            'id': r['id'],
            'vendor': r['vendor'],
            'title': r['title'] or '',
            'price_pounds': r['price_pounds'],
            'url': r['url'],
            'vendor_sku': r['vendor_sku'],
            'category_name': r['category_name'],
            'scraped_at': r['scraped_at'],
            'ean_gtin': r['ean_gtin'],
            'mpn': r['mpn'],
        })

    n = len(rows)
    dsu = DSU(n)
    key_index: Dict[str, List[int]] = defaultdict(list)

    for i, row in enumerate(rows):
        for typ, key in candidate_keys(row):
            key_index[f'{typ}:{key}'].append(i)

    for idxs in key_index.values():
        if len(idxs) > 1:
            first = idxs[0]
            for j in idxs[1:]:
                dsu.union(first, j)

    clusters: Dict[int, List[int]] = defaultdict(list)
    for i in range(n):
        clusters[dsu.find(i)].append(i)

    product_count = 0
    offer_count = 0
    processed_count = 0
    used_fingerprints: set[str] = set()

    try:
        cur.execute("BEGIN;")

        for _, idxs in clusters.items():
            cluster_rows = [rows[i] for i in idxs]

            brands, models, volts, kits, eans, cats = [], [], [], [], [], []
            for rr in cluster_rows:
                b, m = extract_brand_model_base(rr['title'])
                brands.append(b)
                models.append(m)
                volts.append(norm_voltage(rr['title']))
                kits.append(kit_signature(rr['title'], rr.get('mpn')))
                eans.append(norm_ean(rr.get('ean_gtin')))
                cats.append(canon_category(rr.get('category_name'), rr['title']))

            brand = majority_or_first(brands)
            model = majority_or_first(models)
            volt  = majority_or_first(volts)
            kit   = majority_or_first(kits)
            ean   = majority_or_first(eans)
            cat   = majority_or_first(cats)

            base_fp = choose_fingerprint(cluster_rows)
            fingerprint = uniquify_fingerprint(base_fp, cluster_rows, used_fingerprints)

            rep = cluster_rows[0]
            display_name = build_product_display_name(brand, model, volt, kit, rep['title'])

            # SAFE get-or-create (no UNIQUE crashes)
            prod_id = get_or_create_product(
                cur,
                name=display_name,
                category=cat or rep['category_name'],
                fingerprint=fingerprint,
                brand=brand,
                model=model,
                voltage=volt,
                kit=kit,
                ean_gtin=ean
            )
            product_count += 1

            # Offers
            for rr in cluster_rows:
                vendor_id = get_vendor_id(cur, rr['vendor'])
                insert_offer(
                    cur,
                    product_id=prod_id,
                    vendor_id=vendor_id,
                    price_pounds=rr['price_pounds'],
                    url=rr['url'],
                    vendor_sku=rr['vendor_sku'],
                    scraped_at=rr['scraped_at']
                )
                offer_count += 1

            # Mark processed
            cur.executemany(
                "UPDATE raw_offers SET processed=1 WHERE id=?",
                [(rr['id'],) for rr in cluster_rows]
            )
            processed_count += len(cluster_rows)

            if batch_commit_every and processed_count % batch_commit_every == 0:
                con.commit()
                cur.execute("BEGIN;")

        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()

    print(f"Resolved clusters → products: {product_count}, offers: {offer_count}, raw_offers processed: {processed_count}")

if __name__ == "__main__":
    resolve()
