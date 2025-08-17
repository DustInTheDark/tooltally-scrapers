# scripts/backfill_mpn_from_titles.py
"""
Backfill raw_offers.mpn by extracting model codes from product titles and URL slugs.

Why:
- Many UK retailer pages (esp. refurb pages) don't surface MPN/EAN.
- Titles and slugs almost always contain a recognizable model (e.g., Makita DHP453Z, DeWalt DCD996, Bosch GSB18V-55).
- Filling mpn allows the resolver to canonicalize across vendors.

This script:
- Scans raw_offers where mpn is NULL/empty.
- Extracts candidate model from title and URL:
    * Brand-specific regexes (Makita, DeWalt, Bosch, Milwaukee, Ryobi, Einhell, Metabo, Hikoki, Erbauer, Festool, Trend, Titan)
    * Generic fallback for tokens like ABC123, ABC123Z, GSB18V-55, etc.
- Applies a small sanity filter (length, alnum/dash content).
- Updates mpn when confident.
- Prints a summary.

Safe to run multiple times.
"""

import os
import re
import sqlite3
from urllib.parse import urlparse

DB_PATH = os.environ.get("DB_PATH") or os.path.join(os.path.dirname(__file__), "..", "data", "tooltally.db")
DB_PATH = os.path.abspath(DB_PATH)

# --- Helpers -----------------------------------------------------------------

def slug_from_url(url: str) -> str:
    try:
        path = urlparse(url).path or ""
        slug = path.rsplit("/", 1)[-1]
        return slug.lower()
    except Exception:
        return ""

def normalise_token(tok: str) -> str:
    # Keep letters/digits/[-/]
    t = re.sub(r"[^A-Za-z0-9\-\/]", "", tok)
    # Collapse multiple dashes
    t = re.sub(r"-{2,}", "-", t)
    # Uppercase for mpn storage
    return t.upper()

def pick_best(candidates):
    # Prefer longer tokens (but cap to avoid insane strings), then ones with digits
    candidates = list({c for c in candidates if 3 <= len(c) <= 32})
    if not candidates:
        return ""
    candidates.sort(key=lambda s: (len(s), any(ch.isdigit() for ch in s)), reverse=True)
    return candidates[0]

# --- Brand-specific regex (greedy enough to be useful, conservative to avoid garbage) -----
# NOTE: We match case-insensitively over both title and slug.

PATTERNS = [
    # Makita: DHP453Z, DHP484Z, DTD173Z, DTW1002Z, DCS565N, DHR202Z, GA9020 etc.
    re.compile(r"\bD[THSCFWRLM][A-Z]{1,3}\d{2,4}[A-Z]{0,3}\b", re.I),

    # DeWalt: DCD996, DCD805, DCF887, DCH253, DWE560, DCG405N, etc.
    re.compile(r"\bDC[DFGHLMW]\d{3,4}[A-Z]{0,3}\b", re.I),

    # Bosch Pro: GSB/GSR/GSB18V-55, GSR18V-60, GDX18V-200, etc.
    re.compile(r"\bG[SZ]R\d{2}V-\d{2,3}\b", re.I),
    re.compile(r"\bG[SZ]B\d{2}V-\d{2,3}\b", re.I),
    re.compile(r"\bGDX\d{2}V-\d{2,3}\b", re.I),

    # Milwaukee: M12 FPD, M18 FPD2, M18 CCS66-0, etc. (compact form)
    re.compile(r"\bM1[28][ -]?[A-Z0-9-]{2,}\b", re.I),

    # Ryobi: R18PD7, R18IDBL, R18PD3, etc.
    re.compile(r"\bR18[A-Z]{2,4}\d*\b", re.I),

    # Einhell: TE-CD-18-60, TE-CI-18-220, etc.
    re.compile(r"\bTE-[A-Z]{1,3}-\d{1,3}(?:-\d{1,3})?(?:-[A-Z0-9]{1,6})?\b", re.I),

    # Metabo: SSW 18 LTX 400 BL, SSD 18 LTX 200 BL (compact to SSW18LTX400BL)
    re.compile(r"\bSS[WD]\s*18\s*LTX\s*\d{2,4}\s*BL\b", re.I),

    # Hikoki/Hitachi: DV18DE, WH18DBDL2, WR18DBDL2, etc.
    re.compile(r"\b(?:W|D|G|C)V?\d{2}[A-Z]{2,5}\d*\b", re.I),

    # Festool: TPC 18/4, TID 18/4 (normalize to TPC18-4)
    re.compile(r"\bT[IP][CD]\s*18\s*/\s*\d\b", re.I),
]

# Generic “token-ish” fallback:
#   - ABC123, ABC123Z, GSB18V-55, GSR18V-60, DTW1002Z, etc.
GENERIC = re.compile(r"\b[A-Z]{2,5}\d{2,4}[A-Z]{0,3}(?:-\d{1,3})?\b", re.I)

def extract_candidates(text: str):
    cands = []
    for pat in PATTERNS:
        for m in pat.findall(text):
            cands.append(normalise_token(m))
    # Festool special form to normalize TPC 18/4 => TPC18-4
    cands = [re.sub(r"\s*/\s*", "-", c) for c in cands]

    for m in GENERIC.findall(text):
        cands.append(normalise_token(m))
    return cands

def main():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    print(f"DB: {DB_PATH}")

    rows = cur.execute("""
      SELECT id, title, url
      FROM raw_offers
      WHERE (mpn IS NULL OR mpn='')
    """).fetchall()

    updated = 0
    per_host = {}
    per_brand_guess = {}

    for _id, title, url in rows:
        title = title or ""
        slug = slug_from_url(url)
        hay = f"{title} {slug}"

        cands = extract_candidates(hay)
        mpn = pick_best(cands)

        if not mpn:
            continue

        # Filter out obviously generic or too-short nonsense
        if len(mpn) < 4:
            continue

        # Update
        cur.execute("UPDATE raw_offers SET mpn=? WHERE id=?", (mpn, _id))
        updated += 1

        host = (urlparse(url).netloc or "").lower()
        per_host[host] = per_host.get(host, 0) + 1

        # naive brand guess from title
        brand_guess = ""
        tlow = title.lower()
        if "makita" in tlow: brand_guess = "Makita"
        elif "dewalt" in tlow or "de walt" in tlow: brand_guess = "DeWalt"
        elif "bosch" in tlow: brand_guess = "Bosch"
        elif "milwaukee" in tlow: brand_guess = "Milwaukee"
        elif "ryobi" in tlow: brand_guess = "Ryobi"
        elif "einhell" in tlow: brand_guess = "Einhell"
        elif "metabo" in tlow: brand_guess = "Metabo"
        elif "hikoki" in tlow or "hitachi" in tlow: brand_guess = "Hikoki"
        elif "erbauer" in tlow: brand_guess = "Erbauer"
        elif "festool" in tlow: brand_guess = "Festool"
        elif "trend" in tlow: brand_guess = "Trend"
        elif "titan" in tlow: brand_guess = "Titan"

        if brand_guess:
            key = f"{brand_guess}:{mpn}"
            per_brand_guess[key] = per_brand_guess.get(key, 0) + 1

    con.commit()
    con.close()

    print(f"Backfill complete. raw_offers.mpn updated for {updated} rows.")
    if per_host:
        print("Top hosts updated:")
        for h, c in sorted(per_host.items(), key=lambda kv: kv[1], reverse=True)[:10]:
            print(f"  {h}: {c}")
    if per_brand_guess:
        print("Top brand:mpn pairs (guessed) updated:")
        for k, c in sorted(per_brand_guess.items(), key=lambda kv: kv[1], reverse=True)[:15]:
            print(f"  {k}: {c}")

if __name__ == "__main__":
    main()
