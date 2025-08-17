# scripts/enrich_identifiers_from_pages.py
# Enrich raw_offers with EAN/GTIN and MPN by fetching product pages.
# Targeted for hosts: toolstation.com, ukplanettools.co.uk, dm-tools.co.uk, screwfix.com
#
# Requires: requests, beautifulsoup4

import os
import re
import json
import time
import sqlite3
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

DB_PATH = os.environ.get("DB_PATH") or os.path.join(os.path.dirname(__file__), "..", "data", "tooltally.db")
DB_PATH = os.path.abspath(DB_PATH)

TIMEOUT = 15
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ToolTallyBot/1.0; +https://example.com/bot) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
}

# ---------- Normalisers ----------

def norm_text(s: str) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", s).strip()

def norm_mpn(s: str) -> str:
    s = norm_text(s)
    # Common cruft
    s = re.sub(r"\b(?:mpn|model|manufacturer part(?: number)?|part(?: number)?|sku)\b[:\s]*", "", s, flags=re.I)
    # Keep alnum + dashes/slashes only
    s = re.sub(r"[^A-Za-z0-9\-/]", "", s)
    return s[:64] if s else ""

def norm_ean(s: str) -> str:
    s = norm_text(s)
    s = re.sub(r"\b(?:ean|gtin|barcode)\b[:\s]*", "", s, flags=re.I)
    s = re.sub(r"[^0-9]", "", s)
    # Accept GTIN-8/12/13/14. Most UK sites use 13.
    if len(s) in (8, 12, 13, 14):
        return s
    return ""

# ---------- Generic extractors ----------

EAN_PAT = re.compile(r"\b(?:EAN|GTIN|Barcode)\b[:\s]*([0-9\- ]{8,20})", re.I)
MPN_PAT = re.compile(r"\b(?:MPN|Manufacturer(?:’s|s)? Part(?: No\.?| Number)?|Model|Product Code|Man(?:uf)?\.?\s*Code)\b[:\s]*([A-Z0-9\-\/]{3,64})", re.I)

def from_json_ld(soup: BeautifulSoup):
    mpn = ean = ""
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            payload = json.loads(tag.string or "")
        except Exception:
            continue
        # payload can be dict or list
        objs = payload if isinstance(payload, list) else [payload]
        for obj in objs:
            if not isinstance(obj, dict):
                continue
            mpn_try = obj.get("mpn") or obj.get("sku")
            ean_try = obj.get("gtin13") or obj.get("gtin") or obj.get("gtin14") or obj.get("gtin8") or obj.get("gtin12")
            if not mpn and mpn_try:
                mpn = norm_mpn(str(mpn_try))
            if not ean and ean_try:
                ean = norm_ean(str(ean_try))
    return mpn, ean

def from_tables_by_labels(soup: BeautifulSoup):
    mpn = ean = ""
    # Look for definition lists or spec tables
    # dt/dd pairs
    for dl in soup.find_all(["dl"]):
        terms = dl.find_all(["dt", "th", "strong", "span"])
        for t in terms:
            label = norm_text(t.get_text(" "))
            if not label:
                continue
            val_el = t.find_next_sibling(["dd", "td", "span"])
            val = norm_text(val_el.get_text(" ")) if val_el else ""
            if not val:
                continue
            if not mpn and re.search(r"\b(mpn|manufacturer|model|product code|sku)\b", label, re.I):
                mpn = norm_mpn(val)
            if not ean and re.search(r"\b(ean|gtin|barcode)\b", label, re.I):
                ean = norm_ean(val)
    # tables with rows
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            th = tr.find(["th", "td"])
            td = None
            if th:
                cand = th.find_next_sibling("td")
                if not cand:
                    tds = tr.find_all("td")
                    if len(tds) >= 2:
                        th, td = tds[0], tds[1]
                    else:
                        continue
                else:
                    td = cand
            if not th or not td:
                continue
            label = norm_text(th.get_text(" "))
            val = norm_text(td.get_text(" "))
            if not val:
                continue
            if not mpn and re.search(r"\b(mpn|manufacturer|model|product code|sku)\b", label, re.I):
                mpn = norm_mpn(val)
            if not ean and re.search(r"\b(ean|gtin|barcode)\b", label, re.I):
                ean = norm_ean(val)
    return mpn, ean

def from_free_text(soup: BeautifulSoup):
    mpn = ean = ""
    text = soup.get_text(" ")
    m = EAN_PAT.search(text)
    if m and not ean:
        ean = norm_ean(m.group(1))
    m = MPN_PAT.search(text)
    if m and not mpn:
        mpn = norm_mpn(m.group(1))
    return mpn, ean

# ---------- Host-specific helpers ----------

def extract_toolstation(soup):
    # Toolstation typically has JSON-LD with mpn and sometimes gtin13
    mpn, ean = from_json_ld(soup)
    if not (mpn or ean):
        mpn2, ean2 = from_tables_by_labels(soup)
        mpn = mpn or mpn2
        ean = ean or ean2
    if not (mpn or ean):
        mpn3, ean3 = from_free_text(soup)
        mpn = mpn or mpn3
        ean = ean or ean3
    return mpn, ean

def extract_ukplanettools(soup):
    # Often shows MPN/EAN in spec table or dd/dt pairs
    mpn, ean = from_tables_by_labels(soup)
    if not (mpn or ean):
        mpn2, ean2 = from_json_ld(soup)
        mpn = mpn or mpn2
        ean = ean or ean2
    if not (mpn or ean):
        mpn3, ean3 = from_free_text(soup)
        mpn = mpn or mpn3
        ean = ean or ean3
    return mpn, ean

def extract_dmtools(soup):
    mpn, ean = from_tables_by_labels(soup)
    if not (mpn or ean):
        mpn2, ean2 = from_json_ld(soup)
        mpn = mpn or mpn2
        ean = ean or ean2
    if not (mpn or ean):
        mpn3, ean3 = from_free_text(soup)
        mpn = mpn or mpn3
        ean = ean or ean3
    return mpn, ean

def extract_screwfix(soup):
    # Screwfix rarely exposes EAN; sometimes MPN in JSON-LD sku or spec table
    mpn, ean = from_json_ld(soup)
    if not (mpn or ean):
        mpn2, ean2 = from_tables_by_labels(soup)
        mpn = mpn or mpn2
        ean = ean or ean2
    if not (mpn or ean):
        mpn3, ean3 = from_free_text(soup)
        mpn = mpn or mpn3
        ean = ean or ean3
    return mpn, ean

HOST_EXTRACTORS = {
    "toolstation.com": extract_toolstation,
    "ukplanettools.co.uk": extract_ukplanettools,
    "dm-tools.co.uk": extract_dmtools,
    "screwfix.com": extract_screwfix,
}

# ---------- Main ----------

def host_from_url(url: str) -> str:
    try:
        h = urlparse(url).netloc.lower()
        if h.startswith("www."):
            h = h[4:]
        return h
    except Exception:
        return ""

def main():
    allow_env = os.environ.get("ALLOW", "")
    allow_list = [h.strip().lower() for h in allow_env.split(",") if h.strip()] or list(HOST_EXTRACTORS.keys())
    limit_env = os.environ.get("LIMIT")
    limit = int(limit_env) if (limit_env and limit_env.isdigit()) else None

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    # Candidate URLs: missing BOTH mpn and ean
    urls = []
    for (url,) in cur.execute("""
        SELECT DISTINCT url
        FROM raw_offers
        WHERE (ean_gtin IS NULL OR ean_gtin='')
          AND (mpn IS NULL OR mpn='')
    """):
        h = host_from_url(url)
        if h in allow_list:
            urls.append(url)

    if limit is not None:
        urls = urls[:limit]

    print(f"DB: {DB_PATH}")
    print(f"[{time.strftime('%Y-%m-%dT%H:%M:%S%z')}] Found {len(urls)} URLs to enrich from hosts: {', '.join(allow_list)}")

    found_both = found_mpn = found_ean = found_none = 0
    updated_rows = 0

    for idx, url in enumerate(urls, 1):
        h = host_from_url(url)
        extractor = HOST_EXTRACTORS.get(h)
        try:
            resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            html = resp.text
            soup = BeautifulSoup(html, "html.parser")

            mpn = ean = ""
            if extractor:
                mpn, ean = extractor(soup)
            else:
                # generic fallback
                m1, e1 = from_json_ld(soup)
                m2, e2 = from_tables_by_labels(soup)
                m3, e3 = from_free_text(soup)
                mpn = m1 or m2 or m3
                ean = e1 or e2 or e3

            if mpn and ean:
                found_both += 1
                status = "mpn+ean"
            elif mpn:
                found_mpn += 1
                status = "mpn"
            elif ean:
                found_ean += 1
                status = "ean"
            else:
                found_none += 1
                status = "no-ids"

            if mpn or ean:
                cur.execute("""
                    UPDATE raw_offers
                    SET ean_gtin=COALESCE(NULLIF(ean_gtin,''), ?),
                        mpn=COALESCE(NULLIF(mpn,''), ?)
                    WHERE url=?
                """, (ean or None, mpn or None, url))
                updated_rows += cur.rowcount

            # Log every 50 URLs
            if idx % 50 == 0 or not (mpn or ean):
                print(f" {idx}/{len(urls)} {status} {url}")

            if idx % 200 == 0:
                con.commit()

        except KeyboardInterrupt:
            print("\nInterrupted by user. Committing progress…")
            break
        except Exception as e:
            print(f" {idx}/{len(urls)} ERROR {h} {url} :: {e}")

    con.commit()
    con.close()

    print(f"[{time.strftime('%Y-%m-%dT%H:%M:%S%z')}] Enrichment finished. "
          f"URLs processed: {len(urls)} | rows updated: {updated_rows} "
          f"| both: {found_both} | mpn: {found_mpn} | ean: {found_ean} | none: {found_none}")

if __name__ == "__main__":
    main()
