# scripts/enrich_identifiers_from_pages.py
"""
Resumable enrichment of raw_offers.ean_gtin/mpn by fetching product pages.

Improvements vs previous version:
- Domain allowlist to avoid slow/low-yield sites (configure ALLOW_DOMAINS).
- Resumable: records progress so reruns skip completed URLs.
- Retries with backoff; shorter timeout.
- Optional LIMIT to test on a subset first.

Usage examples:
    py scripts\\enrich_identifiers_from_pages.py           # run with defaults
    set ALLOW=toolstation.com,screwfix.com                 # PowerShell: $env:ALLOW="..."
    set LIMIT=800
    py scripts\\enrich_identifiers_from_pages.py
"""
from __future__ import annotations
import os, sqlite3, time
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter, Retry

from util_identifiers import extract_identifiers_from_html

DB_PATH = os.path.abspath(os.environ.get("DB_PATH") or os.path.join(os.path.dirname(__file__), "..", "data", "tooltally.db"))

# Configure which domains to enrich (comma-separated env var ALLOW, or default list)
DEFAULT_ALLOW = ["screwfix.com", "toolstation.com", "ukplanettools.co.uk"]
ALLOW_DOMAINS = [d.strip().lower() for d in (os.environ.get("ALLOW") or ",".join(DEFAULT_ALLOW)).split(",") if d.strip()]

# Optional limit from env
LIMIT = None
try:
    _lim = os.environ.get("LIMIT")
    LIMIT = int(_lim) if _lim else None
except Exception:
    LIMIT = None

TIMEOUT = 8   # seconds
PAUSE   = 0.25  # seconds between requests

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
                       " AppleWebKit/537.36 (KHTML, like Gecko)"
                       " Chrome/124.0.0.0 Safari/537.36")
    })
    retries = Retry(
        total=2,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"])
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=20)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

def domain_allowed(url: str) -> bool:
    u = (url or "").lower()
    return any(d in u for d in ALLOW_DOMAINS)

def ensure_progress_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS enrich_progress (
            url TEXT PRIMARY KEY,
            done INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT
        )
    """)

def main():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("PRAGMA foreign_keys=ON;")
    cur.execute("PRAGMA journal_mode=WAL;")
    ensure_progress_table(cur)
    con.commit()

    # Select distinct URLs that (a) are missing ids AND (b) are in allowed domains AND (c) not done yet
    where_domains = " OR ".join(["url LIKE ?" for _ in ALLOW_DOMAINS])
    params = [f"%{d}%" for d in ALLOW_DOMAINS]

    base_sql = f"""
        SELECT DISTINCT r.url
        FROM raw_offers r
        LEFT JOIN enrich_progress p ON p.url = r.url
        WHERE (r.ean_gtin IS NULL OR TRIM(r.ean_gtin) = '')
          AND (r.mpn     IS NULL OR TRIM(r.mpn)     = '')
          AND r.url IS NOT NULL AND TRIM(r.url) != ''
          AND ({where_domains.replace("url", "r.url")})
          AND COALESCE(p.done, 0) = 0
    """
    if LIMIT:
        base_sql += " LIMIT ?"
        params.append(LIMIT)

    cur.execute(base_sql, params)
    urls = [r[0] for r in cur.fetchall()]
    total = len(urls)
    print(f"DB: {DB_PATH}")
    print(f"[{datetime.now(timezone.utc).isoformat()}] Allowed domains: {', '.join(ALLOW_DOMAINS)}")
    print(f"[{datetime.now(timezone.utc).isoformat()}] Found {total} URL(s) to enrich (LIMIT={LIMIT or 'none'}).")

    if total == 0:
        con.close()
        return

    sess = make_session()
    updated_count = 0

    for i, url in enumerate(urls, 1):
        try:
            resp = sess.get(url, timeout=TIMEOUT)
            if resp.status_code != 200 or not resp.text:
                print(f" {i}/{total} skip ({resp.status_code}) {url}")
                cur.execute("INSERT OR REPLACE INTO enrich_progress(url, done, updated_at) VALUES(?, 1, ?)",
                            (url, datetime.now(timezone.utc).isoformat()))
                con.commit()
                time.sleep(PAUSE)
                continue

            ids = extract_identifiers_from_html(resp.text)
            ean = (ids.get("ean_gtin") or "").strip()
            mpn = (ids.get("mpn") or "").strip()

            if not ean and not mpn:
                print(f" {i}/{total} no-ids {url}")
            else:
                cur.execute("""
                    UPDATE raw_offers
                       SET ean_gtin = COALESCE(NULLIF(TRIM(ean_gtin), ''), ?),
                           mpn      = COALESCE(NULLIF(TRIM(mpn), ''), ?)
                     WHERE url = ?
                """, (ean or None, mpn or None, url))
                updated_count += (1 if cur.rowcount > 0 else 0)
                print(f" {i}/{total} updated {cur.rowcount} row(s) for {url} -> ean={ean!r} mpn={mpn!r}")

            # Mark URL as done (even if no-ids) so we skip it next run
            cur.execute("INSERT OR REPLACE INTO enrich_progress(url, done, updated_at) VALUES(?, 1, ?)",
                        (url, datetime.now(timezone.utc).isoformat()))
            con.commit()
            time.sleep(PAUSE)

        except Exception as e:
            print(f" {i}/{total} error {url}: {e}")
            # Mark as done to avoid tight loops; you can DELETE FROM enrich_progress WHERE url=... to retry later
            cur.execute("INSERT OR REPLACE INTO enrich_progress(url, done, updated_at) VALUES(?, 1, ?)",
                        (url, datetime.now(timezone.utc).isoformat()))
            con.commit()

    con.close()
    print(f"[{datetime.now(timezone.utc).isoformat()}] Enrichment complete. URLs updated: {updated_count}/{total}")

if __name__ == "__main__":
    main()
