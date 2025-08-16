powershell -Command "$content=@'
# ToolTally Backend — Quick Ops Guide

## How it works (rough outline)
1. **Scrapers** write vendor listings into `raw_offers` (title, price, url, vendor, category, etc.).
2. **Enrichment** visits product pages and extracts **EAN/GTIN** and **MPN** where possible.
3. **Resolver** reads new rows from `raw_offers` and builds canonical **products** and **offers**:
   - Normalises categories into umbrella groups (e.g. many vendor names -> **Drills**).
   - Uses **EAN/MPN** when available; else canonical **brand+MODEL+voltage** (kits collapse).
   - Fuzzy fallback if still no match.
4. **Deduper** keeps the latest/best price per **(product_id, vendor_id)** in `offers`.
5. **Flask API** serves `/products` and `/products/<id>` to the frontend proxy.

## Full backend cycle (copy/paste)
:: 1) Backup DB
copy data\tooltally.db data\backups\tooltally_%date:~-4%%date:~3,2%%date:~0,2%.db

:: 2) Reset raw rows to be reprocessed
py -c \"import sqlite3; con=sqlite3.connect(r'data\\tooltally.db'); cur=con.cursor(); cur.execute('UPDATE raw_offers SET processed=0'); con.commit(); con.close(); print('Reset processed=0')\"

:: 3) (Optional) Enrich identifiers from product pages
set ALLOW=screwfix.com,toolstation.com,ukplanettools.co.uk,dm-tools.co.uk
set LIMIT=5000
py scripts\enrich_identifiers_from_pages.py

:: 4) Resolve raw_offers -> products + offers
py scripts\resolver.py

:: 5) Deduplicate offers (keep one per product/vendor)
py scripts\dedupe_offers.py

:: 6) Quick health checks
py -c \"import sqlite3; con=sqlite3.connect(r'data\\tooltally.db'); print('Products with >1 vendor:', con.execute('select count(*) from (select product_id, count(distinct vendor_id) c from offers group by product_id having c>1)').fetchone()[0]); con.close()\"
py -c \"import sqlite3; con=sqlite3.connect(r'data\\tooltally.db'); print('model-key products:', con.execute(\"select count(*) from products where fingerprint like 'model:%'\").fetchone()[0]); con.close()\"

:: 7) Run API
py api.py
'@; Set-Content -Path README.md -Value $content -Encoding UTF8"
