# ToolTally — UK Tool Price Comparison

Backend: [tooltally-scrapers](https://github.com/DustInTheDark/tooltally-scrapers)  
Frontend: [tooltally-frontend](https://github.com/DustInTheDark/tooltally-frontend)

---

## How the backend works (rough outline)
1. **Scrapers** collect listings from multiple UK vendors → insert into `raw_offers`.
2. **Enrichment** fetches product pages → extracts identifiers (`EAN/GTIN`, `MPN`) where possible.
3. **Resolver** turns raw rows into canonical `products` + `offers`:
   - Normalises **categories** into umbrella groups (e.g. `cordless drill`, `combi drill` → `Drills`).
   - Uses identifiers (EAN/MPN) if present, else canonical **brand + model + voltage**.
   - Falls back to fuzzy matching to merge across vendors.
4. **Deduper** removes duplicate vendor entries → keeps lowest price per `(product_id, vendor_id)`.
5. **API** (`api.py`) exposes `/products` and `/products/<id>`.

---

## How the frontend works (rough outline)
- Repo: [tooltally-frontend](https://github.com/DustInTheDark/tooltally-frontend)
- Stack: Next.js 15 (App Router) + TailwindCSS.
- `/api/products` → Proxies Flask API (no CORS issues).
- UI:
  - **Products page**: search grid, pagination, "Load more", vendor count, GBP price formatting.
  - **Product detail**: vendor list sorted by ascending price, “Buy” links open in new tab.

---

## Running the full backend cycle

### CMD (Windows Command Prompt)
```cmd
REM 1. Activate venv
cd tooltally-scrapers
.venv\Scripts\activate

REM 2. Backup DB
copy data\tooltally.db data\backups\tooltally_%date:~-4%%date:~3,2%%date:~0,2%.db

REM 3. Reset raw_offers
py -c "import sqlite3; con=sqlite3.connect(r'data\\tooltally.db'); cur=con.cursor(); cur.execute('UPDATE raw_offers SET processed=0'); con.commit(); con.close(); print('Reset processed=0')"

REM 4. (Optional) Enrich identifiers
set ALLOW=screwfix.com,toolstation.com,ukplanettools.co.uk,dm-tools.co.uk
set LIMIT=5000
py scripts\enrich_identifiers_from_pages.py

REM 5. Resolve
py scripts\resolver.py

REM 6. Deduplicate
py scripts\dedupe_offers.py

REM 7. Health checks
py -c "import sqlite3; con=sqlite3.connect(r'data\\tooltally.db'); print('Products with >1 vendor:', con.execute('select count(*) from (select product_id, count(distinct vendor_id) c from offers group by product_id having c>1)').fetchone()[0]); con.close()"
py -c "import sqlite3; con=sqlite3.connect(r'data\\tooltally.db'); print('Model-key products:', con.execute(\"select count(*) from products where fingerprint like 'model:%'\").fetchone()[0]); con.close()"

REM 8. Run backend API
py api.py
