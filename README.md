# ToolTally — Scrapers & API (Flask + SQLite)

This repository hosts:
- Retailer scrapers that populate `raw_offers`
- Resolver logic that clusters offers into canonical `products` with normalized identifiers (EAN/MPN/model)
- Deduplication & health checks
- A **compatibility API** (`api/compat_search.py`) used by the frontend
- Utility to **backfill product images** (`scripts/backfill_images.py`)

**Database:** `data/tooltally.db` (SQLite)

---

## Setup

We recommend Python 3.10–3.12 on Windows (works on macOS/Linux too).

```powershell
# 1) Create and activate a venv
python -m venv .venv
.venv\Scripts\activate

# 2) Install dependencies
pip install -r requirements.txt
# If not present in requirements, also install:
pip install flask flask-cors requests beautifulsoup4 lxml

# 3) Ensure the DB exists at:
#   data\tooltally.db
# (Your scrapers should populate raw_offers; see the Scrape → Resolve pipeline below.)
```

What each command does:

- `python -m venv .venv` — creates an isolated Python environment.
- `pip install ...` — installs Python dependencies (Flask API, scrapers, parsers).
- The DB is a plain SQLite file used by the pipeline and API.

---

## API (Compat Layer)

Run the **compat** Flask API (used by the frontend):

```powershell
py api\compat_search.py
# Serves at http://127.0.0.1:5000
```

Endpoints:

- `GET /categories`  
  Returns deduped category names (A→Z) with counts.  
  Response:
  ```json
  { "items": [ { "name": "Drills", "slug": "drills", "count": 123 }, ... ] }
  ```

- `GET /products?search=<q>&category=<Name>&page=1&limit=24`  
  Returns paginated product list.  
  - `lowest_price` ignores zero/NULL and offers without URL
  - `vendor_count` counts only offers with a product URL  
  Response:
  ```json
  {
    "items": [ { "id": "623", "title": "...", "brand": "Makita", "image_url": "...", "lowest_price": 194.90, "vendor_count": 2 } ],
    "total": 31, "page": 1, "limit": 24
  }
  ```

- `GET /product/<id>`  
  Returns product info + best offer per vendor (with vendor link).  
  Response:
  ```json
  {
    "product_info": { "id": "623", "title": "...", "brand": "Makita", "description": "", "image_url": "..." },
    "offers": [ { "vendor_name": "Screwfix", "price": 299.99, "vendor_product_url": "..." } ]
  }
  ```

> **Note:** Do **not** run `api.py` for the frontend — it doesn’t implement `/product/<id>`.

---

## Scrape → Resolve Pipeline

Your pipeline takes retailer `raw_offers`, enriches identifiers, clusters, and publishes canonical `products` + `offers`.

### 1) Reset & Clear (optional)

```powershell
# Reset raw_offers processed flag
py -c "import sqlite3; con=sqlite3.connect(r'data\\tooltally.db'); c=con.cursor(); c.execute('UPDATE raw_offers SET processed=0'); con.commit(); con.close(); print('Reset raw_offers.processed=0')"

# Clear products/offers (fresh run)
py -c "import sqlite3; con=sqlite3.connect(r'data\\tooltally.db'); c=con.cursor(); c.execute('DELETE FROM offers'); c.execute('DELETE FROM products'); con.commit(); con.close(); print('Cleared products and offers')"
```

### 2) Resolve

```powershell
py scripts\resolver.py
# Example output:
# Resolved clusters → products: 2892, offers: 5543, raw_offers processed: 5543
```

### 3) Deduplicate offers

```powershell
py scripts\dedupe_offers.py
# Removes duplicate offer rows (keeping best per vendor/product)
```

### 4) Health checks

```powershell
py scripts\health_checks.py
# Prints:
# - row counts and vendor counts
# - cross-vendor MPN overlap
# - multi-vendor product counts
# - fingerprint types (ean/mpn/model)
```

---

## Image Backfill (Product Images)

Add an `image_url` column (run once if missing):

```powershell
py -c "import sqlite3; con=sqlite3.connect(r'data\\tooltally.db'); cur=con.cursor(); cur.execute('ALTER TABLE products ADD COLUMN image_url TEXT'); con.commit(); con.close(); print('Added products.image_url column')"
```

Backfill images from vendor product pages (scrapes `og:image`, etc.):

```powershell
# Try all products (skips ones that already have image_url)
py scripts\backfill_images.py --limit 100000

# (Optional) force refresh even if image_url is set
# py scripts\backfill_images.py --limit 100000 --force
```

Check how many products have images:

```powershell
py -c "import sqlite3; con=sqlite3.connect(r'data\\tooltally.db'); print('Products with image_url:', con.execute('SELECT COUNT(*) FROM products WHERE image_url IS NOT NULL AND TRIM(image_url)<>\"\"').fetchone()[0]); con.close()"
```

---

## Useful SQL Diagnostics

Cross-vendor MPN overlap (after normalization):

```sql
SELECT
  UPPER(REPLACE(REPLACE(mpn,'-',''),' ','')) AS key,
  COUNT(DISTINCT vendor) AS vendor_count,
  COUNT(*) AS rows
FROM raw_offers
WHERE mpn IS NOT NULL AND TRIM(mpn) <> ''
GROUP BY key
HAVING vendor_count >= 2;
```

How many multi-vendor products exist post-resolve:

```sql
SELECT COUNT(*) FROM (
  SELECT product_id, COUNT(DISTINCT vendor_id) AS c
  FROM offers
  GROUP BY product_id
  HAVING c > 1
);
```

Breakdown of product fingerprint types:

```sql
SELECT
  SUM(fingerprint LIKE 'ean:%')   AS ean_key,
  SUM(fingerprint LIKE 'mpn:%')   AS mpn_key,
  SUM(fingerprint LIKE 'model:%') AS model_key
FROM products;
```

---

## Troubleshooting

### Frontend says: “Unexpected token `<` … not valid JSON”

You likely started `api.py`. Run **compat** instead:

```powershell
py api\compat_search.py
```

### `sqlite3.ProgrammingError: Incorrect number of bindings supplied`

This was caused by mismatched `WHERE`/params when combining query + category filters. Fixed in `compat_search.py`. Pull latest or ensure your local copy has the updated SQL that merges filters safely.

### `sqlite3.IntegrityError: UNIQUE constraint failed: products.fingerprint`

If you added a unique index on `products.fingerprint`, ensure the resolver clusters by **normalized** keys (MPN/EAN/model) before insert, or use `INSERT OR IGNORE` and then `UPDATE` existing rows. Our current resolver groups with canonicalized keys and should not violate the uniqueness constraint.

---

## Example cURL

```bash
# Categories
curl "http://127.0.0.1:5000/categories"

# Products (search)
curl "http://127.0.0.1:5000/products?search=Makita%20DHP484&page=1&limit=24"

# Products (category)
curl "http://127.0.0.1:5000/products?category=Drills&page=1&limit=24"

# Product detail
curl "http://127.0.0.1:5000/product/623"
```

---

## License

MIT (project-specific details may vary in the root LICENSE).
