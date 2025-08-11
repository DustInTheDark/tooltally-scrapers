# ToolTally — Scrapers & API (Flask + SQLite + Scrapy)

This repo scrapes UK tool retailers into a **staging table** (`raw_offers`), resolves them into **canonical** `products` + `offers`, and serves a **Flask API** that the frontend consumes.

```
Scrapers  ──►  raw_offers (staging)
Resolver  ──►  products (unique) + offers (per vendor)
Flask API ──►  /products, /products/:id, /categories
Frontend  ──►  Next.js proxy calls the API
```

---

## Requirements

* **Python** 3.11+ (tested on 3.12)
* **pip** / venv
* **SQLite** (no CLI required; Python ships with `sqlite3`)

Default database path: `data/tooltally.db` (override with `DB_PATH` env var).

---

## Quick start (Windows PowerShell shown; macOS/Linux: use `python3` instead of `py`)

### 1) Create venv & install

```powershell
py -m venv .venv
. .venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2) Ensure schema (creates/updates tables)

```powershell
py scripts\migrate.py
# Output should confirm schema ensured and any lightweight migrations (e.g. offers.created_at)
```

### 3) Scrape data (to staging: raw\_offers)

Run scrapers individually:

```powershell
py scripts\scrape_toolstation.py
py scripts\scrape_screwfix.py
py scripts\scrape_toolstop.py
py scripts\scrape_dandm.py
```

Or run them all:

```powershell
py scripts\scrape_all.py
```

Notes:

* Runner scripts throttle requests and **bypass project pipelines**; items are written directly to `raw_offers`.
* Each scraped item has: `vendor, title, price_pounds, url, vendor_sku, category_name, scraped_at`.

### 4) Resolve to canonical products/offers

```powershell
py scripts\resolver.py
# Example:
# Resolver: planned 3086 unique products from 5543 raw rows.
# Resolver: clearing previous products/offers …
# Resolver: inserted 3086 products and 5543 offers.
```

Optional dry-run (prints plan, no DB writes):

```powershell
py scripts\resolver.py --dry-run
```

### 5) Run the Flask API (reads canonical tables)

```powershell
set FLASK_APP=api.py
set FLASK_ENV=development
# Optional: set DB_PATH explicitly
# set DB_PATH=%CD%\data\tooltally.db
py api.py
# http://127.0.0.1:5000
```

---

## API endpoints

* `GET /products?search=<term>&category=<optional>&page=<optional>&limit=<optional>`

  * Returns **unique** products with min price and vendor count (from canonical tables).
  * Response shape (paginated):

    ```json
    {
      "items": [
        { "id": 123, "name": "Makita DHP484Z 18V Bare Unit",
          "category": "Cordless Drill", "min_price": 70.5, "vendors_count": 2 }
      ],
      "total": 312, "page": 1, "limit": 24
    }
    ```

* `GET /products/<id>`

  * One product + **all vendor offers** (sorted by price):

    ```json
    {
      "id": 123, "name": "Makita DHP484Z 18V Bare Unit", "category": "Cordless Drill",
      "vendors": [
        { "vendor": "Toolstation", "price": 70.5, "buy_url": "..." },
        { "vendor": "Screwfix", "price": 72.0, "buy_url": "..." }
      ]
    }
    ```

* `GET /categories`

  * Distinct product categories (from canonical `products`).

---

## Database schema (summary)

### `raw_offers` (staging)

* `id` INTEGER PK
* `vendor` TEXT
* `title` TEXT
* `price_pounds` REAL
* `url` TEXT UNIQUE
* `vendor_sku` TEXT
* `category_name` TEXT
* `scraped_at` TEXT (ISO8601)
* `processed` INTEGER DEFAULT 0

### `vendors`

* `id` INTEGER PK
* `name` TEXT UNIQUE

### `products` (canonical)

* `id` INTEGER PK
* `name` TEXT
* `category` TEXT

### `offers` (canonical)

* `id` INTEGER PK
* `product_id` INTEGER
* `vendor_id` INTEGER
* `price_pounds` REAL
* `url` TEXT UNIQUE
* `vendor_sku` TEXT
* `scraped_at` TEXT
* `created_at` TEXT  ← added by migrations

**Indexes** (created by `scripts/migrate.py`):

* `vendors(name)` unique
* `products(name)` idx
* `offers(product_id)`, `offers(vendor_id)` idx
* `offers(url)` unique
* `raw_offers(vendor)`, `raw_offers(url)` idx

---

## Useful checks

How many rows:

```powershell
py -c "import sqlite3; con=sqlite3.connect(r'data/tooltally.db'); print('products:', con.execute('select count(*) from products').fetchone()[0]); print('offers:', con.execute('select count(*) from offers').fetchone()[0]); print('vendors:', con.execute('select count(*) from vendors').fetchone()[0]); con.close()"
```

Merge quality (how many products have >1 vendor):

```powershell
py -c "import sqlite3; con=sqlite3.connect(r'data/tooltally.db'); print('products with >1 vendor:', con.execute('select count(*) from (select product_id, count(distinct vendor_id) c from offers group by product_id having c>1)').fetchone()[0]); con.close()"
```

Peek some merged examples:

```powershell
py -c "import sqlite3, json; con=sqlite3.connect(r'data/tooltally.db'); con.row_factory=sqlite3.Row; q='''select p.id, p.name, count(distinct o.vendor_id) as vendors, min(o.price_pounds) as min_price\n     from products p join offers o on o.product_id=p.id\n     group by p.id having vendors>1\n     order by vendors desc, min_price asc limit 10'''; print(json.dumps([dict(r) for r in con.execute(q)], indent=2)); con.close()"
```

---

## Resetting (optional helpers)

We rebuild canonical tables each resolver run (it deletes and re-inserts).
If you want an explicit reset script:

```powershell
# Soft reset: clears products/offers (same effect as resolver start)
py scripts\reset_canonical.py

# Hard reset: also clears vendors
py scripts\reset_canonical.py --all

# Nuclear: also clears raw_offers (you must re-scrape)
py scripts\reset_canonical.py --all --raw
```

> If `reset_canonical.py` isn’t in your repo, you can skip it—running `resolver.py` is enough.

---

## Configuration

* `DB_PATH` — path to SQLite DB (default: `data/tooltally.db`)
* Standard Flask env:

  * `FLASK_APP=api.py`
  * `FLASK_ENV=development` (or `FLASK_DEBUG=1`)
  * `FLASK_RUN_PORT=5000` (if using `flask run`)

Example:

```powershell
set DB_PATH=C:\path\to\tooltally.db
py scripts\migrate.py
py api.py
```

---

## Troubleshooting

* **`offers has no column named created_at`**
  Run:

  ```powershell
  py scripts\migrate.py
  ```
* **`No module named 'scripts'` when running scrapers**
  Run from repo root and use `py scripts\...`, e.g.:

  ```powershell
  py scripts\scrape_toolstation.py
  ```
* **PowerShell quoting errors**
  Keep one-liners truly single-line (avoid `\` line continuations). Use the examples above.
* **SQLite CLI missing**
  Use the Python one-liners (they use the built-in `sqlite3` module). Alternatively, install DB Browser for SQLite.
* **Scrapy signals/pipelines oddities**
  Our runner scripts connect their own signals and **disable project pipelines/telnet** to avoid interference.

---

## Notes

* The resolver (v2.1) merges across vendors using **brand + model/MPN + voltage + kit signature (bare/kit)** with brand-aware regex (Makita, DeWalt, Bosch, Milwaukee, Einhell, Ryobi, Black+Decker). Each run is idempotent.
* WAL mode is enabled for better read/write concurrency while the frontend queries the DB.
* The Flask API queries **canonical** tables only (`products`, `offers`, `vendors`)—the frontend gets unique items with min price and vendor counts out of the box.
