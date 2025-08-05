# 🕷️ ToolTally Scrapers

Utilities for harvesting product data for the
[ToolTally](https://github.com/DustInTheDark/tooltally-frontend) project.
The spiders target several UK tool vendors and write the scraped results to a
local SQLite database at `data/tooltally.db`.

## Supported vendors

- D&M Tools
- Screwfix
- Toolstation
- Toolstop
- UK Planet Tools

## Getting started

1. **Clone the repository**

   ```bash
   git clone https://github.com/DustInTheDark/tooltally-scrapers.git
   cd tooltally-scrapers
   ```

2. **Install dependencies**

   Create and activate a virtual environment (optional but recommended) and
   install the required packages. The project now includes a
   `requirements.txt` listing all Python dependencies, including Flask for the
   API:

   ```bash
   pip install -r requirements.txt
   ```

3. **(Optional) Configure PostgreSQL**

   The scraping pipeline stores data in a local SQLite database. If you would
   like to mirror the schema in PostgreSQL for use with other ToolTally
   services, create a `.env` file containing a `DATABASE_URL` and initialise the
   database:

   ```bash
   echo "DATABASE_URL=postgresql://postgres:tooltally@localhost:5432/tooltally" > .env
   python scripts/init_db.py
   ```

## Running the scrapers

Each vendor has a dedicated script in the `scripts/` directory. Run them from
the project root to populate `data/tooltally.db`:

```bash
python scripts/scrape_dandm.py          # D&M Tools
python scripts/scrape_screwfix.py       # Screwfix
python scripts/scrape_toolstation.py    # Toolstation
python scripts/scrape_toolstop.py       # Toolstop
python scripts/scrape_ukplanettools.py  # UK Planet Tools
```

Each script clears any proxy environment variables that might interfere with
Scrapy's downloader and will create the SQLite database (and required tables)
if it does not already exist. After a spider completes, the scraped products
can be inspected using standard SQLite tooling, for example:

```bash
sqlite3 data/tooltally.db 'SELECT vendor_id, name, price FROM products LIMIT 10;'
```

## Running the API

The repository includes a small Flask application (`app.py`) that serves the
contents of `data/tooltally.db` for consumption by the front end. Set up a
virtual environment, install dependencies, and start the development server:

```bash
py -3 -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt

set FLASK_APP=app.py
set FLASK_ENV=development
set SQLALCHEMY_DATABASE_URI=sqlite:///tooltally.db
flask run --host=0.0.0.0 --port=5000
```

Key endpoints:

- `GET /products` – List deduplicated products. Supports optional `search` and
  `category` query parameters for filtering.
- `GET /products/<id>` – Return vendor prices and buy links for a product.
- `GET /categories` – List all available categories.

Set `SQLALCHEMY_DATABASE_URI` to point to a different SQLite database file if
required.

## Notes

- Running the scrapers against live retail sites may take a while and be
  subject to rate limiting. Be considerate and adhere to each site's terms of
  use.
- The scripts are intended for development and data collection purposes; they
  are not a production‑grade, continuously running crawler.
  