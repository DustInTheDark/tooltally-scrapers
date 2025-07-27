# 🕷️ ToolTally Scrapers

This repository contains scraping utilities for the [ToolTally](https://github.com/DustInTheDark/tooltally-frontend) project. The scrapers gather product data from various UK tool vendors.

## Features

- Individual spiders for each vendor
- Output JSON files that can be fed into the ToolTally database

## Getting Started

1. **Clone the repo**

```bash
git clone https://github.com/DustInTheDark/tooltally-scrapers.git
cd tooltally-scrapers
```

2. **Install dependencies**

```bash
pip install sqlalchemy psycopg2-binary python-dotenv scrapy
```

3. **Configure your environment**

Create a `.env` file with a `DATABASE_URL` pointing to your PostgreSQL instance, e.g.:

```
DATABASE_URL=postgresql://postgres:tooltally@localhost:5432/tooltally
```

4. **Start your PostgreSQL server**

If a local database isn't already running, you can launch one quickly using Docker:

```bash
docker run --name tooltally-db -e POSTGRES_PASSWORD=tooltally -e POSTGRES_DB=tooltally -p 5432:5432 -d postgres
```

The command above starts a PostgreSQL server with user `postgres`, password
`tooltally`, and a database also named `tooltally`. Ensure your `DATABASE_URL`
matches these values.

5. **Initialise the database**

With the PostgreSQL server running, create the tables by executing:

```bash
python scripts/init_db.py
```

6. **Run the Screwfix spider**

```bash
python scripts/scrape_screwfix.py drill
```
This saves the results to `output/products.json`.
