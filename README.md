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
DATABASE_URL=postgresql://user:password@localhost:5432/tooltally
```

4. **Initialise the database**

```bash
python -c "from tooltally import init_db; init_db()"
```

5. **Run a spider**

```bash
scrapy crawl screwfix -a query=drill -o output/products.json
```