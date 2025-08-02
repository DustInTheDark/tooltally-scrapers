import os
import sqlite3
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

DB_PATH = os.path.join("data", "tooltally.db")

def ensure_db() -> None:
    """Ensure the SQLite database and required tables exist."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS vendors (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY,
            vendor_id INTEGER,
            product_code TEXT,
            name TEXT,
            price REAL,
            category TEXT,
            url TEXT,
            FOREIGN KEY(vendor_id) REFERENCES vendors(id),
            UNIQUE(vendor_id, product_code)
        )
        """
    )
    cur.execute("INSERT OR IGNORE INTO vendors(name) VALUES (?)", ("UK Planet Tools",))
    conn.commit()
    conn.close()

def main() -> None:
    # Remove proxy environment variables to avoid connect errors
    for var in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"):
        os.environ.pop(var, None)

    ensure_db()

    process = CrawlerProcess(get_project_settings())
    process.crawl("ukplanettools")
    process.start()

if __name__ == "__main__":
    main()