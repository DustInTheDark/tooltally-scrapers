# scripts/raw_offers_writer.py
from datetime import datetime
import os
import sqlite3

# Prefer your project's helper if available
try:
    from data.db import get_conn  # type: ignore
except Exception:
    def get_conn():
        db_path = os.environ.get(
            "DB_PATH",
            os.path.join(os.path.dirname(__file__), "..", "data", "tooltally.db"),
        )
        conn = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn


def _to_cents(price_pounds) -> int:
    return int(round(float(price_pounds) * 100))


def save_raw_offer(
    *,
    vendor: str,
    title: str,
    price_pounds: float,
    url: str,
    vendor_sku: str | None = None,
    category_name: str | None = None,
    scraped_at: str | None = None,
    currency: str = "GBP",
) -> None:
    """
    Insert ONE listing row into staging table raw_offers.
    """
    if not scraped_at:
        scraped_at = datetime.utcnow().isoformat()
    price_cents = _to_cents(price_pounds)

    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO raw_offers
              (vendor, raw_title, price_cents, currency, buy_url, vendor_sku, category_name, scraped_at)
            VALUES (?,?,?,?,?,?,?,?);
            """,
            (
                vendor,
                title,
                price_cents,
                currency,
                url,
                vendor_sku,
                category_name,
                scraped_at,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def save_many_raw_offers(rows: list[dict]) -> int:
    """
    Bulk insert many listing rows (faster when your scraper has dozens/hundreds).
    Each dict should have keys accepted by save_raw_offer.
    Returns number of inserted rows.
    """
    if not rows:
        return 0
    conn = get_conn()
    try:
        payload = []
        for r in rows:
            price_cents = _to_cents(r["price_pounds"])
            payload.append(
                (
                    r["vendor"],
                    r["title"],
                    price_cents,
                    r.get("currency", "GBP"),
                    r["url"],
                    r.get("vendor_sku"),
                    r.get("category_name"),
                    r.get("scraped_at") or datetime.utcnow().isoformat(),
                )
            )
        conn.executemany(
            """
            INSERT INTO raw_offers
              (vendor, raw_title, price_cents, currency, buy_url, vendor_sku, category_name, scraped_at)
            VALUES (?,?,?,?,?,?,?,?);
            """,
            payload,
        )
        conn.commit()
        return len(payload)
    finally:
        conn.close()
