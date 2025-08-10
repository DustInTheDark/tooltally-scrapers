# scripts/scrape_ukplanettools.py
from __future__ import annotations

# ---- bootstrap so this runs via `python scripts/...py` OR `python -m ...` ----
import os, sys
if __package__ in (None, ""):
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
# -----------------------------------------------------------------------------

import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from scrapy import signals
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from scripts.raw_offers_writer import save_many_raw_offers


VENDOR = "UK Planet Tools"
SPIDER_NAME = "ukplanettools"


def _parse_price_to_float(v: Any) -> Optional[float]:
    """Accepts numbers like 149.99 or strings like '£149.99' and returns float pounds."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        f = float(v)
        return f if f >= 0 else None
    s = str(v).strip()
    if not s:
        return None
    m = re.search(r"([0-9]+(?:[.,][0-9]{1,2})?)", s.replace(",", ""))
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def _norm_item(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Map a Scrapy item/dict to our staging shape; None = skip."""
    title = item.get("title") or item.get("name") or item.get("raw_title")
    url = item.get("url") or item.get("product_url") or item.get("link")
    sku = item.get("sku") or item.get("vendor_sku") or item.get("mpn") or item.get("model")
    category = item.get("category") or item.get("category_name")

    price = (
        item.get("price_gbp")
        or item.get("price_pounds")
        or item.get("price")
        or item.get("current_price")
        or item.get("amount")
    )
    price_gbp = _parse_price_to_float(price)

    # minimal required fields
    if not title or not url or price_gbp is None:
        return None

    return {
        "vendor": VENDOR,
        "title": str(title),
        "price_pounds": price_gbp,
        "url": str(url),
        "vendor_sku": (str(sku) if sku else None),
        "category_name": (str(category) if category else None),
        "scraped_at": datetime.utcnow().isoformat(),
    }


def _try_import_spider_class():
    """Fallback import if spider name isn't registered in the project."""
    try:
        from tooltally.spiders.ukplanettools import UKPlanetToolsSpider  # type: ignore
        return UKPlanetToolsSpider
    except Exception:
        return None


def run() -> None:
    settings = get_project_settings()
    process = CrawlerProcess(settings=settings)

    rows: List[Dict[str, Any]] = []
    seen: set[Tuple[str, Optional[str]]] = set()  # (url, sku) dedupe per run

    def on_item_scraped(item, response, spider):
        norm = _norm_item(dict(item))
        if not norm:
            return
        key = (norm["url"], norm.get("vendor_sku"))
        if key in seen:
            return
        seen.add(key)
        rows.append(norm)

    # Hook signal before starting
    process.signals.connect(on_item_scraped, signal=signals.item_scraped)

    # Prefer crawling by spider name
    try:
        process.crawl(SPIDER_NAME)
    except KeyError:
        # Fallback to importing a spider class
        spider_cls = _try_import_spider_class()
        if not spider_cls:
            raise RuntimeError(
                f"Could not find Scrapy spider '{SPIDER_NAME}' or import fallback class."
            )
        process.crawl(spider_cls)

    process.start()  # blocks until finished

    if rows:
        inserted = save_many_raw_offers(rows)
        print(f"[{VENDOR}] inserted {inserted} raw offers")
    else:
        print(f"[{VENDOR}] no rows scraped; nothing inserted.")


if __name__ == "__main__":
    run()
