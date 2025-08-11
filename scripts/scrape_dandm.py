# scripts/scrape_dandm.py
from __future__ import annotations

import os, sys
if __package__ in (None, ""):
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import re
from urllib.parse import urlparse, unquote
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from scrapy import signals
from pydispatch import dispatcher
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from scripts.raw_offers_writer import save_many_raw_offers

VENDOR = "D&M Tools"
SPIDER_NAME = "dandm"  # adjust if your project uses a different name


def _parse_price_to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        f = float(v)
        return f if f >= 0 else None
    s = str(v).strip()
    m = re.search(r"([0-9]+(?:[.,][0-9]{1,2})?)", s.replace(",", ""))
    try:
        return float(m.group(1)) if m else None
    except ValueError:
        return None


def _title_from_url(url: str) -> Optional[str]:
    try:
        path = unquote(urlparse(url).path or "")
        seg = path.rstrip("/").rsplit("/", 1)[-1]
        seg = seg.split("?")[0].split("#")[0]
        text = seg.replace("-", " ").replace("_", " ").strip()
        return text or None
    except Exception:
        return None


def _norm_item(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    title = item.get("title") or item.get("name") or item.get("raw_title")
    url = item.get("url") or item.get("product_url") or item.get("link")
    sku = item.get("sku") or item.get("vendor_sku") or item.get("mpn") or item.get("model")
    category = item.get("category") or item.get("category_name")
    price = item.get("price_gbp") or item.get("price_pounds") or item.get("price") or item.get("current_price") or item.get("amount")
    price_gbp = _parse_price_to_float(price)

    if (not title) and url:
        title = _title_from_url(url)

    if not title or not url or price_gbp is None:
        return None

    return {
        "vendor": VENDOR,
        "title": str(title),
        "price_pounds": price_gbp,
        "url": str(url),
        "vendor_sku": (str(sku) if sku else None),
        "category_name": (str(category) if category else None),
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }


def _try_import_spider_class():
    for mod, cls in [
        ("tooltally.spiders.dandm", "DandmSpider"),
        ("tooltally.spiders.dandmtools", "DandmToolsSpider"),
    ]:
        try:
            module = __import__(mod, fromlist=[cls])
            return getattr(module, cls)
        except Exception:
            continue
    return None


def _make_process() -> CrawlerProcess:
    settings = get_project_settings()
    settings.set("ITEM_PIPELINES", {}, priority="cmdline")
    settings.set("EXTENSIONS", {"scrapy.extensions.telnet.TelnetConsole": None}, priority="cmdline")
    settings.set("DOWNLOAD_DELAY", 1, priority="cmdline")
    settings.set("CONCURRENT_REQUESTS_PER_DOMAIN", 1, priority="cmdline")
    return CrawlerProcess(settings=settings)


def run() -> None:
    process = _make_process()

    rows: List[Dict[str, Any]] = []
    seen: set[Tuple[str, Optional[str]]] = set()

    def on_item_scraped(item, response, spider):
        norm = _norm_item(dict(item))
        if not norm:
            return
        key = (norm["url"], norm.get("vendor_sku"))
        if key in seen:
            return
        seen.add(key)
        rows.append(norm)

    dispatcher.connect(on_item_scraped, signal=signals.item_scraped)

    try:
        process.crawl(SPIDER_NAME)
    except KeyError:
        spider_cls = _try_import_spider_class()
        if not spider_cls:
            raise RuntimeError(f"Could not find spider '{SPIDER_NAME}' or import fallback class.")
        process.crawl(spider_cls)

    process.start()

    if rows:
        inserted = save_many_raw_offers(rows)
        print(f"[{VENDOR}] inserted {inserted} raw offers")
    else:
        print(f"[{VENDOR}] no rows scraped; nothing inserted.")


if __name__ == "__main__":
    run()
