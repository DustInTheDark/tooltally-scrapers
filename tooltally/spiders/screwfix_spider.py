"""
Wrapper to launch the Screwfix spider.

If a search term is provided as the first command‑line argument, it will be
passed to the spider.  If no argument is provided, an empty string is used,
triggering the spider to scrape all categories.
"""

import json
import os
import scrapy
from scrapy_playwright.page import PageMethod

import sys
from scrapy.cmdline import execute

if __name__ == "__main__":
    # allow a missing argument
    query = sys.argv[1] if len(sys.argv) > 1 else ""
    execute(["scrapy", "crawl", "screwfix", "-a", f"query={query}"])

class ScrewfixSpider(scrapy.Spider):
    name = "screwfix"
    allowed_domains = ["screwfix.com"]

    # Override settings for this spider
    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "USER_AGENT": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:117.0) "
                       "Gecko/20100101 Firefox/117.0"),
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        "PLAYWRIGHT_BROWSER_TYPE": "chromium",
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 60000,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
        "DOWNLOAD_DELAY": 1,
    }

    def __init__(self, query="", *args, **kwargs):
        super().__init__(*args, **kwargs)
        # query is ignored – we crawl everything
        self.items = []

    async def start(self):
        # Use Playwright to render the home page and extract category links
        yield scrapy.Request(
            "https://www.screwfix.com/",
            meta={
                "playwright": True,
                "playwright_page_methods": [
                    PageMethod("wait_for_selector", "a[href*='/c/']")
                ],
            },
            callback=self.parse_home,
        )

    def parse_home(self, response):
        category_links = response.css("a[href*='/c/']::attr(href)").getall()
        for href in category_links:
            url = response.urljoin(href)
            yield scrapy.Request(
                url,
                meta={
                    "playwright": True,
                    "playwright_page_methods": [
                        PageMethod("wait_for_selector", ".ProductListItem")
                    ],
                },
                callback=self.parse_category,
            )

    def parse_category(self, response):
        # Extract product details from a rendered category page
        for product in response.css(".ProductListItem"):
            title = " ".join(product.css("a ::text").getall()).strip()
            price = product.css(".price ::text, .Price::text").get()
            price = price.strip() if price else ""
            url = product.css("a::attr(href)").get()
            if url:
                url = response.urljoin(url)

            item = {
                "productTitle": title,
                "price": price,
                "vendorName": "Screwfix",
                "buyUrl": url,
            }
            self.items.append(item)
            yield item

        # Follow pagination links
        next_page = response.css("a.pagination--next::attr(href), a[rel=next]::attr(href)").get()
        if next_page:
            yield scrapy.Request(
                response.urljoin(next_page),
                meta=response.meta,
                callback=self.parse_category,
            )

    def closed(self, reason):
        os.makedirs("output", exist_ok=True)
        with open("output/products.json", "w", encoding="utf-8") as f:
            json.dump(self.items, f, ensure_ascii=False, indent=2)

