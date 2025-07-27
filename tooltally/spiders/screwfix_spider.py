import json
import os
from urllib.parse import urlencode

import scrapy


class ScrewfixSpider(scrapy.Spider):
    name = "screwfix"
    allowed_domains = ["screwfix.com"]
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'USER_AGENT': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:117.0) '
                       'Gecko/20100101 Firefox/117.0'),
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-GB,en;q=0.5',
        },
    }

    def __init__(self, query=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # query is ignored when scraping everything
        self.items = []

    async def start(self):
        # Start at the home page to gather category links
        yield scrapy.Request("https://www.screwfix.com/", callback=self.parse_home)

    def parse_home(self, response):
        # Find links that look like category pages – adjust the selector as needed
        for href in response.css("a[href*='/c/']::attr(href)").getall():
            yield response.follow(href, callback=self.parse_category)

    def parse_category(self, response):
        # Scrape products on this category page (same as your original parse)
        for product in response.css("div.product, li.product, div.ProductListItem"):
            title_parts = product.css("a ::text").getall()
            title = " ".join(t.strip() for t in title_parts if t.strip())
            price = product.css("span.price, span.Price::text, .price::text").get()
            url = product.css("a::attr(href)").get()
            if url:
                url = response.urljoin(url)
            item = {
                "productTitle": title,
                "price": price.strip() if price else "",
                "vendorName": "Screwfix",
                "buyUrl": url,
            }
            self.items.append(item)
            yield item

        # Follow pagination links
        next_page = response.css("a.pagination--next::attr(href), a[rel=next]::attr(href)").get()
        if next_page:
            yield response.follow(next_page, callback=self.parse_category)

    def closed(self, reason):
        os.makedirs("output", exist_ok=True)
        with open("output/products.json", "w", encoding="utf-8") as f:
            json.dump(self.items, f, ensure_ascii=False, indent=2)

