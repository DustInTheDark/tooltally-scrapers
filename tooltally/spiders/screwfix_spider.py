import json
import os
from urllib.parse import urlencode

import scrapy


class ScrewfixSpider(scrapy.Spider):
    name = "screwfix"
    allowed_domains = ["screwfix.com"]

    # Disable robots.txt rules for this spider so it can crawl the search pages.
    custom_settings = {
        'ROBOTSTXT_OBEY': False
    }

    def __init__(self, query=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not query:
            raise ValueError("query parameter is required")
        self.query = query
        self.items = []

    def start_requests(self):
        params = urlencode({"search": self.query})
        url = f"https://www.screwfix.com/search?{params}"
        yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        for product in response.css("div.product, li.product, div.ProductListItem"):
            title = (
                product.css("a ::text").getall()
            )  # gather all text from anchor
            title = " ".join(t.strip() for t in title if t.strip())
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

        next_page = response.css("a.pagination--next::attr(href), a[rel=next]::attr(href)").get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)

    def closed(self, reason):
        os.makedirs("output", exist_ok=True)
        with open("output/products.json", "w", encoding="utf-8") as f:
            json.dump(self.items, f, ensure_ascii=False, indent=2)

