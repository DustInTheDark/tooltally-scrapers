import json
import os
from urllib.parse import urlencode

import scrapy


class ScrewfixSpider(scrapy.Spider):
    name = "screwfix"
    allowed_domains = ["screwfix.com"]
    custom_settings = {
        'ROBOTSTXT_OBEY': False
    }

    def __init__(self, query=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Accept an empty query.  An empty or "all" query will trigger a full scrape.
        self.query = query or ""
        self.items = []

    def start_requests(self):
        # If no specific search term, start at the category listing page
        if not self.query or self.query.lower() == "all":
            url = "https://www.screwfix.com/c/"
            yield scrapy.Request(url, callback=self.parse_categories)
        else:
            from urllib.parse import urlencode
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

    def parse_categories(self, response):
        # Find every category link on the page and follow it
        for link in response.css("a.category-link::attr(href)").getall():
            yield response.follow(link, callback=self.parse)

            next_page = response.css("a.pagination--next::attr(href), a[rel=next]::attr(href)").get()
            if next_page:
                yield response.follow(next_page, callback=self.parse)

    def closed(self, reason):
        os.makedirs("output", exist_ok=True)
        with open("output/products.json", "w", encoding="utf-8") as f:
            json.dump(self.items, f, ensure_ascii=False, indent=2)

