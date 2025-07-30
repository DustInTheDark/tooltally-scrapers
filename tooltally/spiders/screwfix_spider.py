import scrapy
import json

class ScrewfixSpider(scrapy.Spider):
    name = "screwfix"
    allowed_domains = ["screwfix.com"]
    start_urls = [
        "https://www.screwfix.com/c/tools/drills/cat830704",
        "https://www.screwfix.com/c/tools/saws/cat830716",
        "https://www.screwfix.com/c/tools/hand-tools/cat830992",
        "https://www.screwfix.com/c/tools/power-tools/cat830692",
        "https://www.screwfix.com/c/tools/power-tool-accessories/cat830036",
        "https://www.screwfix.com/c/tools/tool-storage/cat831040",
        "https://www.screwfix.com/c/tools/measuring-tools/cat9260004",
        "https://www.screwfix.com/c/tools/testing-equipment/cat8830001",
    ]

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url,
                callback=self.parse,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/114.0.0.0 Safari/537.36"
                    )
                }
            )

    def parse(self, response):
        json_ld = response.xpath('//script[@type="application/ld+json"]/text()').get()
        if json_ld:
            try:
                data = json.loads(json_ld)
                for product in data.get("itemListElement", []):
                    yield {
                        "productTitle": product.get("name"),
                        "price": product.get("offers", {}).get("price"),
                        "vendorName": "Screwfix",
                        "buyUrl": response.urljoin(product.get("url")),
                        "sku": product.get("sku"),
                    }
            except json.JSONDecodeError:
                self.logger.warning("Failed to parse JSON-LD at %s", response.url)

        next_page = response.xpath('//a[@data-qaid="pagination-button-next"]/@href').get()
        if next_page:
            yield response.follow(
                next_page,
                callback=self.parse,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/114.0.0.0 Safari/537.36"
                    )
                }
            )