import scrapy


class ToolstopSpider(scrapy.Spider):
    """Spider for scraping product data from Toolstop."""

    name = "toolstop"
    allowed_domains = ["toolstop.co.uk"]

    # Hardcoded category URLs to scrape
    start_urls = [
        "https://www.toolstop.co.uk/power-tools/corded-power-tools/corded-drills/",
        "https://www.toolstop.co.uk/power-tools/corded-power-tools/angle-grinders/",
        "https://www.toolstop.co.uk/power-tools/corded-power-tools/impact-wrenches/",
        "https://www.toolstop.co.uk/power-tools/corded-power-tools/reciprocating-saws/",
        "https://www.toolstop.co.uk/power-tools/cordless-power-tools/naked-body-only-tools/",
        "https://www.toolstop.co.uk/power-tools/cordless-power-tools/cordless-combi-drills/",
        "https://www.toolstop.co.uk/power-tools/cordless-power-tools/cordless-impact-drivers/",
        "https://www.toolstop.co.uk/power-tools/cordless-power-tools/cordless-impact-wrenches/",
        "https://www.toolstop.co.uk/power-tools/cordless-power-tools/cordless-circular-saws/",
        "https://www.toolstop.co.uk/power-tools/cordless-power-tools/cordless-angle-grinders/",
        "https://www.toolstop.co.uk/power-tools/cordless-power-tools/cordless-reciprocating-saws/",
        "https://www.toolstop.co.uk/power-tools/cordless-power-tools/cordless-jigsaws/",
        "https://www.toolstop.co.uk/hand-tools/woodworking-tools/",
    ]

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/114.0.0.0 Safari/537.36"
        )
    }

    def start_requests(self):  # type: ignore[override]
        """Send the initial requests with custom headers."""
        for url in self.start_urls:
            yield scrapy.Request(url, headers=self.headers, callback=self.parse)

    async def start(self):  # type: ignore[override]
        """Compatibility for Scrapy 2.13+ asynchronous start."""
        for req in self.start_requests():
            yield req

    def parse(self, response):  # type: ignore[override]
        """Parse a listing page, yield product info, and follow pagination."""
        for product in response.css("li.product article.card"):
            sku = product.attrib.get("data-sku", "").strip()
            name = product.attrib.get("data-product-name", "").strip()
            category = product.attrib.get("data-product-category", "").strip()
            price_raw = product.attrib.get("data-product-price", "").strip()
            price = f"£{price_raw}" if price_raw else ""
            link = product.css("a::attr(href)").get()
            url = response.urljoin(link) if link else response.url

            yield {
                "vendorName": "Toolstop",
                "product_code": sku,
                "name": name,
                "price": price,
                "category": category,
                "url": url,
            }

        next_page = response.css("li.pagination-item--next a.pagination-link::attr(href)").get()
        if next_page:
            yield response.follow(next_page, headers=self.headers, callback=self.parse)