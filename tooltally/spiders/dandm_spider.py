import scrapy


class DandmSpider(scrapy.Spider):
    """Spider for scraping product data from D&M Tools."""

    name = "dandm"
    allowed_domains = ["dm-tools.co.uk"]

    # Mapping of category names to their listing URLs
    category_urls = {
        "Cordless Combi Hammers": "https://www.dm-tools.co.uk/Cordless-Combi-Hammers/C410519",
        "Cordless Impact Wrenches": "https://www.dm-tools.co.uk/Cordless-Impact-Wrenches/C410523",
        "Cordless Impact Drivers": "https://www.dm-tools.co.uk/Cordless-Impact-Drivers/C410521",
    }

    # Default headers to mimic a real browser
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }

    def start_requests(self):  # type: ignore[override]
        """Initiate requests for each category page."""

        for category, url in self.category_urls.items():
            yield scrapy.Request(
                url,
                callback=self.parse,
                headers=self.headers,
                meta={"category": category},
            )

    async def start(self):  # type: ignore[override]
        """Yield requests from ``start_requests`` (Scrapy 2.13+ compatibility)."""

        for request in self.start_requests():
            yield request

    def parse(self, response):  # type: ignore[override]
        """Parse a category page and yield product information."""

        category = response.meta.get("category", "")
        for prod in response.css("div.productmini_container"):
            title = prod.css("a.productmini_title::text").get()
            if not title:
                continue
            title = title.strip()

            link = prod.css("a.productmini_title::attr(href)").get("")
            url = response.urljoin(link)

            brand = prod.css("div.productmini_brand img::attr(alt)").get()

            sku = ""
            if brand and title.lower().startswith(brand.lower()):
                after_brand = title[len(brand):].strip()
                if after_brand.startswith("-"):
                    after_brand = after_brand[1:].strip()
                if after_brand:
                    sku = after_brand.split()[0]

            if not sku and link:
                parts = link.strip("/").split("/")
                if len(parts) >= 2:
                    tokens = parts[-2].split("-")
                    if len(tokens) > 1:
                        sku = tokens[1]
                    elif tokens:
                        sku = tokens[0]

            price_text = ""
            price_div = prod.css("div.productmini_price")
            if price_div:
                full_text = price_div.xpath("normalize-space()").get("")
                if full_text.upper().startswith("NOW"):
                    full_text = full_text[3:].strip()
                if full_text.upper().startswith("ONLY"):
                    full_text = full_text[4:].strip()
                if " INC " in full_text:
                    price_text = full_text.split(" INC ")[0].strip()
                else:
                    price_text = full_text.strip()

            yield {
                "vendorName": "D&M Tools",
                "product_code": sku,
                "name": title,
                "price": price_text,
                "category": category,
                "url": url,
            }

        next_page = response.css("a.next::attr(href), a.pagination_next::attr(href)").get()
        if next_page:
            yield response.follow(
                next_page,
                callback=self.parse,
                headers=self.headers,
                meta={"category": category},
            )