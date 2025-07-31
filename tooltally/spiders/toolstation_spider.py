import re
import scrapy

class ToolstationSpider(scrapy.Spider):
    name = "toolstation"
    allowed_domains = ["toolstation.com"]
    # Start URLs: the specified Toolstation category pages
    start_urls = [
        "https://www.toolstation.com/hand-tools/sawing-cutting/c674",
        "https://www.toolstation.com/hand-tools/plumbing-tools/c672",
        "https://www.toolstation.com/hand-tools/electrical-tools/c39",
        "https://www.toolstation.com/hand-tools/engineering-tools/c673",
        "https://www.toolstation.com/hand-tools/hammers/c27",
        "https://www.toolstation.com/hand-tools/pliers/c30",
    ]

    def parse(self, response):
        # Derive category name from URL (e.g. "sawing-cutting" -> "Sawing Cutting")
        url_path = response.url.split('?')[0]                 # drop any query params (for pagination)
        parts = url_path.rstrip('/').split('/')
        category_slug = parts[-2] if parts[-1].startswith('c') else parts[-1]
        category_name = category_slug.replace('-', ' ').title()

        # Iterate over each product card on the page
        for card in response.css("[data-testid='product-card']"):
            # Extract product code from the "Product code: XXXX" text
            code = card.css("::text").re_first(r"Product code:\s*([\w-]+)")
            if not code:
                continue  # skip if not found (safety check)

            # Find the product name and URL from the product link (href contains '/p')
            name_link = card.xpath(".//a[contains(@href, '/p')][1]")  # first product link after code
            product_name = name_link.xpath("normalize-space(string())").get(default="")
            # Remove any leading discount text (e.g. "10% Off") from the name
            product_name = re.sub(r"^\d+% Off\s*", "", product_name).strip()

            product_url = response.urljoin(name_link.xpath("@href").get(default=""))

            # Extract the price text (current price, excluding any "was £X" old price)
            price_text = card.xpath(".//*[contains(text(), '£') and not(contains(text(), 'VAT'))][1]/text()").get()
            if price_text:
                price_text = price_text.strip()
                # If there is an old price, remove it (keep only the first price before "was")
                if "was" in price_text:
                    price_text = price_text.split(" was")[0].strip()

            # Yield the item with the expected fields
            yield {
                "vendorName": "Toolstation",
                "product_code": code,
                "name": product_name,
                "price": price_text,
                "category": category_name,
                "url": product_url
            }

        # Follow the "Next Page" link if it exists
        next_page = response.css("a[data-testid='show-next-link']::attr(href)").get()
        if next_page:
            # Use response.follow to retain meta (if any) and navigate to the next page
            yield response.follow(next_page, callback=self.parse)
