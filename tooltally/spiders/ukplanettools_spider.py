import scrapy
import re


class UkplanettoolsSpider(scrapy.Spider):
    name = "ukplanettools"
    allowed_domains = ["ukplanettools.co.uk"]
    start_urls = [
        "https://www.ukplanettools.co.uk/cordless-power-tools/cordless-drill/",
        "https://www.ukplanettools.co.uk/cordless-power-tools/drill-driver/",
        "https://www.ukplanettools.co.uk/cordless-power-tools/cordless-impact-wrench/",
        "https://www.ukplanettools.co.uk/cordless-power-tools/cordless-impact-driver/",
        "https://www.ukplanettools.co.uk/handtools/hammers/",
        "https://www.ukplanettools.co.uk/handtools/wrench-tool/",
        "https://www.ukplanettools.co.uk/handtools/pliers-strippers-snips-croppers/",
    ]

    # Default headers to mimic a regular browser
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/114.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }

    async def start(self):  # type: ignore[override]
        """Send initial requests with custom headers."""
        for url in self.start_urls:
            yield scrapy.Request(url, headers=self.headers)

    def parse(self, response):  # type: ignore[override]
        """Parse product listing pages and follow pagination."""
        # Derive category from the URL
        url_path = response.url.split("?")[0].rstrip("/")
        parts = url_path.split("/")
        if "p" in parts:
            p_index = parts.index("p")
            category_slug = parts[p_index - 1] if p_index > 0 else parts[-1]
        else:
            category_slug = parts[-1]
        category = category_slug.replace("-", " ").title()

        # Iterate over product cards
        for product in response.css("div[class^='ProductItem_productItem__']"):
            name = product.css("a[class^='ProductItem_productDetails__name__']::text").get()
            link = product.css("a[class^='ProductItem_productDetails__name__']::attr(href)").get()
            if not link:
                continue
            product_url = response.urljoin(link)
            # Use the slug from the URL as a product code
            slug = product_url.rstrip("/").split("/")[-1].replace(".html", "")
            price_text = product.css("div[class^='ProductItem_price__']::text").get()
            if price_text:
                price_text = price_text.strip()

            yield {
                "vendorName": "UK Planet Tools",
                "product_code": slug,
                "name": (name or "").strip(),
                "price": price_text,
                "category": category,
                "url": product_url,
            }

        # Follow pagination links
        next_page = response.xpath("//a[span[contains(text(), 'Next Page')]]/@href").get()
        if next_page:
            yield response.follow(next_page, headers=self.headers, callback=self.parse)