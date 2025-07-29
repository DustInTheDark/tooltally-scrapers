import json
import scrapy

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

    def parse(self, response):
        # Determine the category name from the page title (for context in items)
        title_text = response.xpath('//title/text()').get(default="")
        category_name = title_text.split("- Screwfix")[0].strip()
        if "|" in category_name:
            category_name = category_name.split("|")[0].strip()

        # Extract all JSON-LD scripts on the page
        scripts = response.xpath('//script[@type="application/ld+json"]/text()').getall()
        for script in scripts:
            try:
                data = json.loads(script)
            except json.JSONDecodeError:
                continue  # skip if JSON is invalid
            dtype = data.get("@type", "")
            # Skip breadcrumb JSON-LD or other non-item lists
            if dtype == "BreadcrumbList":
                continue
            # If this is an ItemList (list of products or subcategories)
            if dtype == "ItemList" and "itemListElement" in data:
                for element in data["itemListElement"]:
                    # Each element could be a Product or a ListItem referring to an item
                    if isinstance(element, dict):
                        # If it's a ListItem wrapper, extract the actual item
                        if element.get("@type") == "ListItem" and "item" in element:
                            item_obj = element["item"]
                        else:
                            item_obj = element
                        # If item_obj is a subcategory (link) rather than a product
                        if isinstance(item_obj, str) or (isinstance(item_obj, dict) and item_obj.get("@type") not in ["Product"]):
                            # Follow subcategory link if present
                            sub_url = item_obj if isinstance(item_obj, str) else item_obj.get("@id") or item_obj.get("url")
                            if sub_url:
                                yield response.follow(sub_url, callback=self.parse)
                            continue  # move to next element
                        # If we reach here, item_obj is a product dict
                        if item_obj.get("@type") == "Product":
                            # Extract product fields and yield item
                            yield from self._extract_product(item_obj, category_name)
            # If the JSON-LD directly describes a single Product (edge case on category page)
            elif dtype == "Product":
                yield from self._extract_product(data, category_name)

        # Follow pagination to next page if available
        next_page = response.xpath('//a[contains(@href, "page_start") and contains(text(), "Next")]/@href').get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)

    def _extract_product(self, product_data, category_name):
        """Helper method to extract fields from a Product JSON-LD object and yield an item dict."""
        name = product_data.get("name")
        sku = product_data.get("sku")
        mpn = product_data.get("mpn")
        # Find any GTIN field if present (gtin, gtin13, gtin14, etc.)
        gtin = None
        for key in ["gtin", "gtin13", "gtin14", "gtin12", "gtin8"]:
            if key in product_data:
                gtin = product_data.get(key)
                break
        # Extract brand name (could be a dict or string)
        brand = None
        if isinstance(product_data.get("brand"), dict):
            brand = product_data["brand"].get("name")
        elif product_data.get("brand"):
            brand = product_data.get("brand")
        description = product_data.get("description")
        # Image could be a URL string or a list of URLs; take the first if list
        image = product_data.get("image")
        image_url = image[0] if isinstance(image, list) else image
        # Offers may be a list (e.g., multiple offers for pickup/delivery) or a single object
        price = currency = availability = None
        url = product_data.get("url")  # product page URL
        offers = product_data.get("offers")
        if offers:
            if isinstance(offers, list):
                offer = offers[0]
            else:
                offer = offers
            price = offer.get("price")
            currency = offer.get("priceCurrency")
            availability = offer.get("availability")
            # Convert availability to a simple string (e.g., "InStock", "OutOfStock")
            if isinstance(availability, str) and "schema.org" in availability:
                availability = availability.split("/")[-1]
            # Use the offer's URL if available (usually same as product URL)
            if offer.get("url"):
                url = offer.get("url")
        else:
            # Some pages might not have an offers object; fall back to top-level fields
            price = product_data.get("price")
            currency = product_data.get("priceCurrency")
            availability = product_data.get("availability")
            if isinstance(availability, str) and "schema.org" in availability:
                availability = availability.split("/")[-1]
        # Construct the item dictionary
        item = {
            "vendorName": "Screwfix",
            "name": name,
            "sku": sku,
            "mpn": mpn,
            "gtin": gtin,
            "brand": brand,
            "category": category_name,
            "description": description,
            "image_url": image_url,
            "price": price,
            "currency": currency,
            "availability": availability,
            "url": url
        }
        yield item
