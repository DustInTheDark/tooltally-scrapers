# items.py (define fields for each product item)
import scrapy

class ProductItem(scrapy.Item):
    vendorName = scrapy.Field()      # Name of the vendor/manufacturer
    product_code = scrapy.Field()    # Unique product code or SKU
    name = scrapy.Field()            # Product name/description
    price = scrapy.Field()           # Price (as float or string that can be cast to float)
    category = scrapy.Field()        # Category or type of the product
    url = scrapy.Field()             # URL of the product page
