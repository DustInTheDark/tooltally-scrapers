from tooltally import db

class DatabasePipeline:
    def __init__(self):
        # Initialize database connection and ensure schema exists
        self.conn = db.connect_db()
        db.create_schema(self.conn)

    def process_item(self, item, spider):
        # 1. Look up or insert the vendor
        vendor_name = item.get('vendorName')
        vendor_id = db.get_or_create_vendor(self.conn, vendor_name)
        # 2. Look up or insert the product
        product_id = db.get_or_create_product(
            self.conn,
            name=item.get('name'),
            brand=item.get('brand'),
            sku=item.get('sku'),
            mpn=item.get('mpn'),
            gtin=item.get('gtin'),
            category=item.get('category'),
            description=item.get('description'),
            image_url=item.get('image_url')
        )
        # 3. Insert or update the vendor’s price/info for the product
        price = item.get('price')
        currency = item.get('currency')
        availability = item.get('availability')
        buy_url = item.get('url')
        db.upsert_vendor_product(self.conn, vendor_id, product_id, price, currency, availability, buy_url)
        return item

    def close_spider(self, spider):
        # Close the database connection when the spider finishes
        if self.conn:
            self.conn.close()
