import sqlite3

class DatabasePipeline:
    def open_spider(self, spider):
        self.connection = sqlite3.connect("tooltally.db")
        self.cursor = self.connection.cursor()
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                productTitle TEXT,
                price TEXT,
                vendorName TEXT,
                buyUrl TEXT,
                sku TEXT UNIQUE
            )
        """)
        self.connection.commit()

    def close_spider(self, spider):
        self.connection.commit()
        self.connection.close()

    def process_item(self, item, spider):
        self.cursor.execute("""
            INSERT OR IGNORE INTO products (productTitle, price, vendorName, buyUrl, sku)
            VALUES (?, ?, ?, ?, ?)
        """, (
            item.get("productTitle"),
            item.get("price"),
            item.get("vendorName"),
            item.get("buyUrl"),
            item.get("sku"),
        ))
        return item
