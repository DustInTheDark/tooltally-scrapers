import os
import sqlite3
from itemadapter import ItemAdapter

class SQLitePipeline:
    def __init__(self):
        # Ensure the data directory exists
        db_dir = 'data'
        db_path = os.path.join(db_dir, 'tooltally.db')
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)
        # Connect to the SQLite database (this will create the file if it doesn't exist)
        self.conn = sqlite3.connect(db_path)
        self.cur = self.conn.cursor()
        # Create tables if they do not exist
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS vendors (
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE
            )
        """)
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY,
                vendor_id INTEGER,
                product_code TEXT,
                name TEXT,
                price REAL,
                category TEXT,
                url TEXT,
                FOREIGN KEY(vendor_id) REFERENCES vendors(id),
                UNIQUE(vendor_id, product_code)
            )
        """)
        # Commit table creation
        self.conn.commit()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        vendor_name = adapter.get('vendorName')
        if not vendor_name:
            spider.logger.error(f"Missing vendorName in item: {item}")
            return item  # Skip processing if vendor name is not provided
        # Ensure vendor exists in the vendors table
        self.cur.execute("SELECT id FROM vendors WHERE name = ?", (vendor_name,))
        result = self.cur.fetchone()
        if result:
            vendor_id = result[0]
        else:
            # Insert new vendor and get its ID
            self.cur.execute("INSERT INTO vendors (name) VALUES (?)", (vendor_name,))
            vendor_id = self.cur.lastrowid
        # Insert product record (or ignore if the (vendor_id, product_code) already exists)
        self.cur.execute("""
            INSERT OR IGNORE INTO products (vendor_id, product_code, name, price, category, url)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            vendor_id,
            adapter.get('product_code'),
            adapter.get('name'),
            adapter.get('price'),
            adapter.get('category'),
            adapter.get('url')
        ))
        # Commit the transaction (ensures data is saved to disk)
        self.conn.commit()
        return item

    def close_spider(self, spider):
        # Close the database connection when spider finishes
        self.conn.close()
