import sqlite3
import os
from datetime import datetime

# Path to the SQLite database file
DB_PATH = os.path.join('data', 'tooltally.db')

def connect_db():
    """Connect to the SQLite database (creating the file and directory if needed) and enable foreign keys."""
    # Ensure the data directory exists
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    # Enable foreign key constraint support
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def create_schema(conn):
    """Create database tables with required schema if they do not already exist."""
    cursor = conn.cursor()
    # Create vendors table (unique vendor names)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS vendors (
        id      INTEGER PRIMARY KEY,
        name    TEXT UNIQUE
    )
    """)
    # Create products table (with unique constraints on key identifiers)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS products (
        id          INTEGER PRIMARY KEY,
        name        TEXT,
        brand       TEXT,
        sku         TEXT,
        mpn         TEXT,
        gtin        TEXT,
        category    TEXT,
        description TEXT,
        image_url   TEXT,
        UNIQUE(mpn, brand),
        UNIQUE(gtin),
        UNIQUE(name, brand)
    )
    """)
    # Create vendor_products table (linking vendors to products with pricing and availability)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS vendor_products (
        id            INTEGER PRIMARY KEY,
        vendor_id     INTEGER,
        product_id    INTEGER,
        price         REAL,
        currency      TEXT,
        availability  TEXT,
        buy_url       TEXT,
        last_updated  TEXT,
        UNIQUE(vendor_id, product_id),
        FOREIGN KEY(vendor_id) REFERENCES vendors(id) ON DELETE CASCADE,
        FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE
    )
    """)
    conn.commit()

def get_or_create_vendor(conn, vendor_name):
    """Lookup a vendor by name, inserting it if not found. Returns the vendor's ID."""
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM vendors WHERE name = ?", (vendor_name,))
    row = cursor.fetchone()
    if row:
        return row[0]
    # Insert new vendor if not exists
    cursor.execute("INSERT INTO vendors(name) VALUES(?)", (vendor_name,))
    conn.commit()
    return cursor.lastrowid

def get_or_create_product(conn, name, brand, sku=None, mpn=None, gtin=None, category=None, description=None, image_url=None):
    """
    Lookup a product by key identifiers (GTIN, MPN+brand, SKU, name+brand). 
    If not found, insert a new product record. Returns the product's ID.
    """
    cursor = conn.cursor()
    # Try matching by GTIN (global trade item number)
    if gtin:
        cursor.execute("SELECT id FROM products WHERE gtin = ?", (gtin,))
        row = cursor.fetchone()
        if row:
            return row[0]
    # Try matching by MPN (manufacturer part number) and brand
    if mpn and brand:
        cursor.execute("SELECT id FROM products WHERE mpn = ? AND brand = ?", (mpn, brand))
        row = cursor.fetchone()
        if row:
            return row[0]
    # Try matching by SKU (possibly unique per vendor; in this design SKU is stored per product)
    if sku:
        cursor.execute("SELECT id FROM products WHERE sku = ?", (sku,))
        row = cursor.fetchone()
        if row:
            return row[0]
    # Try matching by name and brand as a last resort
    if name and brand:
        cursor.execute("SELECT id FROM products WHERE name = ? AND brand = ?", (name, brand))
        row = cursor.fetchone()
        if row:
            return row[0]
    # If no existing product matches, insert a new product record
    cursor.execute("""
        INSERT INTO products(name, brand, sku, mpn, gtin, category, description, image_url)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (name, brand, sku, mpn, gtin, category, description, image_url))
    conn.commit()
    return cursor.lastrowid

def upsert_vendor_product(conn, vendor_id, product_id, price, currency, availability, buy_url):
    """
    Insert or update the vendor-specific product info (price, availability, URL, timestamp).
    If a record for the given vendor and product exists, it will be updated; otherwise inserted.
    """
    cursor = conn.cursor()
    timestamp = datetime.utcnow().isoformat()  # current timestamp in ISO format
    # Attempt to update an existing vendor_product record
    cursor.execute("""
        UPDATE vendor_products
        SET price = ?, currency = ?, availability = ?, buy_url = ?, last_updated = ?
        WHERE vendor_id = ? AND product_id = ?
    """, (price, currency, availability, buy_url, timestamp, vendor_id, product_id))
    if cursor.rowcount == 0:
        # No existing record updated, so insert a new record
        cursor.execute("""
            INSERT INTO vendor_products(vendor_id, product_id, price, currency, availability, buy_url, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (vendor_id, product_id, price, currency, availability, buy_url, timestamp))
    conn.commit()
