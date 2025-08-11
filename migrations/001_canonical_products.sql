-- migrations/001_canonical_products.sql
PRAGMA foreign_keys = ON;

-- Canonical products table (one row per unique physical product/variant)
CREATE TABLE IF NOT EXISTS products (
  id                INTEGER PRIMARY KEY,
  brand             TEXT NOT NULL,
  mpn               TEXT,
  ean               TEXT UNIQUE,
  name              TEXT NOT NULL,
  category_id       INTEGER,
  variant_signature TEXT NOT NULL,
  normalized_key    TEXT UNIQUE NOT NULL,
  specs_json        TEXT,
  created_at        DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at        DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (category_id) REFERENCES categories(id)
);

-- Vendors (distinct sellers)
CREATE TABLE IF NOT EXISTS vendors (
  id       INTEGER PRIMARY KEY,
  name     TEXT NOT NULL UNIQUE,
  slug     TEXT UNIQUE,
  site_url TEXT
);

-- Offers (one row per vendor's active listing for a product)
CREATE TABLE IF NOT EXISTS offers (
  id             INTEGER PRIMARY KEY,
  product_id     INTEGER NOT NULL,
  vendor_id      INTEGER NOT NULL,
  vendor_sku     TEXT,
  price_cents    INTEGER NOT NULL,
  currency       TEXT DEFAULT 'GBP',
  buy_url        TEXT NOT NULL,
  in_stock       INTEGER,
  shipping_cents INTEGER,
  scraped_at     DATETIME NOT NULL,
  UNIQUE (vendor_id, vendor_sku),
  FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE,
  FOREIGN KEY (vendor_id) REFERENCES vendors(id)  ON DELETE CASCADE
);

-- Optional alias mapping (raw titles -> canonical product)
CREATE TABLE IF NOT EXISTS product_aliases (
  id               INTEGER PRIMARY KEY,
  product_id       INTEGER NOT NULL,
  alias_name       TEXT NOT NULL,
  source_vendor_id INTEGER,
  confidence       REAL,
  UNIQUE (alias_name, source_vendor_id),
  FOREIGN KEY (product_id) REFERENCES products(id),
  FOREIGN KEY (source_vendor_id) REFERENCES vendors(id)
);

-- Categories
CREATE TABLE IF NOT EXISTS categories (
  id   INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  slug TEXT UNIQUE
);

-- Staging table for scraper output (raw rows go here first)
CREATE TABLE IF NOT EXISTS raw_offers (
  id              INTEGER PRIMARY KEY,
  vendor          TEXT NOT NULL,
  raw_title       TEXT NOT NULL,
  price_cents     INTEGER NOT NULL,
  currency        TEXT DEFAULT 'GBP',
  buy_url         TEXT NOT NULL,
  vendor_sku      TEXT,
  category_name   TEXT,
  scraped_at      DATETIME NOT NULL,
  processed       INTEGER DEFAULT 0,  -- 0 = pending, 1 = processed by resolver
  resolved_product_id INTEGER,
  FOREIGN KEY (resolved_product_id) REFERENCES products(id)
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_offers_product      ON offers(product_id);
CREATE INDEX IF NOT EXISTS idx_offers_vendor       ON offers(vendor_id);
CREATE INDEX IF NOT EXISTS idx_products_normkey    ON products(normalized_key);
CREATE INDEX IF NOT EXISTS idx_raw_offers_processed ON raw_offers(processed);

-- Full-text search for product discovery (optional but nice)
CREATE VIRTUAL TABLE IF NOT EXISTS products_fts USING fts5(
  name,
  brand,
  mpn,
  content='products',
  content_rowid='id'
);

-- Triggers to keep FTS in sync
CREATE TRIGGER IF NOT EXISTS products_ai AFTER INSERT ON products BEGIN
  INSERT INTO products_fts(rowid, name, brand, mpn)
  VALUES (new.id, new.name, new.brand, new.mpn);
END;

CREATE TRIGGER IF NOT EXISTS products_ad AFTER DELETE ON products BEGIN
  INSERT INTO products_fts(products_fts, rowid, name, brand, mpn)
  VALUES ('delete', old.id, old.name, old.brand, old.mpn);
END;

CREATE TRIGGER IF NOT EXISTS products_au AFTER UPDATE ON products BEGIN
  INSERT INTO products_fts(products_fts, rowid, name, brand, mpn)
  VALUES ('delete', old.id, old.name, old.brand, old.mpn);
  INSERT INTO products_fts(rowid, name, brand, mpn)
  VALUES (new.id, new.name, new.brand, new.mpn);
END;
