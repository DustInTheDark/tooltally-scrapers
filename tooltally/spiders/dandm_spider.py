import os
import sqlite3
import requests
from bs4 import BeautifulSoup

def main():
    # Define path to the SQLite database (create if it doesn't exist)
    db_path = os.path.join(os.path.dirname(__file__), 'tooltally.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    # Ensure the products table exists with the required schema
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY,
            productTitle TEXT,
            price TEXT,
            vendorName TEXT,
            buyUrl TEXT,
            sku TEXT,
            category TEXT
        )
    """)
    conn.commit()  # Save the table creation if it was just created

    # List of categories to scrape: (Category Name, URL)
    categories = [
        ("Cordless Combi Hammers", "https://www.dm-tools.co.uk/Cordless-Combi-Hammers/C410519"),
        ("Cordless Impact Wrenches", "https://www.dm-tools.co.uk/Cordless-Impact-Wrenches/C410523"),
        ("Cordless Impact Drivers", "https://www.dm-tools.co.uk/Cordless-Impact-Drivers/C410521"),
    ]

    # HTTP headers to mimic a real browser
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9"
    }

    for category_name, url in categories:
        try:
            print(f"Scraping category: {category_name} ({url})")
            response = requests.get(url, headers=headers, timeout=15)
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            continue
        if response.status_code != 200:
            print(f"Failed to retrieve {url} (status code {response.status_code})")
            continue

        # Parse the page content with BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        # Find all product containers on the page
        product_divs = soup.find_all('div', class_='productmini_container')
        if not product_divs:
            print(f"No products found for category '{category_name}'.")
            continue

        for prod_div in product_divs:
            # Get product title and link
            title_tag = prod_div.find('a', class_='productmini_title')
            if not title_tag:
                continue  # skip if structure is not as expected
            product_title = title_tag.get_text(strip=True)
            relative_link = title_tag.get('href', '')
            # Construct full product URL if a relative path is given
            if relative_link.startswith('/'):
                product_url = "https://www.dm-tools.co.uk" + relative_link
            else:
                product_url = relative_link

            # Get the brand name from the brand image alt text (if available)
            brand_name = None
            brand_div = prod_div.find('div', class_='productmini_brand')
            if brand_div:
                brand_img = brand_div.find('img')
                if brand_img and brand_img.get('alt'):
                    brand_name = brand_img['alt'].strip()

            # Derive SKU (model code) by removing brand from title and taking the first token after the brand
            sku = ""
            if brand_name:
                # If the title begins with the brand name, remove it to isolate the model
                if product_title.lower().startswith(brand_name.lower()):
                    after_brand = product_title[len(brand_name):].strip()
                    # Remove a leading hyphen if present (e.g. "Brand - Model")
                    if after_brand.startswith('-'):
                        after_brand = after_brand[1:].strip()
                    if after_brand:
                        # SKU is assumed to be the first word after the brand name
                        sku = after_brand.split()[0]
            # Fallback: If no brand found or SKU still empty, try extracting from URL pattern
            if not sku:
                # Many D&M Tools product URLs contain the model after the brand, e.g. /Brand-Model-Other-words/P12345
                parts = relative_link.strip('/').split('/')
                if len(parts) >= 2:
                    name_part = parts[-2]  # the part with "Brand-Model-Other-words"
                    tokens = name_part.split('-')
                    if tokens:
                        # Skip the first token (brand) and take the second token as model if available
                        if len(tokens) > 1:
                            sku = tokens[1]
                        else:
                            sku = tokens[0]

            # Extract the current price text
            price_text = ""
            price_div = prod_div.find('div', class_='productmini_price')
            if price_div:
                # Get text of price div (including any nested span for inc/ex)
                full_price_text = price_div.get_text(separator=" ", strip=True)
                # full_price_text might look like "NOW £259.95 INC £216.63 EX"
                if full_price_text.upper().startswith("NOW"):
                    # Remove the "NOW" prefix if present
                    full_price_text = full_price_text[3:].strip()  # remove "NOW":contentReference[oaicite:2]{index=2}
                # If there's an " INC " segment, strip it out to get only the main price (inc VAT)
                if " INC " in full_price_text:
                    price_text = full_price_text.split(" INC ")[0].strip()
                else:
                    price_text = full_price_text.strip()

            # Prepare data for insertion
            product_title = product_title.strip()
            price_text = price_text.strip()
            vendor_name = "D&M Tools"
            category = category_name

            # Check if this product (by URL and vendor) is already in the database
            cursor.execute("SELECT id FROM products WHERE buyUrl=? AND vendorName=?", (product_url, vendor_name))
            exists = cursor.fetchone()
            if exists:
                # Product already exists, skip insertion
                continue

            # Insert new product record into the database
            cursor.execute(
                "INSERT INTO products (productTitle, price, vendorName, buyUrl, sku, category) VALUES (?, ?, ?, ?, ?, ?)",
                (product_title, price_text, vendor_name, product_url, sku, category)
            ):contentReference[oaicite:3]{index=3}
            # Log the inserted product (optional)
            print(f"Inserted: {product_title} | {price_text} | {vendor_name} | {sku} | {category}")
            conn.commit()

    # Close the database connection after processing all categories
    conn.close()

# Run the scraper if executed directly
if __name__ == "__main__":
    main()
