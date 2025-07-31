import sqlite3
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

def main():
    # Ensure the 'Toolstation' vendor exists in the database
    conn = sqlite3.connect("tooltally.db")
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS vendors(id INTEGER PRIMARY KEY, name TEXT UNIQUE)")
    cur.execute("INSERT OR IGNORE INTO vendors(name) VALUES (?)", ("Toolstation",))
    conn.commit()
    conn.close()

    # Set up and start the Scrapy crawler for the Toolstation spider
    process = CrawlerProcess(get_project_settings())
    process.crawl("toolstation")  # use the spider name defined in ToolstationSpider
    process.start()

if __name__ == "__main__":
    main()
