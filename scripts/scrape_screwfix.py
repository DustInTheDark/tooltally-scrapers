import sys
from scrapy.cmdline import execute

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/scrape_screwfix.py <query>")
        sys.exit(1)
    query = sys.argv[1]
    execute(["scrapy", "crawl", "screwfix", "-a", f"query={query}"])
