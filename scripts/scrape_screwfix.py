import os
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

# Ensure the output directory exists
os.makedirs("output", exist_ok=True)

# Get project settings and configure feed export
settings = get_project_settings()
settings.set("FEEDS", {
    "output/products.json": {"format": "json", "encoding": "utf8", "indent": 4}
}, priority='project')

process = CrawlerProcess(settings)
# Start crawling with the Screwfix spider (identified by spider name)
process.crawl("screwfix")
process.start()
