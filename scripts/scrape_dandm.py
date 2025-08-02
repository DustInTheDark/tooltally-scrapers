from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


def main() -> None:
    """Run the D&M Tools spider using the project's settings."""

    process = CrawlerProcess(get_project_settings())
    process.crawl("dandm")
    process.start()


if __name__ == "__main__":
    main()
