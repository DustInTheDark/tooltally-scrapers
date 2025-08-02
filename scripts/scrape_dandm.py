"""Entrypoint for running the D&M Tools spider."""

from __future__ import annotations

import os

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


def main() -> None:
    """Run the D&M Tools spider using the project's settings."""

    # Bypass any proxy configuration that may be present in the environment.
    # Some execution environments set HTTP(S)_PROXY variables which can cause
    # Scrapy's downloader to route requests through an invalid proxy leading to
    # errors like "Could not open CONNECT tunnel". Clearing these variables
    # ensures the spider connects directly to the target site.
    for var in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"):
        os.environ.pop(var, None)

    process = CrawlerProcess(get_project_settings())
    process.crawl("dandm")
    process.start()


if __name__ == "__main__":
    main()