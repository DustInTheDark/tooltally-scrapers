BOT_NAME = "tooltally"

SPIDER_MODULES = ["tooltally.spiders"]
NEWSPIDER_MODULE = "tooltally.spiders"

ROBOTSTXT_OBEY = False
CONCURRENT_REQUESTS_PER_DOMAIN = 1
DOWNLOAD_DELAY = 1

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/114.0.0.0 Safari/537.36"
)

ITEM_PIPELINES = {
    "tooltally.pipelines.DatabasePipeline": 300,
}
