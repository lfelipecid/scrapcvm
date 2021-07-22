from spider.stocks_headers import HeaderStock
from scrapy.crawler import CrawlerProcess

# Load SPIDERS.
process = CrawlerProcess()

# Run Headers
process.crawl(HeaderStock)
process.start()
