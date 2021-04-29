from spider.stocks_headers import HeaderStock
from scrapy.crawler import CrawlerProcess
from spider.qrtly_results import scrap_qrtly_results


# # Load SPIDERS.
# process = CrawlerProcess()
#
# # Run Headers
# process.crawl(HeaderStock)
# process.start()

# Run Spider over Qrtly Results:
scrap_qrtly_results()


