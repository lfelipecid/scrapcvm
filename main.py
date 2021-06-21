import logging

from spider.stocks_headers import HeaderStock
from scrapy.crawler import CrawlerProcess
from spider.qrtly_results import scrap_qrtly_results
from db_process.connect_db import cursor
from spider.scrap_results_OLD_ import scrap_results
from spider.build_4th import build_4th
from spider.formated_result import fomated_result
from spider.build_ttm import build_ttm
from spider.build_ind import build_indicadres

logging.getLogger('pymongo').propagate = False
logging.getLogger('urllib3').propagate = False
logging.getLogger('Scrapy').propagate = False

# # Load SPIDERS.
# process = CrawlerProcess()
#
# # Run Headers
# process.crawl(HeaderStock)
# process.start()
#
# # Run Spider over Qrtly Results:
# scrap_qrtly_results()

# Run format result


# Chain Process
stocks_list = cursor().distinct('key_cvm')

for key_cvm in stocks_list:
    # scrap results @ CVM:
    # scrap_results(key_cvm)

    # Build 4th result if need it:
    build_4th(key_cvm)

    # Build Formated Result:
    fomated_result(key_cvm)

    # Build TTM
    build_ttm(key_cvm)

    # Build Indicadores:
    build_indicadres(key_cvm)
