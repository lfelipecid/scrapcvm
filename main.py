from self_apps.connect_db import cursor_stock
from spider.scrap_results_OLD_ import scrap_results
from spider.build_4th import build_4th
from spider.build_res import build_res
from spider.build_ttm import build_ttm
from spider.build_ind import build_indicadres

# Get Keys!
stocks_list = cursor_stock().distinct('key_cvm')

for key_cvm in stocks_list:

    # # scrap results @ CVM:
    # try:
    #     scrap_results(key_cvm)
    # except Exception as e:
    #     print(f'ERROR = {e}')

    # Build 4th result if need it:
    build_4th(key_cvm)

    # Build Formated Result:
    build_res(key_cvm)

    # Build TTM
    build_ttm(key_cvm)

    # Build Indicadores:
    build_indicadres(key_cvm)
