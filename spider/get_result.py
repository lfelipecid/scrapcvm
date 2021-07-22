import json
import time

from selenium import webdriver
from spider.gathering_data import get_data
from proxy.proxy import ProxyManager
from self_apps.connect_db import cursor_stock
from spider.filters_cvm import filters_cvm

"""  FIX DEF incase missing result """


def get_result(key_cvm, error):
    # Import and Format ERROR DATA
    _year = error.get('year')
    _typ = error.get('typ').upper()

    _date = error.get('date')
    if _date is not None:
        _date = _date.split('/')[0]
        if len(_date) == 1:
            _date = '0' + _date

    _dr = error.get('dr')

    # Base URL
    urlcvm = 'https://www.rad.cvm.gov.br/ENET/frmConsultaExternaCVM.aspx?codigoCVM='
    cvmbasic = 'https://www.rad.cvm.gov.br/ENET/'

    """ Load Proxy and WebDriver """

    # Load PROXY
    proxy = ProxyManager()
    server = proxy.start_server()
    client = proxy.start_client()

    # Load Web-Driver
    options = webdriver.ChromeOptions()
    options.add_argument(f'--proxy-server={client.proxy}')
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    driver = webdriver.Chrome('./chromedriver', options=options)

    """ Start Scrap """

    driver.get(urlcvm + key_cvm)  # Start Driver
    filters_cvm(driver)  # Load Filters
    result_table = driver.find_elements_by_xpath('//table[contains(@id, "grdDocumentos")]//tbody/tr')  # Load Filters

    # Get URL @ CVM
    result_url = None
    for _row in result_table:
        if 'Ativo' in _row.text:
            result_typ = str(_row.text).split(' -')[0].split(' ')[-1]
            result_date = str(_row.text).split('- - ')[1].split(' ')[0]
            result_year = result_date[-4:]
            result_month = result_date.split('/')[1]

            if _typ == 'DFP':
                if result_year == _year and result_typ == _typ:
                    result_url = cvmbasic + str(
                        _row.find_element_by_id('VisualizarDocumento').get_attribute('onclick')).replace(
                        "OpenPopUpVer('", "").replace("')", "")

            else:
                if result_year == _year and result_typ == _typ and result_month == _date:
                    result_url = cvmbasic + str(
                        _row.find_element_by_id('VisualizarDocumento').get_attribute('onclick')).replace(
                        "OpenPopUpVer('", "").replace("')", "")

    client.new_har(result_url)  # Parse URL
    missing_result = get_data(driver, result_url, client, _dr)  # GATHERING DATA

    """ Save @ DB """
    fomated_result = f'{_date}/{_year}'[1:]

    if _typ == 'DFP':
        cursor_stock().update_one(
            {'key_cvm': key_cvm},
            {'$set': {f'results.{_year}.dfp.{_dr}': missing_result}}
        )

        raw_data = cursor_stock().find_one({'key_cvm': key_cvm})
        time.sleep(1)
        validate_data = True if raw_data.get('results').get(_year).get('dfp').get(_dr) is not None else False

        # Close
        server.stop()
        driver.close()
        return validate_data


    else:
        print(fomated_result)
        cursor_stock().update_one(
            {'key_cvm': key_cvm},
            {'$set': {f'results.{_year}.itr.{fomated_result}.{_dr}': missing_result}}
        )

        raw_data = cursor_stock().find_one({'key_cvm': key_cvm})
        time.sleep(1)
        validate_data = True if raw_data.get('results').get(_year).get('itr').get(fomated_result).get(
            _dr) is not None else False

        # Close
        server.stop()
        driver.close()

        return validate_data
