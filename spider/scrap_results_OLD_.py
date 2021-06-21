from selenium import webdriver
from spider.gathering_data import get_data
from proxy.proxy import ProxyManager
from db_process.connect_db import cursor
from spider.filters_cvm import filters_cvm
from datetime import datetime as dt

def scrap_results(key_cvm):

    # URLS CVM:
    urlcvm = 'https://www.rad.cvm.gov.br/ENET/frmConsultaExternaCVM.aspx?codigoCVM='
    cvmbasic = 'https://www.rad.cvm.gov.br/ENET/'

    # Load PROXY
    proxy = ProxyManager()
    server = proxy.start_server()
    client = proxy.start_client()

    # Load Web-driver
    options = webdriver.ChromeOptions()
    options.add_argument(f'--proxy-server={client.proxy}')
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    driver = webdriver.Chrome('./chromedriver', options=options)

    # Load OUT DATA:
    stock_data = cursor().find_one({'key_cvm': key_cvm})
    nome_pregao = stock_data.get('nome_pregao')

    # Acess CVM & Filter:
    driver.get(urlcvm + key_cvm)
    filters_cvm(driver)

    result_cvm = driver.find_elements_by_xpath('//table[contains(@id, "grdDocumentos")]//tbody/tr')

    """ Parse CVM and Check if result EXIST """
    url_list = []
    print(f'STOCK = {nome_pregao} | K: {key_cvm}')
    for _row in result_cvm:
        # Parse only ACTIVE Results:
        if 'Ativo' in _row.text:
            # DATETIME
            cvm_date = str(_row.text).split(' - - ')[1].split(' ')[0]
            cvm_month = str(dt.strptime(cvm_date, '%d/%m/%Y').month)
            cvm_year = str(dt.strptime(cvm_date, '%d/%m/%Y').year)
            formated_qrtly = f'{cvm_month}/{cvm_year}'

            # URL
            get_url = _row.find_elements_by_id('VisualizarDocumento')

            # Get Results URLS @ CVM:
            for _urls in get_url:
                raw_result = _urls.get_attribute('onclick')
                if 'frmGerenciaPaginaFRE' in raw_result:
                    cvm_url = cvmbasic + str(raw_result).replace("OpenPopUpVer('", "").replace("')", "")

                    """
                    Check if DATA exist @ DB:
                    """

                    # Check DFP
                    if cvm_month == '12':
                        if stock_data.get('results') is None:
                            check_result_cvm = False
                        else:
                            check_result_cvm = False if stock_data.get('results').get(cvm_year).get(
                                'dfp') is None else True

                    # Check ITR
                    else:
                        if stock_data.get('results') is None:
                            check_result_cvm = False
                        else:
                            try:
                                check_result_cvm = False if stock_data.get('results').get(cvm_year).get('itr').get(
                                    formated_qrtly) is None else True
                            except:
                                check_result_cvm = False

                    if check_result_cvm:
                        print(f'\tCVM RESULT = {cvm_date} exist!')

                    else:
                        print(f'\tCVM RESULT = {cvm_date} not exist!')
                        url_list.append(cvm_url)

    """ Parse URLS and CREATE DATA """
    for _url_list in url_list:
        # Create Har file (PROXY)
        client.new_har(_url_list)

        # Get DRE
        dre_result = get_data(driver, _url_list, client, 'dre')

        # Get BPA
        bpa_result = get_data(driver, _url_list, client, 'bpa')

        # Get BPP
        bpp_result = get_data(driver, _url_list, client, 'bpp')

        # Get CFA
        cfa_result = get_data(driver, _url_list, client, 'cfa')

        # Build Dict:
        result_date = dre_result.get('result_date')
        get_month = dt.strptime(result_date, '%d/%m/%Y').month
        get_year = dt.strptime(result_date, '%d/%m/%Y').year

        result_dict = {
            'dre': dre_result,
            'bpa': bpa_result,
            'bpp': bpp_result,
            'cfa': cfa_result,
        }

        """ Save @ DataBase """
        # Save DFP:
        if get_month == 12:
            print(f'\n\tCreated DFP = {get_year}')
            cursor().update_one(
                {'key_cvm': key_cvm},
                {'$set': {f'results.{get_year}.dfp': result_dict}}
            )
        # Save ITR:
        else:
            print(f'\tCreated ITR = {get_month}/{get_year}')
            cursor().update_one(
                {'key_cvm': key_cvm},
                {'$set': {f'results.{get_year}.itr.{get_month}/{get_year}': result_dict}}
            )










