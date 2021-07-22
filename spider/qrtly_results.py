from selenium import webdriver
from spider.gathering_data import get_data
from proxy.proxy import ProxyManager
from self_apps.connect_db import cursor_stock
from spider.filters_cvm import filters_cvm
from datetime import datetime as dt


#TODO: Fazer validação de resultado, verificar se todos resultados e valores estão presentes.


def scrap_qrtly_results():
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

    # Load DB:
    raw_db = cursor_stock().find({})
    data_db = [x for x in raw_db]

    # Parse DATA DB x CVM and CREATE LIST:
    url_list = []
    for _data in data_db:
        key_cvm = _data.get('key_cvm')
        nome_pregao = _data.get('nome_pregao')

        print(f'\nSTOCK: {nome_pregao} | Key: {key_cvm}')

        # Start Browser
        driver.get(urlcvm + key_cvm)

        # Load CVM filters
        filters_cvm(driver)

        # Get DATA from CVM:
        result_table = driver.find_elements_by_xpath('//table[contains(@id, "grdDocumentos")]//tbody/tr')

        """ Parse CVM and check if Result EXIST """
        for _row in result_table:
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
                            if _data.get('results') is None:
                                check_result_cvm = False
                            else:
                                check_result_cvm = False if _data.get('results').get(cvm_year).get(
                                    'dfp') is None else True

                        # Check ITR
                        else:
                            if _data.get('results') is None:
                                check_result_cvm = False
                            else:
                                try:
                                    check_result_cvm = False if _data.get('results').get(cvm_year).get('itr').get(
                                        formated_qrtly) is None else True
                                except:
                                    check_result_cvm = False

                        if check_result_cvm:
                            print(f'\tCVM RESULT = {cvm_date} exist!')

                        else:
                            print(f'\tCVM RESULT = {cvm_date} not exist!')
                            url_list.append(cvm_url)

        """ Parse URLS and CREATE DATA if not EXIST """
        for _url_list in url_list:
            # Create Har file (Proxy)
            client.new_har(_url_list)

            # Get DRE = 4
            dre_result = get_data(driver, _url_list, client, 'dre')

            # Get BPA = 2
            bpa_result = get_data(driver, _url_list, client, 'bpa')

            # Get BPP
            bpp_result = get_data(driver, _url_list, client, 'bpp')

            # Get CFA
            cfa_result = get_data(driver, _url_list, client, 'cfa')

            # Build Dict
            result_date = dre_result.get('result_date')
            result_month = dt.strptime(result_date, '%d/%m/%Y').month
            result_year = dt.strptime(result_date, '%d/%m/%Y').year

            result_dict = {
                'dre': dre_result,
                'bpa': bpa_result,
                'bpp': bpp_result,
                'cfa': cfa_result,
            }

            # Save DB
            get_month = dt.strptime(result_date, '%d/%m/%Y').month
            get_year = dt.strptime(result_date, '%d/%m/%Y').year
            formated_qrtly = f'{get_month}/{get_year}'

            # Save DFP
            if result_month == 12:
                cursor_stock().update_one(
                    {'key_cvm': key_cvm},
                    {'$set': {f'results.{result_year}.dfp': result_dict}},
                    upsert=True
                )

            # Save ITR
            else:
                cursor_stock().update_one(
                    {'key_cvm': key_cvm},
                    {'$set': {f'results.{result_year}.itr.{formated_qrtly}': result_dict}},
                    upsert=True
                )

            # output
            print(f'\tRESULT SAVED {formated_qrtly}')

            # # TODO: Break on first RESULT!
            # break

        """ If ITR have Results BUILD the 4th """
        schema = {
            'dre': {},
            'bpa': {},
            'bpp': {},
            'cfa': {},
        }

        check_stock = True if _data.get('results') is not None else False
        if check_stock:
            for _year in _data.get('results'):
                check_itr = False
                check_dfp = False

                for _typ in _data.get('results').get(_year):
                    amount_qrtly = 0
                    if _typ == 'itr':
                        # Validation
                        for _date in _data.get('results').get(_year).get(_typ):
                            amount_qrtly += 1

                        check_itr = True if amount_qrtly == 3 else False

                    check_dfp = True if _data.get('results').get(_year).get('dfp') is not None else False

                # BUILD 4th result
                if check_itr and check_dfp:

                    # Parse ITR
                    for _typ in _data.get('results').get(_year):

                        # SUM ALL QRTLY RESULTS:
                        if _typ == 'itr':
                            for _date in _data.get('results').get(_year).get(_typ):
                                for _dr in _data.get('results').get(_year).get(_typ).get(_date):
                                    for _key, _val in _data.get('results').get(_year).get(_typ).get(_date).get(
                                            _dr).items():

                                        # Filter & Format DATA:
                                        if _key not in 'result_date' and _key not in 'result_sended' and _key not in 'result_url' and _key not in 'descricao' and _key not in 'on':
                                            if _val == '':
                                                _val = 0

                                            # SUM ALL ITR:
                                            if schema.get(_dr).get(_key) is None:
                                                schema[_dr][_key] = _val

                                            else:
                                                schema[_dr][_key] += _val

                    # Parse DFP
                    for _typ in _data.get('results').get(_year):
                        # SUB DFP - ALL ITR:
                        if _typ == 'dfp':
                            for _dr in _data.get('results').get(_year).get(_typ):
                                for _key, _val in _data.get('results').get(_year).get(_typ).get(_dr).items():

                                    if _key == 'result_sended':
                                        result_sended = _val

                                    # Filter & Format DATA:
                                    if _key not in 'result_date' and _key not in 'result_sended' and _key not in 'result_url' and _key not in 'descricao' and _key not in 'on':
                                        if _val == '':
                                            _val = 0
                                        # else:
                                        #     _val = int(str(_val).replace('.', ''))

                                        # SUB DFP - ITR:
                                        if schema.get(_dr).get(_key) is None:
                                            schema[_dr][_key] = _val
                                        else:
                                            schema_val = schema.get(_dr).get(_key)
                                            schema[_dr][_key] = _val - schema_val

                    # Save @ DB:
                    schema['dre']['result_date'] = f'31/12/{_year}'
                    schema['dre']['result_sended'] = result_sended

                    cursor_stock().update_one(
                        {'key_cvm': key_cvm},
                        {'$set': {f'results.{_year}.itr.12/{_year}': schema}}
                    )

                    print(f'\tBuilded New 4th result to {_year}')

        #
        # """ Build Formated Results """
        # fomated_result()



        # # TODO: Break on first STOCK!
        # break

    driver.close()
    server.stop()
