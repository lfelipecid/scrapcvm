from selenium import webdriver
from spider.gathering_data import get_data
from proxy.proxy import ProxyManager
from db_process.connect_db import cursor
from spider.filters_cvm import annual_filters
from datetime import datetime as dt


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
    driver = webdriver.Chrome(options=options)

    # Load DB:
    raw_db = cursor().find({})
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
        annual_filters(driver)

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
                                check_result_cvm = False if _data.get('results').get(cvm_year).get('itr').get(
                                    cvm_date) is None else True

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
            if result_month == 12:
                cursor().update_one(
                    {'key_cvm': key_cvm},
                    {'$set': {f'results.{result_year}.dfp': result_dict}},
                    upsert=True
                )

            else:
                cursor().update_one(
                    {'key_cvm': key_cvm},
                    {'$set': {f'results.{result_year}.itr.{result_date}': result_dict}},
                    upsert=True
                )

            # output
            print(f'\tRESULT SAVED {result_date}')

            # # TODO: Break on first RESULT!
            # break

        """ If ITR have Results BUILD the 4th """

        # Build SCHEMA
        schema = {
            'dre': {},
            'bpa': {},
            'bpp': {},
            'cfa': {},
        }

        result_sended = ''
        for _year in _data.get('results'):

            # Validate if ITR exist:
            full_check = False
            check_itr = False if _data.get('results').get(_year).get('itr') is None else True
            if check_itr:
                for _ck_dt in _data.get('results').get(_year).get('itr'):
                    full_check = False if '31/12' in _ck_dt else True

            if full_check:

                # Parse and SUM ITR:
                for _dt in _data.get('results').get(_year).get('itr'):
                    # SUM ALL ITR:
                    for _typ in _data.get('results').get(_year).get('itr').get(_dt):
                            for _key, _val in _data.get('results').get(_year).get('itr').get(_dt).get(_typ).items():

                                # Filter DATA
                                key = str(_key)
                                if '1' in key or '2' in key or '3' in key or '6' in key:

                                    # Format
                                    if _val == '':
                                        _val = 0
                                    else:
                                        _val = int(_val.replace('.', ''))

                                    # SUM ALL ITR:
                                    if schema[_typ].get(_key) is None:
                                        schema[_typ][_key] = _val
                                    else:
                                        schema_val = schema.get(_typ).get(_key)
                                        schema[_typ][_key] = int(_val) + int(schema_val)

                # Parse SUB DFP:
                for _typ in _data.get('results').get(_year).get('dfp'):
                    for _key, _val in _data.get('results').get(_year).get('dfp').get(_typ).items():

                        # Filter DATA
                        if _key == 'result_sended':
                            result_sended = _val
                        key = str(_key)
                        if '1' in key or '2' in key or '3' in key or '6' in key:

                            # Format
                            if _val == '':
                                _val = 0
                            else:
                                _val = int(_val.replace('.', ''))

                            # SUM ALL
                            if schema.get(_typ).get(_key) is None:
                                schema[_typ][_key] = str(_val)
                            else:
                                schema_val = schema.get(_typ).get(_key)
                                schema[_typ][_key] = str(int(_val) - int(schema_val))

                # Save @ DB":
                schema['dre']['result_date'] = f'31/12/{_year}'
                schema['dre']['result_sended'] = result_sended

                cursor().update_one(
                    {'key_cvm': key_cvm},
                    {'$set': {f'results.{_year}.itr.31/12/{_year}': schema}}
                )
                print(f'\tYear {_year}: New last ITR has creadted.')
            else:
                print(f'\tYear {_year}: Data already exist or do not exist ITR.')

        # TODO: Break on first STOCK!
        break

    driver.close()
    server.stop()
