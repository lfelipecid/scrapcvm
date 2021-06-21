from time import sleep
from slugify import slugify
from selenium.webdriver.support.ui import Select


def get_data(driver, url, client, *args):
    # Variable
    result_dict = {}
    doc = ''
    sel = False
    ind = None

    # Validation
    if args[0] == 'dre':
        doc = '4'

    elif args[0] == 'bpa':
        doc = '2'
        ind = 0
        sel = True

    elif args[0] == 'bpp':
        doc = '3'
        ind = 1
        sel = True

    elif args[0] == 'cfa':
        doc = '99'
        ind = 4
        sel = True

    try:
        # Start Broweser
        driver.get(url)
        sleep(1)
        driver.implicitly_wait(10)

        result_date = driver.find_element_by_id('lblDataDocumento').text
        result_sended = driver.find_element_by_id('lblDataEnvio').text
        result_url = driver.current_url
        result_dict['result_date'] = result_date
        result_dict['result_sended'] = result_sended
        result_dict['result_url'] = result_url

        if sel:
            select = Select(driver.find_element_by_id('cmbQuadro'))
            select.select_by_index(ind)
            sleep(1)
            driver.implicitly_wait(10)

        # Load HTTP Trafic
        httptrafic = client.har
        httptrafic = httptrafic['log']['entries']

        # Get URL @ PROXY:
        last_url = ''
        for trafic in httptrafic:
            proxy_url = trafic['request']['url']
            if 'Hash=' and f'Demonstracao={doc}' in proxy_url:
                last_url = proxy_url

        # URL from PROXY:
        driver.get(last_url)
        sleep(1)

        tabelecvm = driver.find_elements_by_xpath('//table[contains(@id, "ctl00_cphPopUp_tbDados")]/tbody/tr')

        # Gathering DATA:
        for item in tabelecvm:
            _row = str(item.text)
            _header = _row.split('  ')[0].replace(' ', '').replace('.', '')
            _key = _row.split('  ')[1]
            _val = _row.split('  ')[2].replace(' ', '')

            check_none = True if _val == '' else False
            if check_none:
                _val = 0

            if args[0] == 'bpp':

                # Filter
                if 'Empréstimos' in _key:

                    # Liquido
                    if '20104' == _header:
                        if check_none is False:
                            _key = str(slugify(_key, separator='')) + 'liq'
                            _val = int(str(_val).replace('.', ''))
                            result_dict[_key] = _val

                    # Bruto
                    elif '20201' == _header:
                        if check_none is False:
                            _key = str(slugify(_key, separator='')) + 'brt'
                            _val = int(str(_val).replace('.', ''))
                            result_dict[_key] = _val

                else:
                    if 'Conta' not in _header:
                        _key = slugify(_key, separator='')
                        _val = int(str(_val).replace('.', ''))
                        result_dict[_key] = _val

            else:
                if '399' not in _header and 'Conta' not in _header:
                    check_none = True if _val == '' else False
                    if check_none:
                        _val = 0

                    _key = slugify(_key, separator='')
                    _val = int(str(_val).replace('.', ''))
                    result_dict[_key] = _val

        return result_dict

    except Exception as e:
        print(e)

#
# _row = str(item.text)
# header = _row.split('  ')[0].replace(' ', '').replace('.', '')
# key = slugify(_row.split('  ')[1], separator='')
# _row = _row.split('  ')[2].replace(' ', '')
#
# # Filter DATA:
# number_key = 0
# if '399' not in header and 'Conta' not in header:
#
#     if _row == '':
#         _row = 0
#     elif 'emprestimosefinanciamentos' in key:
#         if '2.01' in header:
#             key = 'emprestimosefinanciamentosliq'
#         elif '2.02' in header:
#             key = 'emprestimosefinanciamentosbrt'
#
#     else:
#         _row = int(str(_row.replace('.', '')))
#
#     result_dict[key] = _row
#     # print(f'K: {key}: {_row} | T: {type(_row)}')
