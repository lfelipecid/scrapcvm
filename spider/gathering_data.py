from time import sleep
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
            header = _row.split('  ')[0].replace(' ', '').replace('.', '')
            # description = _row.split('  ')[1].replace(' ', '')
            result = _row.split('  ')[2].replace(' ', '')

            # TODO: Removed Description check who will be
            if not '399' in header:
                if not 'Conta' in header:
                    result_dict[header] = result

        return result_dict

    except Exception as e:
        print(e)
