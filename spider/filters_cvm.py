from datetime import datetime as dt
from selenium.webdriver.common.keys import Keys
from time import sleep


def filters_cvm(driver):
    sleep(1)
    driver.implicitly_wait(10)

    # Btn Period
    btn_periodo = driver.find_element_by_id('rdPeriodo')
    btn_periodo.click()
    driver.implicitly_wait(1)

    # Data
    dt_hoje = dt.now().strftime('%d/%m/%Y')
    btn_datain = driver.find_element_by_id('txtDataIni')
    btn_datain.send_keys('01/01/2011')
    driver.implicitly_wait(1)
    btn_dataout = driver.find_element_by_id('txtDataFim')
    btn_dataout.send_keys(dt_hoje)
    driver.implicitly_wait(1)

    # Categoria
    btn_categoria = driver.find_element_by_id('cboCategorias_chosen_input')
    btn_categoria.send_keys('ITR - Informações Trimestrais')
    driver.implicitly_wait(1)
    btn_categoria.send_keys(Keys.ENTER)
    driver.implicitly_wait(1)
    btn_categoria.send_keys('DFP - Demonstrações Financeiras Padronizadas')
    driver.implicitly_wait(1)
    btn_categoria.send_keys(Keys.ENTER)
    driver.implicitly_wait(1)

    driver.find_element_by_id('btnConsulta').click()
    driver.implicitly_wait(10)
    sleep(2)
