# import yfinance as yf
#
# stock = yf.Ticker('AERI3.SA')
#
# stock = stock.info
#
# print(stock)


from selenium import webdriver
from selenium.webdriver.chrome.options import Options

options = Options()
options.add_argument('--headless')
options.add_argument('--disable-gpu')

driver = webdriver.Chrome('./chromedriver', options=options)
driver.get('http://www.google.com')
