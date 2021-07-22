import scrapy
from datetime import datetime as dt
from self_apps.connect_db import cursor_stock
from slugify import slugify
import json
import yfinance as yf
from pathlib import Path
from urllib.request import urlopen, Request


class HeaderStock(scrapy.Spider):
    name = 'headers'

    # Parse Links
    basic_url = 'http://bvmf.bmfbovespa.com.br/'
    start_url = 'http://bvmf.bmfbovespa.com.br/cias-listadas/empresas-listadas/BuscaEmpresaListada.aspx?Letra='
    principal_url = 'http://bvmf.bmfbovespa.com.br/cias-listadas/empresas-listadas/ResumoEmpresaPrincipal.aspx?codigoCvm='
    eventos_url = 'http://bvmf.bmfbovespa.com.br/cias-listadas/empresas-listadas/ResumoEventosCorporativos.aspx?codigoCvm='

    # Browser Headers
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36'
    }

    # TIME
    dt_now = dt.now().strftime('%d/%m/%Y %H:%M')
    ts_now = dt.timestamp(dt.strptime(dt.now().strftime('%d/%m/%Y %H:%M'), '%d/%m/%Y %H:%M'))

    # ALPHA
    alphabet = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U',
                'V', 'W', 'X', 'Y', 'Z']

    def start_requests(self):

        # Parse the start URL
        for letter in self.alphabet:
            yield scrapy.Request(
                url=self.start_url + letter + '&idioma=pt-br',
                headers=self.headers,
                callback=self.parse_stocks,
                meta={
                    'download_timeout': 2,
                    'max_retry_times': 10,
                }
            )

            # TODO: Stop frist letter:
            break

    def parse_stocks(self, res):
        # Geting links
        stocks_url = res.xpath(
            '//tr[contains(@class, "SiteBmfBovespa")]//a/@href').getall()

        # Get KEYS_CVM @ CVM:
        clean_key = ''
        key_cvm_list = []
        for item in stocks_url:
            key_cvm = str(item).split('=')[1].split('&')[0]
            if clean_key != key_cvm:
                key_cvm_list.append(key_cvm)
            clean_key = key_cvm

        # Check if exist
        for _key_cvm in key_cvm_list:

            key_check = True if cursor_stock().find_one({'key_cvm': _key_cvm}) else False
            key_check = False
            # IF FALSE CREATE DATA!
            if not key_check:
                print(f'\n\nCREATE: KEY CVM | {_key_cvm}\n\n ')

                yield res.follow(
                    url=self.principal_url + _key_cvm + '&idioma=pt-br',
                    callback=self.parse_iframe,
                    meta={'download_timeout': 2, 'max_retry_times': 10}
                )
            else:
                print(f'\n\nKEY CVM = {_key_cvm} : Arealdy Exit \n\n ')

            # # TODO Break on First Stock
            # break

    def parse_iframe(self, res):
        # Gathering data
        razao_social = res.xpath(
            '//span[contains(@id, "ctl00_contentPlaceHolderConteudo_MenuEmpresasListadas1_lblNomeEmpresa")]/text()'
        ).get()
        iframe = str(res.xpath(
            '//iframe[contains(@id, "ctl00_contentPlaceHolderConteudo_iframeCarregadorPaginaExterna")]/@src'
        ).get().replace('../../', ''))

        yield res.follow(
            url=self.basic_url + iframe,
            callback=self.parse_principal,
            meta={
                'razao_social': razao_social,
                'download_timeout': 2,
                'max_retry_times': 10,
            }
        )

    def parse_principal(self, res):
        # Get data from above
        razao_social = res.meta.get('razao_social')

        # Gathering data
        key_cvm = str(res.url).split('=')[1].split('&')[0]
        nome_pregao = res.xpath(
            '//td[contains(text(), "Nome de Pregão:")]/following-sibling::td/text()').get()
        code_stock = res.xpath(
            '//a[contains(@class, "LinkCodNeg")]/text()').get()
        cnpj_stock = res.xpath(
            '//td[contains(text(), "CNPJ:")]/following-sibling::td/text()').get()
        atvd_stock = res.xpath(
            '//td[contains(text(), "Atividade Principal:")]/following-sibling::td/text()').get()
        class_stock = res.xpath(
            '//td[contains(text(), "Classificação Setorial:")]/following-sibling::td/text()').get()

        site_ri = res.xpath('//a[contains(@target, "_blank")]/@href').get()
        site_b3 = self.principal_url + key_cvm + '&idioma=pt-br'

        if site_ri == None:
            site_ri = 'N/A'

        header_data = {
            'slug': slugify(code_stock),
            'key_cvm': key_cvm,
            'razao_social': razao_social,
            'nome_pregao': nome_pregao,
            'code_stock': code_stock,
            'cnpj_stock': cnpj_stock,
            'atvd_stock': atvd_stock,
            'class_stock': class_stock,
            'site_ri': site_ri,
            'site_b3': site_b3,
        }

        yield res.follow(
            url=self.eventos_url + key_cvm,
            callback=self.parse_eventos,
            meta={
                'header_data': header_data,
                'download_timeout': 2,
                'max_retry_times': 10,
            }
        )

    def parse_eventos(self, res):
        # Get data from above
        header_data = res.meta.get('header_data')

        # Gathering new data
        segmento = res.xpath(
            '//span[contains(@id, "ctl00_contentPlaceHolderConteudo_lblSegmentoValor")]/text()').get()
        total_acoes = res.xpath(
            '//span[contains(@id, "ctl00_contentPlaceHolderConteudo_lblTotalAcoesValor")]/text()').get()

        # Load DATA from YFINANCE
        ticker = header_data.get('code_stock')
        stock = yf.Ticker(f'{ticker}.SA')
        stock = stock.info

        site_stock = stock.get('website')
        logo_url = stock.get('logo_url')

        # Create PATH for LOGO
        path = Path(f'/home/felipecid/documents/inside_market/django_im/media/stocks/{ticker}.png')
        url_path = f'stocks/{ticker}.png'
        path.parent.mkdir(parents=True, exist_ok=True)

        # Upate dict
        header_data.update({
            'site_stock': site_stock,
            'segmento': segmento,
            'total_acoes': total_acoes,
            'logo_url': url_path,
            'last_update': self.dt_now,
            'views': 0,
        })

        # Validate data
        check_data = True
        for check in header_data.values():
            if None == check:
                check_data = False

        # Save DATA @ DB:
        if check_data:
            # CREATE DATA

            print(f'\n\nNEW DATA = {json.dumps(header_data, indent=5)}\n\n')
            cursor_stock().insert_one(header_data)

            # Save LOGO
            with open(path, 'wb') as local:
                req = Request(url=logo_url, headers=self.headers)
                with urlopen(req) as response:
                    local.write(response.read())
        else:
            print(f'ERROR on Gathering DATA')
