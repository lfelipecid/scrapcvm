import scrapy
from datetime import datetime as dt
from db_process.connect_db import cursor
from slugify import slugify
import json


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

    # Create or Update
    status_key = ''

    alphabet = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U',
                'V', 'W', 'X', 'Y', 'Z']

    def start_requests(self):

        # Load DB:
        raw_db = cursor().find({})
        data_db = []
        for _data in raw_db:
            data_db.append(_data)

        # Parse the start URL
        for letter in self.alphabet:
            yield scrapy.Request(
                url=self.start_url + letter + '&idioma=pt-br',
                headers=self.headers,
                callback=self.parse_stocks,
                meta={
                    'download_timeout': 2,
                    'max_retry_times': 10,
                    'data_db': data_db,
                }
            )

            # TODO: Stop frist letter:
            break

    def parse_stocks(self, res):
        # Get data from above
        data_db = res.meta.get('data_db')

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

        # Parse over KEY_CVM and DB:
        for _key_cvm in key_cvm_list:
            print(f'\n\nKEY CVM = {_key_cvm}')

            # If not exist Table
            if len(data_db) == 0:
                self.status_key = 'create'

            # If Exist:
            for _data_db in data_db:
                _key_bd = _data_db.get('key_cvm')
                _last_update_db = _data_db.get('last_update')

                # TODO: Logic based on TS
                if _key_bd == _key_cvm:
                    self.status_key = 'updated'
                    break
                else:
                    self.status_key = 'create'

            if self.status_key == 'create':
                print(f'\tData dont exist')
                yield res.follow(
                    url=self.principal_url + _key_cvm + '&idioma=pt-br',
                    callback=self.parse_iframe,
                    meta={'download_timeout': 2, 'max_retry_times': 10}
                )

            elif self.status_key == 'updated':
                print(f'\tData arealdy created!')

            print('-=' * 45)
            # TODO Break on First Stock
            break

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
        site_stock = res.xpath('//a[contains(@target, "_blank")]/@href').get()

        if site_stock == None:
            site_stock = 'N/A'

        header_data = {
            'id': int(key_cvm),
            'slug': slugify(code_stock),
            'key_cvm': key_cvm,
            'razao_social': razao_social,
            'nome_pregao': nome_pregao,
            'code_stock': code_stock,
            'cnpj_stock': cnpj_stock,
            'atvd_stock': atvd_stock,
            'class_stock': class_stock,
            'site_stock': site_stock,
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
        lote = res.xpath(
            '//span[contains(@id, "ctl00_contentPlaceHolderConteudo_lblLoteNegociacaoValor")]/text()').get()
        capital_social = res.xpath(
            '//span[contains(@id, "ctl00_contentPlaceHolderConteudo_lblCapitalSocialValor")]/text()').get()
        total_acoes = res.xpath(
            '//span[contains(@id, "ctl00_contentPlaceHolderConteudo_lblTotalAcoesValor")]/text()').get()
        acoes_ordinarias = res.xpath(
            '//span[contains(@id, "ctl00_contentPlaceHolderConteudo_lblQtdeAcoesOrdinariasValor")]/text()').get()
        escriturador = str(res.xpath(
            '//span[contains(@id, "ctl00_contentPlaceHolderConteudo_lblFormaAcaoOrdValor")]/text()').get()).replace(
            'Escritural - ', '')
        acoes_preferencias = res.xpath(
            '//span[contains(@id, "ctl00_contentPlaceHolderConteudo_lblQtdeAcoesPreferenciaisValor")]/text()').get()

        # Upate dict
        header_data.update({
            'segmento': segmento,
            'lote': lote,
            'capital_social': capital_social,
            'total_acoes': total_acoes,
            'acoes_ordi': acoes_ordinarias,
            'acoes_pref': acoes_preferencias,
            'escriturador': escriturador,
            'last_update': self.dt_now,
        })

        # Validate data
        check_data = True
        for check in header_data.values():
            if 'None' == check:
                check_data = False

        # Save DATA @ DB:
        if check_data:
            # Get Key
            key_cvm = header_data.get('key_cvm')
            nome_pregao = header_data.get('nome_pregao')

            # UPDATED DATA
            if self.status_key == 'outdated':
                print(f'\n\nTEM ITEMS PARA UPDATED = {json.dumps(header_data, indent=5)}')

            # CREATE DATA
            elif self.status_key == 'create':
                print(f'\n\nNEW DATA = {json.dumps(header_data, indent=5)}')
                cursor().insert_one(header_data)
