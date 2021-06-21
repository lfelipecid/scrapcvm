from db_process.connect_db import cursor
from datetime import datetime as dt
import requests
from lxml import html
import json


def build_indicadres(key_cvm):
    data_db = cursor().find({'key_cvm': key_cvm})

    # Build TTM List
    ttm_list = []
    for stock in data_db:
        for _year in stock.get('results'):
            for _typ in stock.get('results').get(_year):
                if _typ == 'ttm':
                    for _date in stock.get('results').get(_year).get(_typ):
                        ttm_list.append(_date)

        def sort_date(x):
            x = dt.timestamp(dt.strptime(x, '%m/%Y'))
            return x

        ttm_list.sort(key=sort_date, reverse=True)

        # Check if exist
        for _ttm_result in ttm_list:
            _month = str(dt.strptime(_ttm_result, '%m/%Y').month)
            _year = str(dt.strptime(_ttm_result, '%m/%Y').year)

            try:
                check_ind = False if stock.get('results').get(_year).get('ind').get(_ttm_result) is None else True
            except:
                check_ind = False

            # Build IND
            if not check_ind:
                ind_dict = {
                    'amount_stocks': None,
                    'lpa': None,
                    'vpa': None,
                    'margem_ebit': None,
                    'margem_liq': None,
                    'roic': None,
                    'roe': None,
                    'liquidez': None,
                    'divida_patri': None,
                    'ebitda': None,
                }

                """
                Get Items
                """

                # Get Total Ações
                base_url = f'http://bvmf.bmfbovespa.com.br/pt-br/mercados/acoes/empresas/ExecutaAcaoConsultaInfoEmp.asp?CodCVM={key_cvm}'

                def make_reqeust(url):

                    for x in range(0, 5):
                        try:
                            page = requests.get(url, timeout=0.5)
                            if page.status_code == 200:
                                return page

                        except Exception as e:
                            print(e)

                tree = html.fromstring(make_reqeust(base_url).content)
                amount_stocks = tree.xpath('//div[contains(@id, "divComposicaoCapitalSocial")]//td[contains(text(), "Total")]/following-sibling::td/text()')[0]
                amount_stocks = int(str(amount_stocks).replace('.', ''))


                # Get Lucro Liq
                try:
                    lucro_liq = stock.get('results').get(_year).get('ttm').get(_ttm_result).get('lucro_liq') * 1000
                except:
                    lucro_liq = None

                # Get Pat Liq
                try:
                    pat_liq = stock.get('results').get(_year).get('ttm').get(_ttm_result).get('pat_liq') * 1000
                except:
                    pat_liq = None

                # Get Ebit
                try:
                    ebit = stock.get('results').get(_year).get('ttm').get(_ttm_result).get('ebit') * 1000
                except:
                    ebit = None

                # Get Receita Liq.
                try:
                    receita_liq = stock.get('results').get(_year).get('ttm').get(_ttm_result).get('receita_liq') * 1000
                except:
                    receita_liq = None

                # Caixa
                try:
                    caixa = stock.get('results').get(_year).get('ttm').get(_ttm_result).get('caixa') * 1000
                except:
                    caixa = None

                # Ativo Total
                try:
                    ativototal = stock.get('results').get(_year).get('itr').get(_ttm_result).get('bpa').get(
                        'ativototal') * 1000
                except:
                    ativototal = None

                # fornecedores
                try:
                    fornecedores = stock.get('results').get(_year).get('itr').get(_ttm_result).get('bpp').get(
                        'fornecedores') * 1000
                except:
                    fornecedores = None

                # Ativo Circulante
                try:
                    ativocirculante = stock.get('results').get(_year).get('itr').get(_ttm_result).get('bpa').get(
                        'ativocirculante') * 1000
                except:
                    ativocirculante = None

                # Passivo Circulante
                try:
                    passivocirculante = stock.get('results').get(_year).get('itr').get(_ttm_result).get('bpp').get(
                        'passivocirculante') * 1000
                except:
                    passivocirculante = None

                # Divida bruta
                try:
                    dividabruta = stock.get('results').get(_year).get('ttm').get(_ttm_result).get('dividabruta') * 1000
                except:
                    dividabruta = None

                # Depreciação
                try:
                    depreciacao_amortizao = stock.get('results').get(_year).get('ttm').get(_ttm_result).get(
                        'depreciacao_amortizao') * 1000
                except:
                    depreciacao_amortizao = None

                """ 
                Indicators
                """

                # LPA:
                if lucro_liq and amount_stocks is not None:
                    ind_dict['lpa'] = float(f'{lucro_liq / amount_stocks:.2f}')
                else:
                    ind_dict['lpa'] = None

                # VPA:
                if pat_liq and amount_stocks is not None:
                    ind_dict['vpa'] = float(f'{pat_liq / amount_stocks:.2f}')
                else:
                    ind_dict['vpa'] = None

                # Margem EBIT:
                if ebit and receita_liq is not None:
                    ind_dict['margem_ebit'] = float(f'{(ebit / receita_liq) * 100:.2f}')
                else:
                    ind_dict['margem_ebit'] = None

                # Margem Liq:
                if lucro_liq and receita_liq is not None:
                    ind_dict['margem_liq'] = float(f'{(lucro_liq / receita_liq) * 100:.2f}')
                else:
                    ind_dict['margem_liq'] = None

                # ROIC:
                if ebit and ativototal and ativototal and caixa and fornecedores is not None:
                    ind_dict['roic'] = float(f'{(ebit / (ativototal - caixa - fornecedores)) * 100:.2f}')
                else:
                    ind_dict['roic'] = None

                # ROE:
                if lucro_liq and pat_liq is not None:
                    ind_dict['roe'] = float(f'{(lucro_liq / pat_liq) * 100:.2f}')
                else:
                    ind_dict['roe'] = None

                # Liquidez:
                if ativocirculante and passivocirculante is not None:
                    ind_dict['liquidez'] = float(f'{(ativocirculante / passivocirculante):.2f}')
                else:
                    ind_dict['liquidez'] = None

                # Divida / Patri:
                if dividabruta and pat_liq is not None:
                    ind_dict['divida_patri'] = float(f'{(dividabruta / pat_liq):.2f}')
                else:
                    ind_dict['divida_patri'] = None

                # Ebitida:
                if ebit and depreciacao_amortizao is not None:
                    ind_dict['ebitda'] = int(f'{(ebit + abs(depreciacao_amortizao))/1000:.0f}')
                else:
                    ind_dict['ebitda'] = None

                # T Acoes
                if amount_stocks is not None:
                    ind_dict['amount_stocks'] = amount_stocks

                """ Save @ DB """
                print(f'IND has created = {_ttm_result}')
                # print(json.dumps(ind_dict, indent=4))
                cursor().update_one(
                    {'key_cvm': key_cvm},
                    {'$set': {f'results.{_year}.ind.{_ttm_result}': ind_dict}}
                )
