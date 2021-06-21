from db_process.connect_db import cursor
from datetime import datetime as dt
import json


def fomated_result(key_cvm):
    # Load DB:
    data_db = cursor().find({'key_cvm': key_cvm})

    """ Unpack & build DATA """
    for stock in data_db:

        # Check if Formated Result exist:
        for _year in stock.get('results'):
            for _typ in stock.get('results').get(_year):

                # Validação / Create ITR:
                if _typ == 'itr':
                    for _date in stock.get('results').get(_year).get(_typ):
                        check_itr_res = False
                        for _dr in stock.get('results').get(_year).get(_typ).get(_date):

                            # Resultado
                            if _dr == 'res':
                                check_itr_res = True
                                break

                        # CREATE RES
                        if not check_itr_res:
                            result_dict = {
                                'result_date': None,
                                'pat_liq': None,
                                'receita_liq': None,
                                'ebit': None,
                                'depreciacao_amortizao': None,
                                'res_fin': None,
                                'impostos': None,
                                'lucro_liq': None,
                                'caixa': None,
                                'dividabruta': None,
                            }

                            print(f'CHECK RES(ITR) = {_date}')

                            """ Build Result Dict ITR """

                            # DRE
                            for _key, _val in stock.get('results').get(_year).get(_typ).get(_date).get('dre').items():

                                # Result Date:
                                if 'result_date' == _key:
                                    result_dict['result_date'] = _val

                                # Receita Líquida:
                                if 'receitadevenda' in _key:
                                    result_dict['receita_liq'] = _val

                                # Ebit:
                                if 'resultadoantesdoresultadofinanceiroedostributos' == _key:
                                    result_dict['ebit'] = _val

                                # Resultado Financeiro:
                                if 'resultadofinanceiro' in _key:
                                    result_dict['res_fin'] = _val

                                # Impostos:
                                if 'imposto' in _key:
                                    result_dict['impostos'] = _val

                                # Lucro Líq.:
                                if 'lucroprejuizoconsolidadodoperiodo' in _key:
                                    result_dict['lucro_liq'] = _val

                            # BPP
                            emprestimos_liq = 0
                            emprestimos_brt = 0
                            for _key, _val in stock.get('results').get(_year).get(_typ).get(_date).get('bpp').items():

                                # Patrimonio Liq:
                                if 'patrimonioliquido' in _key:
                                    result_dict['pat_liq'] = _val

                                # Divida Bruta:
                                if 'emprestimosefinanciamentosliq' in _key:
                                    emprestimos_liq = _val

                                if 'emprestimosefinanciamentosbrt' in _key:
                                    emprestimos_brt = _val

                            result_dict['dividabruta'] = emprestimos_liq + emprestimos_brt

                            # CFA
                            for _key, _val in stock.get('results').get(_year).get(_typ).get(_date).get('cfa').items():

                                # Patrimonio Liq:
                                if 'saldofinal' in _key:
                                    result_dict['caixa'] = _val

                            # Depreciação & Amortização:

                            get_month = dt.strptime(_date, '%m/%Y').month
                            get_year = dt.strptime(_date, '%m/%Y').year

                            if get_month == 12 or get_month == 3:
                                # Get PRESENT
                                for _key, _val in stock.get('results').get(_year).get(_typ).get(_date).get(
                                        'cfa').items():

                                    # Depreciação & Amortização:
                                    if 'depreciacao' in _key:
                                        result_dict['depreciacao_amortizao'] = -_val

                            else:
                                # Get LAST
                                get_last = f'{get_month - 3}/{get_year}'
                                last_depre = None
                                for _key, _val in stock.get('results').get(_year).get(_typ).get(get_last).get(
                                        'cfa').items():

                                    if 'depreciacao' in _key:
                                        last_depre = _val

                                # Get Actual
                                actual_depre = None
                                for _key, _val in stock.get('results').get(_year).get(_typ).get(_date).get(
                                        'cfa').items():

                                    if 'depreciacao' in _key:
                                        actual_depre = _val

                                result_dict['depreciacao_amortizao'] = last_depre - actual_depre

                            # Validate DATA
                            check_result_dict = False
                            for _check in result_dict.values():
                                check_result_dict = False if _check == '' else True

                            # Save @ DB:
                            if check_result_dict:
                                # print(json.dumps(result_dict, indent=4))
                                cursor().update_one(
                                    {'key_cvm': key_cvm},
                                    {'$set': {f'results.{_year}.itr.{_date}.res': result_dict}}
                                )

                            print(f'CREATED RES(ITR) = {_date}')

                # Validação / Create DFP:
                elif _typ == 'dfp':
                    check_dfp_res = False
                    for _dr in stock.get('results').get(_year).get(_typ):

                        # Resultado
                        if _dr == 'res':
                            check_dfp_res = True
                            break

                    # CREATE RES
                    if not check_dfp_res:
                        result_dict = {
                            'result_date': None,
                            'pat_liq': None,
                            'receita_liq': None,
                            'ebit': None,
                            'depreciacao_amortizao': None,
                            'res_fin': None,
                            'impostos': None,
                            'lucro_liq': None,
                            'caixa': None,
                            'dividabruta': None,
                        }

                        print(f'CHECK RES(DFP) = {_year}')

                        """ Build Result Dict DFP"""
                        # DRE
                        for _key, _val in stock.get('results').get(_year).get(_typ).get('dre').items():

                            # Result Date:
                            if 'result_date' == _key:
                                result_dict['result_date'] = _val

                            # Receita Líquida:
                            if 'receitadevenda' in _key:
                                result_dict['receita_liq'] = _val

                            # Ebit:
                            if 'resultadoantesdoresultadofinanceiroedostributos' == _key:
                                result_dict['ebit'] = _val

                            # Resultado Financeiro:
                            if 'resultadofinanceiro' in _key:
                                result_dict['res_fin'] = _val

                            # Impostos:
                            if 'imposto' in _key:
                                result_dict['impostos'] = _val

                            # Lucro Líq.:
                            if 'lucroprejuizoconsolidadodoperiodo' in _key:
                                result_dict['lucro_liq'] = _val

                        # BPP
                        emprestimos_liq = 0
                        emprestimos_brt = 0
                        for _key, _val in stock.get('results').get(_year).get(_typ).get('bpp').items():

                            # Patrimonio Liq:
                            if 'patrimonioliquido' in _key:
                                result_dict['pat_liq'] = _val

                            # Divida Bruta:
                            if 'emprestimosefinanciamentosliq' in _key:
                                emprestimos_liq = _val

                            if 'emprestimosefinanciamentosbrt' in _key:
                                emprestimos_brt = _val

                        result_dict['dividabruta'] = emprestimos_liq + emprestimos_brt

                        # CFA
                        for _key, _val in stock.get('results').get(_year).get(_typ).get('cfa').items():

                            # Depreciação & Amortização:
                            if 'depreciaca' in _key:
                                result_dict['depreciacao_amortizao'] = -_val

                            # Caixa:
                            if 'saldofinaldecaixaeequivalentes' == _key:
                                result_dict['caixa'] = _val

                        check_result_dict = False
                        for _check in result_dict.values():
                            check_result_dict = False if _check == '' else True

                        # Save @ DB:
                        if check_result_dict:
                            # print(json.dumps(result_dict, indent=4))
                            cursor().update_one(
                                {'key_cvm': key_cvm},
                                {'$set': {f'results.{_year}.dfp.res': result_dict}}
                            )

                        print(f'CREATED RES(DFP) = {_year}')
