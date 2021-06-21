import json
from db_process.connect_db import cursor


def build_4th(key_cvm):
    # Load OUT DATA
    stock_data = cursor().find_one({'key_cvm': key_cvm})

    # Build 4th if need it:
    for _year in stock_data.get('results'):

        # Validation Process
        check_itr = False
        check_dfp = False
        for _typ in stock_data.get('results').get(_year):
            amount_qrtly = 0
            if _typ == 'itr':
                for _date in stock_data.get('results').get(_year).get(_typ):
                    amount_qrtly += 1

                check_itr = True if amount_qrtly == 3 else False

            check_dfp = True if stock_data.get('results').get(_year).get('dfp') is not None else False

        # Build 4th
        if check_itr and check_dfp:

            # Schema
            schema = {
                'dre': {},
                'bpa': {},
                'bpp': {},
                'cfa': {},
            }

            """ Parse ITR and SUM ALL """
            for _typ in stock_data.get('results').get(_year):

                # SUM ALL ITRS:
                if _typ == 'itr':
                    for _date in stock_data.get('results').get(_year).get(_typ):
                        for _dr in stock_data.get('results').get(_year).get(_typ).get(_date):
                            if _dr != 'res':
                                for _key, _val in stock_data.get('results').get(_year).get(_typ).get(_date).get(
                                        _dr).items():

                                    # Depreciação, Caixa e Património LIQ.
                                    if 'depreciacao' in _key or 'saldofinal' in _key or 'patrimonioliquido' in _key or 'emprestimosefinanciamentosliq' in _key or 'emprestimosefinanciamentosbrt' in _key:
                                        _val = 0

                                    # Filter & SUM all DATA:
                                    if _key not in 'result_date' and _key not in 'result_sended' and _key not in 'result_url' and _key not in 'descricao' and _key not in 'on':
                                        if _val == '':
                                            _val = 0

                                        # SUM ALL ITR:
                                        if schema.get(_dr).get(_key) is None:
                                            schema[_dr][_key] = _val

                                        else:
                                            schema[_dr][_key] += _val

            """ Parse DFP and SUB """
            for _typ in stock_data.get('results').get(_year):
                # Parse DFP:
                if _typ == 'dfp':
                    for _dr in stock_data.get('results').get(_year).get(_typ):
                        if _dr != 'res':
                            for _key, _val in stock_data.get('results').get(_year).get(_typ).get(_dr).items():

                                # Depreciação, Caixa e Património LIQ.
                                if 'depreciacao' in _key or 'saldofinal' in _key or 'patrimonioliquido' in _key or 'emprestimosefinanciamentosliq' in _key or 'emprestimosefinanciamentosbrt' in _key:
                                    _val = 0

                                # Filter & SUM all DATA:
                                if _key == 'result_sended':
                                    result_sended = _val
                                #
                                if _key not in 'result_date' and _key not in 'result_sended' and _key not in 'result_url' and _key not in 'descricao' and _key not in 'on' and _key not in 'depreciacao':

                                    # print(f'K: {_key} = {_val}')

                                    # SUB DFP - ALL ITR:
                                    if schema.get(_dr).get(_key) is None:
                                        schema[_dr][_key] = _val
                                    else:
                                        schema_val = schema.get(_dr).get(_key)
                                        schema[_dr][_key] = _val - schema_val

            """ Build Depreciação, Caixa e Património LIQ, Emprestimo/Financiamneto LIQ e BRT: """
            get_3th_depre = 0

            # Get 3th Depreciação
            for _key, _val in stock_data.get('results').get(_year).get('itr').get(f'9/{_year}').get('cfa').items():
                if 'depreciacao' in _key:
                    get_3th_depre = _val

            # Get 4th Depreciação
            for _key, _val in stock_data.get('results').get(_year).get('dfp').get('cfa').items():
                if 'depreciacao' in _key:
                    get_4th_depre = _val

                    # Update Depreciação
                    schema['cfa'][_key] = get_4th_depre - get_3th_depre

            # Get last Caixa
            for _key, _val in stock_data.get('results').get(_year).get('dfp').get('cfa').items():
                if 'saldofinal' in _key:
                    get_caixa = _val

                    # update Caixa
                    schema['cfa'][_key] = get_caixa

            # Get last Patrimonio Liq.
            for _key, _val in stock_data.get('results').get(_year).get('dfp').get('bpp').items():
                if 'patrimonioliquido' in _key:
                    get_pat = _val

                    # update Pat Liq
                    schema['bpp'][_key] = get_pat

            # Get Last emprestimosefinanciamentosliq
            for _key, _val in stock_data.get('results').get(_year).get('dfp').get('bpp').items():
                if 'emprestimosefinanciamentosliq':
                    get_emp_fin_liq = _val

                    # update Emprestimo/Financiamento Liq
                    schema['bpp'][_key] = get_emp_fin_liq

            # Get Last emprestimosefinanciamentosbrt
            for _key, _val in stock_data.get('results').get(_year).get('dfp').get('bpp').items():
                if 'emprestimosefinanciamentosbrt':
                    get_emp_fin_brt = _val

                    # update Emprestimo/Financiamento Liq
                    schema['bpp'][_key] = get_emp_fin_brt

            # Save @ DB:
            # print(json.dumps(schema, indent=4))
            cursor().update_one(
                {'key_cvm': key_cvm},
                {'$set': {f'results.{_year}.itr.12/{_year}': schema}}
            )

            print(f'Builded new 4th ITR: 12/{_year}')
