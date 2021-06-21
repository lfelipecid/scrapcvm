import json

from db_process.connect_db import cursor
from datetime import datetime as dt


def build_ttm(key_cvm):
    # Load DB:
    data_db = cursor().find({'key_cvm': key_cvm})

    # Unpack DATA & Create List:
    for stock in data_db:

        # Crete list with all ITR & Sort
        last_results = []
        for _year in stock.get('results'):
            for _typ in stock.get('results').get(_year):
                if _typ == 'itr':
                    for _date in stock.get('results').get(_year).get(_typ):
                        last_results.append(_date)

        def sort_date(e):
            e = dt.timestamp(dt.strptime(e, '%m/%Y'))
            return e

        last_results.sort(key=sort_date, reverse=True)

        # Build TTM
        raw_ttm = {
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

        first_ttm = str(last_results[0])
        _month_ttm = str(dt.strptime(first_ttm, '%m/%Y').month)
        _year_ttm = str(dt.strptime(first_ttm, '%m/%Y').year)
        formated_ttm = f'{_month_ttm}/{_year_ttm}'

        try:
            check_ttm = True if stock.get('results').get(_year_ttm).get('ttm').get(formated_ttm) is not None else False
        except:
            check_ttm = False

        if not check_ttm and _month_ttm != '12':
            for _last in last_results[:4]:
                _year = str(dt.strptime(_last, '%m/%Y').year)
                for _key, _val in stock.get('results').get(_year).get('itr').get(_last).get('res').items():

                    if _key not in 'result_date' and _key not in 'pat_liq' and _key not in 'caixa' and _key not in 'dividabruta':
                        if raw_ttm[_key] is None:
                            raw_ttm[_key] = _val
                        else:
                            raw_ttm[_key] += _val

            # Get Last Res (PAT. Liq, Caixa, Divida):
            for _key, _val in stock.get('results').get(_year_ttm).get('itr').get(first_ttm).get('res').items():
                if _key == 'pat_liq':
                    raw_ttm[_key] = _val

                if _key == 'caixa':
                    raw_ttm[_key] = _val

                if _key == 'dividabruta':
                    raw_ttm[_key] = _val

            # Create result_date
            raw_ttm['result_date'] = formated_ttm

            # Save @ DB:
            # print(json.dumps(raw_ttm, indent=4))
            cursor().update_one(
                {'key_cvm': key_cvm},
                {'$set': {f'results.{_year_ttm}.ttm.{formated_ttm}': raw_ttm}}
            )
            print(f'\tTTM has created = {formated_ttm}')
