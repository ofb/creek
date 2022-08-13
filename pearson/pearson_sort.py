import os
import requests
import pandas as pd

headers = {
      'APCA-API-KEY-ID':'PK52PMRHMCY15OZGMZLW',
      'APCA-API-SECRET-KEY':'F8270IxVZS3hXdghv7ChIyQUalFRIZZxYYqMKfUh'
}
url_v2 = 'https://paper-api.alpaca.markets/v2/'

def main():
  resp = requests.get(url_v2 + 'assets', headers=headers)
  equity_raw = resp.json()
  symbol_dict = {}
  fund_symbols = []
  for element in equity_raw:
    symbol_dict[element['symbol']] = element['name']
    if 'ETF' in element['name']:
      fund_symbols.append(element['symbol'])
    if 'ETN' in element['name']:
      fund_symbols.append(element['symbol'])
    if 'ProShares' in element['name']:
      fund_symbols.append(element['symbol'])
    if 'Direxion' in element['name']:
      fund_symbols.append(element['symbol'])
    if 'Fund' in element['name']:
      fund_symbols.append(element['symbol'])
    if 'Trust' in element['name']:
      fund_symbols.append(element['symbol'])
    if 'iShares' in element['name']:
      fund_symbols.append(element['symbol'])
    if 'SPDR' in element['name']:
      fund_symbols.append(element['symbol'])



  p = pd.read_csv('pearson.csv')
  p = p[abs(p['pearson']) >= 0.9]
  p = p[~p['symbol1'].isin(fund_symbols)]
  p = p[~p['symbol2'].isin(fund_symbols)]
  p['abs'] = abs(p['pearson'])
  p.sort_values(by=['abs'], ascending=False, inplace=True)
  p.drop(['abs'], axis=1, inplace=True)
  p['symbol1_name'] = p['symbol1'].map(symbol_dict)
  p['symbol2_name'] = p['symbol2'].map(symbol_dict)
  p.to_csv('pearson_9plus.csv')
  print('Database is %s rows long' % len(p))

if __name__ == '__main__':
  main()