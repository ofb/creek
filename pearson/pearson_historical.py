import os
import requests
import pandas as pd
import logging
import logging.handlers
import math
from pandarallel import pandarallel
import multiprocessing as mp

headers = {
      'APCA-API-KEY-ID':'PK52PMRHMCY15OZGMZLW',
      'APCA-API-SECRET-KEY':'F8270IxVZS3hXdghv7ChIyQUalFRIZZxYYqMKfUh'
}
url_v2 = 'https://paper-api.alpaca.markets/v2/'

last_year_cutoff = 0.99
historical_cutoff = 0.99

handler = logging.handlers.WatchedFileHandler(
    os.environ.get("LOGFILE", "pearson_historical.log"))
formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(name)s:%(message)s")
handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOGLEVEL", "INFO"))
logger.addHandler(handler)

# p will be populated with our correlation dataframe
p = pd.DataFrame()
# Frames will be populated with the dataframes for each symbol
frames = {}

def initial_truncate():
  resp = requests.get(url_v2 + 'assets', headers=headers)
  equity_raw = resp.json()
  symbol_dict = {}
  fund_symbols = ['BAM', 'BAMR', 'KKR']
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

  global p
  p = pd.read_csv('pearson.csv')
  p = p[abs(p['pearson']) >= last_year_cutoff]
  p = p[~p['symbol1'].isin(fund_symbols)]
  p = p[~p['symbol2'].isin(fund_symbols)]
  p['symbol1_name'] = p['symbol1'].map(symbol_dict)
  p['symbol2_name'] = p['symbol2'].map(symbol_dict)

def pearson(row):
  symbol1 = row['symbol1']
  symbol2 = row['symbol2']
  merged = frames[symbol1].merge(frames[symbol2], 'inner', 
								on='timestamp', suffixes=('1', '2'))
  n = len(merged)
  merged['xy'] = merged['vwap1'] * merged['vwap2']
  merged['xsquared'] = merged['vwap1'] * merged['vwap1']
  merged['ysquared'] = merged['vwap2'] * merged['vwap2']
  x = merged['vwap1'].sum()
  y = merged['vwap2'].sum()
  xy = merged['xy'].sum()
  xsquared = merged['xsquared'].sum()
  ysquared = merged['ysquared'].sum()
  return ((n * xy - x * y) / 
			math.sqrt((n * xsquared - x * x) * (n * ysquared - y * y)))

def pearson_historical():
  global p
  global frames
  active_symbols = []
  active_symbols.extend(p['symbol1'].tolist())
  active_symbols.extend(p['symbol2'].tolist())
  active_symbols = set(active_symbols)
  logger.info('Opening %s databases' % len(active_symbols))
  frame = pd.DataFrame()
  missing_bars = []
  for symbol in active_symbols:
    try:
      frame = pd.read_csv('/mnt/disks/creek-1/us_equities_hourly/%s.csv' % symbol)
      frame = frame.drop(columns=['symbol','open','high','low','close','volume','trade_count'],axis=1)
      frame.set_index('timestamp', inplace=True)
      frame.index = pd.to_datetime(frame.index)
      frames[symbol] = frame
    except FileNotFoundError as error:
      logger.warning('%s.csv not found' % symbol)
      missing_bars.append(symbol)
  logger.info('Completed loading databases')
  if len(missing_bars) > 0:
    print('There were missing bars. Exiting.')
    missing_bars_df = pd.DataFrame({'symbol': missing_bars})
    missing_bars_df.to_csv('missing_bars.csv')
    logger.error('There were missing bars.')
    return 0
  logger.info('Beginning Pearson correlation computation')
  pandarallel.initialize(nb_workers = mp.cpu_count(), progress_bar = True)
  p['pearson_historical'] = p.parallel_apply(pearson, axis=1)
  p = p[['symbol1', 'symbol2', 'pearson', 'pearson_historical', 'symbol1_name', 'symbol2_name']]
  logger.info('Computation complete')
  return 1

def historical_sort():
  global p
  p = p[abs(p['pearson_historical']) >= historical_cutoff]
  p['abs'] = abs(p['pearson_historical'])
  p.sort_values(by=['abs'], ascending=False, inplace=True)
  p.drop(['abs'], axis=1, inplace=True)

def main():
  initial_truncate()
  r = pearson_historical()
  if not r: return
  else: p.to_csv('pearson_historical_truncated.csv')

if __name__ == '__main__':
  main()