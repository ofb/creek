import os
import requests
import pandas as pd
import logging
import logging.handlers
import math
from datetime import date
from datetime import time
from datetime import datetime as dt
import pytz as tz
from pandarallel import pandarallel
import multiprocessing as mp # To get correct number of cpus
import sys
import numpy as np
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass
import config as g
import refresh_bars as rb

last_year_cutoff = 0.95
historical_cutoff = 0.95
sparse_cutoff = 20000

# p will be populated with our correlation dataframe
p = pd.DataFrame()
# Frames will be populated with the dataframes for each symbol
frames = {}
missing_bars = []

def initial_truncate():
  search_params = GetAssetsRequest(asset_class=AssetClass.US_EQUITY)
  assets = g.tclient.get_all_assets(search_params)
  symbol_dict = {}
  fund_symbols = ['BAM', 'BAMR', 'KKR', 'CG', 'NLY', 'TSLX']
  for element in assets:
    symbol_dict[element.symbol] = element.name
    if 'ETF' in element.name:
      fund_symbols.append(element.symbol)
    if 'ETN' in element.name:
      fund_symbols.append(element.symbol)
    if 'ProShares' in element.name:
      fund_symbols.append(element.symbol)
    if 'Direxion' in element.name:
      fund_symbols.append(element.symbol)
    if 'Fund' in element.name:
      fund_symbols.append(element.symbol)
    if 'FUND' in element.name:
      fund_symbols.append(element.symbol)
    if 'Trust' in element.name:
      fund_symbols.append(element.symbol)
    if 'TRUST' in element.name:
      fund_symbols.append(element.symbol)
    if 'iShares' in element.name:
      fund_symbols.append(element.symbol)
    if 'SPDR' in element.name:
      fund_symbols.append(element.symbol)

  global p
  path = os.path.join(g.pearson_dir, 'pearson.csv')
  p = pd.read_csv(path)
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
			
def compare_mean(row):
  symbol1 = row['symbol1']
  symbol2 = row['symbol2']
  mean1 = frames[symbol1]['vwap'].mean()
  mean2 = frames[symbol2]['vwap'].mean()
  if mean1 > mean2: return 1
  else: return 0

def get_frame(symbol, interval):
  global frames
  global missing_bars
  frame = pd.DataFrame()
  if (interval == 'Hour'):
    directory = g.hour_bar_dir
  elif (interval == 'Minute'):
    directory = g.minute_bar_dir
  try:
    path = os.path.join(directory, symbol + '.csv')
    frame = pd.read_csv(path)
    frame = frame.drop(columns=['symbol','open','high','low','close','volume','trade_count'],axis=1)
    frame.set_index('timestamp', inplace=True)
    frame.index = pd.to_datetime(frame.index)
    frames[symbol] = frame
  except FileNotFoundError as error:
    logger = logging.getLogger(__name__)
    logger.warning('%s.csv not found' % symbol)
    missing_bars.append(symbol)
  return

def get_active_symbols():
  active_symbols = []
  active_symbols.extend(p['symbol1'].tolist())
  active_symbols.extend(p['symbol2'].tolist())
  return set(active_symbols) # remove duplicates

def check_missing_bars():
  if len(missing_bars) > 0:
    logger = logging.getLogger(__name__)
    print('There were missing bars. Exiting.')
    missing_bars_df = pd.DataFrame({'symbol': missing_bars})
    missing_bars_df.to_csv('missing_bars.csv')
    logger.error('There were missing bars.')
    return 0
  else: return 1

def pearson_historical():
  logger = logging.getLogger(__name__)
  global p
  global frames
  active_symbols = get_active_symbols()
  logger.info('Opening %s hour databases' % len(active_symbols))
  for symbol in active_symbols: get_frame(symbol, 'Hour')
  logger.info('Completed loading hour databases')
  if not check_missing_bars(): return 0
  logger.info('Beginning Pearson correlation computation')
  p['pearson_historical'] = p.parallel_apply(pearson, axis=1)
  p = p[['symbol1', 'symbol2', 'pearson', 'pearson_historical', 'symbol1_name', 'symbol2_name']]
  logger.info('Computation complete')
  return 1

def is_sparse(row):
  symbol1 = row['symbol1']
  symbol2 = row['symbol2']
  merged = frames[symbol1].merge(frames[symbol2], 'inner', on='timestamp', suffixes=('1', '2'))
  cutoff = date.today().replace(year=date.today().year-3)
  if merged.iloc[0].name.date() > cutoff: return True
  one_year_date = date.today().replace(year=date.today().year-1)
  t = time(hour=0,minute=0,tzinfo=tz.timezone('UTC'))
  one_year = dt.combine(one_year_date, t)
  n = len(merged[one_year:])
  if n < sparse_cutoff: return True
  return False

def historical_sort():
  global p
  p = p[abs(p['pearson_historical']) >= historical_cutoff]
  p['abs'] = abs(p['pearson_historical'])
  p.sort_values(by=['abs'], ascending=False, inplace=True)
  p.drop(['abs'], axis=1, inplace=True)

def sparse_truncate():
  logger = logging.getLogger(__name__)
  global p
  global frames
  global missing_bars
  frames = {}
  missing_bars = []
  active_symbols = get_active_symbols()
  logger.info('Opening %s minute databases' % len(active_symbols))
  for symbol in active_symbols:
    get_frame(symbol, 'Minute')
  logger.info('Completed loading minute databases')
  if not check_missing_bars(): return 0
  logger.info('Beginning sparse truncation on minute bars with cutoff %s', sparse_cutoff)
  p['sparse'] = p.parallel_apply(is_sparse, axis=1)
  p = p[~p['sparse']]
  p = p.drop(columns=['sparse'],axis=1)
  logger.info('Sparse drop complete')
  # Reorder symbols so the one with the larger average price is second
  logger.info('Swapping symbols')
  p['swap'] = p.parallel_apply(compare_mean, axis=1)
  p[['symbol1','symbol2','symbol1_name','symbol2_name']] = p[['symbol2','symbol1','symbol2_name','symbol1_name']].where(p['swap'] == 1, p[['symbol1','symbol2','symbol1_name','symbol2_name']].values)
  p = p.drop(columns=['swap'],axis=1)
  logger.info('Swap complete')
  return 1

def main():
  logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(name)s:%(message)s",
  handlers=[logging.handlers.WatchedFileHandler(os.environ.get("LOGFILE", "creek-pearson.log"))]
  )
  logger = logging.getLogger(__name__)
  pandarallel.initialize(nb_workers = mp.cpu_count(), progress_bar = True)
  initial_truncate()
  r = pearson_historical()
  if not r: return
  historical_sort()
  if not sparse_truncate(): return
  path = os.path.join(g.pearson_dir, 'pearson_historical.csv')
  p.to_csv(path)
  path = os.path.join(g.root, 'pearson.csv')
  path2 = os.path.join(g.root, 'pearson_backup.csv')
  os.rename(path, path2)
  p.to_csv(path)

if __name__ == '__main__':
  main()