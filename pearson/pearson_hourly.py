import os
import pandas as pd
import logging
import logging.handlers
import math
from pandarallel import pandarallel

pandarallel.initialize(progress_bar = True)

handler = logging.handlers.WatchedFileHandler(
    os.environ.get("LOGFILE", "pearson_hourly.log"))
formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(name)s:%(message)s")
handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOGLEVEL", "INFO"))
logger.addHandler(handler)

p = pd.read_csv('pearson_9plus.csv')
p.drop(columns=['Unnamed: 0'], axis=1)
active_symbols = []
active_symbols.extend(p['symbol1'].tolist())
active_symbols.extend(p['symbol2'].tolist())
active_symbols = set(active_symbols)
frames = {}
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

def main():
  global missing_bars
  if len(missing_bars) > 0:
    print('There were missing bars. Exiting.')
    missing_bars_df = pd.DataFrame({'symbol': missing_bars})
    missing_bars_df.to_csv('missing_bars.csv')
    logger.error('There were missing bars.')
    return
  global p
  logger.info('Beginning Pearson correlation computation')
  p['pearson_historical'] = p.parallel_apply(pearson, axis=1)
  p = p[['symbol1', 'symbol2', 'pearson', 'pearson_historical', 'symbol1_name', 'symbol2_name']]
  logger.info('Computation complete')
  p.to_csv('pearson_9plus_historical.csv')

if __name__ == '__main__':
  main()