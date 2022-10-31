import logging
import logging.handlers
import os
from datetime import date
from datetime import time
from datetime import datetime as dt
from datetime import timedelta as td
import pandas as pd
import pytz as tz
import multiprocessing as mp
import glob
import config as g
import refresh_bars as rb

# This global list will contain the symbols that have been interpolated
interpolated = []

def interpolate(symbol):
  logger = logging.getLogger(__name__)
  logger.info('Interpolating %s', symbol)
  path = os.path.join(g.minute_bar_dir, symbol + '.csv')
  try:
    bars = pd.read_csv(path)
  except FileNotFoundError:
    logger.error('%s.csv not found' % symbol)
    return []
  if bars.empty:
    return []
  interpolated_bars = bars.drop(columns=['symbol','open','high','low','close','volume','trade_count'],axis=1)
  interpolated_bars.set_index('timestamp', inplace=True)
  interpolated_bars.index = pd.to_datetime(interpolated_bars.index)
  start_date = date.today().replace(year=date.today().year-1)
  buffer_start_date = start_date - td(days=7)
  end_date = date.today() - td(days=2)
  start = pd.to_datetime(start_date.strftime('%Y-%m-%d') + ' 9:30')
  start = start.tz_localize(tz.timezone('US/Eastern'))
  start = start.tz_convert('UTC')
  start_str = start.strftime('%Y-%m-%d %H:%M:%S%z')
  buffer_start = pd.to_datetime(buffer_start_date.strftime('%Y-%m-%d') + ' 9:30')
  buffer_start = buffer_start.tz_localize(tz.timezone('US/Eastern'))
  buffer_start = buffer_start.tz_convert('UTC')
  start_str = start.strftime('%Y-%m-%d %H:%M:%S%z')
  end = pd.to_datetime(end_date.strftime('%Y-%m-%d') + ' 9:30')
  end = end.tz_localize(tz.timezone('US/Eastern'))
  end = end.tz_convert('UTC')
  end_str = end.strftime('%Y-%m-%d %H:%M:%S%z')
  if (interpolated_bars.iloc[0].name > buffer_start): return []
  if (interpolated_bars.iloc[-1].name < end):
    interpolated_bars.loc[end] = interpolated_bars.iloc[-1]['vwap']
  interpolated_bars = interpolated_bars.resample('1T').interpolate('linear')
  interpolated_bars = interpolated_bars.loc[start_str:end_str]
  return interpolated_bars

def interpolate_wrapper(symbol, length):
  logger = logging.getLogger(__name__)
  b = interpolate(symbol)
  if len(b) == length:
    path = os.path.join(g.interpolated_bars_dir, symbol + '.csv')
    b.to_csv(path)
    return symbol, 1
  elif len(b) > 0:
    logger.error('%s has %s bars, should have %s bars' % (symbol, len(b), length))
  else: return symbol, 0

def interpolated_callback(result):
  global interpolated
  if result[1]:
    interpolated.append(result[0])

def pool_error_callback(error):
  logger = logging.getLogger(__name__)
  logger.error('Pool error: %s', error)

def main():
  logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(name)s:%(message)s",
    handlers=[logging.handlers.WatchedFileHandler(os.environ.get("LOGFILE", "creek-interpolate.log"))]
  )
  logger = logging.getLogger(__name__)
  path = os.path.join(g.interpolated_bars_dir, '*')
  files = glob.glob(path)
  for f in files:
    os.remove(f)
  symbol_list = list(set(rb.get_shortable_equities() + rb.get_open_symbols()))
  target_length = len(interpolate('AAPL'))
  logger.info('Target length = %s' % target_length)
  path = os.path.join(g.pearson_dir, 'pearson.config')
  with open(path, 'w') as f:
    f.write(str(target_length))
  pool = mp.Pool(mp.cpu_count())
  logger.info('Initializing %s pools', mp.cpu_count())
  symbol_list = ['AAPL', 'GOOG', 'TSLA']
  for symbol in symbol_list:
    # A special problem is the construction of tuples containing 0 or 1 
    # items: the syntax has some extra quirks to accommodate these. 
    # Empty tuples are constructed by an empty pair of parentheses; a 
    # tuple with one item is constructed by following a value with a 
    # comma (it is not sufficient to enclose a single value in 
    # parentheses). Ugly, but effective.
    pool.apply_async(interpolate_wrapper, args=(symbol, target_length), callback=interpolated_callback, error_callback=pool_error_callback)
  pool.close()
  # postpones the execution of next line of code until all processes in 
  # the queue are done.
  pool.join()
  interpolated_df = pd.DataFrame({'symbol': interpolated})
  path = os.path.join(g.pearson_dir, 'interpolated.csv')
  interpolated_df.to_csv(path)

if __name__ == '__main__':
  main()
