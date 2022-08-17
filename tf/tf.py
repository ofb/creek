import logging
import logging.handlers
import os
import time
from datetime import date
from datetime import timedelta
import pandas as pd
import pytz
import multiprocessing as mp
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.common.exceptions import APIError
import matplotlib.pyplot as plt
import tensorflow.compat.v2 as tf
tf.enable_v2_behavior()
import tensorflow_probability as tfp
tfd = tfp.distributions
import numpy as np
# keys required for stock historical data client
# these are the paper trading keys
client = StockHistoricalDataClient('PK52PMRHMCY15OZGMZLW', 'F8270IxVZS3hXdghv7ChIyQUalFRIZZxYYqMKfUh')
headers = {
      'APCA-API-KEY-ID':'PK52PMRHMCY15OZGMZLW',
      'APCA-API-SECRET-KEY':'F8270IxVZS3hXdghv7ChIyQUalFRIZZxYYqMKfUh'
}

url_v2 = 'https://paper-api.alpaca.markets/v2/'

handler = logging.handlers.WatchedFileHandler(
    os.environ.get("LOGFILE", "interpolate.log"))
formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(name)s:%(message)s")
handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOGLEVEL", "INFO"))
logger.addHandler(handler)

# This global list will contain the symbols whose datatable is nonempty
nonempty_list = []
# Build probabilistic model.
model = tf.keras.Sequential([
  tf.keras.layers.Dense(1 + 1),
  tfp.layers.DistributionLambda(
      lambda t: tfd.Normal(loc=t[..., :1],
                scale=1e-3 + tf.math.softplus(0.05 * t[...,1:]))),
])

def interpolate(symbol):
  logger.info('Interpolating %s', symbol)
  bars = pd.read_csv('/mnt/disks/creek-1/us_equities_2022/%s.csv' % symbol)
  if bars.empty:
    return [symbol,0]
  interpolated_bars = bars.drop(columns=['symbol','open','high','low','close','volume','trade_count'],axis=1)
  interpolated_bars.set_index('timestamp', inplace=True)
  interpolated_bars.index = pd.to_datetime(interpolated_bars.index)
  # Markets open 2022-01-03 09:30 in US/Eastern
  market_open_pytz = pd.to_datetime("01-03-2022 9:30")
  tz_pytz = pytz.timezone("US/Eastern")
  market_open_pytz = market_open_pytz.tz_localize(tz_pytz)
  market_open_pytz = market_open_pytz.tz_convert('UTC')
  market_open_str = market_open_pytz.strftime('%Y-%m-%d %H:%M:%S%z')
  # Last open day was August 5, 2022
  market_close_pytz = pd.to_datetime('08-05-2022 16:00')
  market_close_pytz = market_close_pytz.tz_localize(tz_pytz)
  market_close_pytz = market_close_pytz.tz_convert('UTC')
  market_close_str = market_close_pytz.strftime('%Y-%m-%d %H:%M:%S%z')
  # Interpolate missing minutes
  # There should be 308491 rows at the end of the interpolation
  if (interpolated_bars.iloc[0].name > market_open_pytz):
    interpolated_bars.loc[market_open_pytz] = interpolated_bars.iloc[0]['vwap']
  if (interpolated_bars.iloc[-1].name < market_close_pytz):
    interpolated_bars.loc[market_close_pytz] = interpolated_bars.iloc[-1]['vwap']
  interpolated_bars = interpolated_bars.resample('1T').interpolate('linear')
  interpolated_bars = interpolated_bars.loc[market_open_str:market_close_str]
  interpolated_bars.to_csv('/mnt/disks/creek-1/us_equities_2022_interpolated/%s.csv' % symbol) 
  if (len(interpolated_bars) != 308491):
    logger.error('Wrong number of bars for %s', symbol)
  return [symbol,1]

def isempty_callback(result):
  global nonempty_list
  if result[1]:
    nonempty_list.append(result[0])

def pool_error_callback(error):
  logger.error('Pool error: %s', error)

def m():
  pool = mp.Pool(mp.cpu_count())
  logger.info('Initializing %s pools', mp.cpu_count())
  equity_list = pd.read_csv('shortable_equity_list.csv')
  shortable_list = equity_list['symbol'].tolist()
  for symbol in shortable_list:
    # A special problem is the construction of tuples containing 0 or 1 
    # items: the syntax has some extra quirks to accommodate these. 
    # Empty tuples are constructed by an empty pair of parentheses; a 
    # tuple with one item is constructed by following a value with a 
    # comma (it is not sufficient to enclose a single value in 
    # parentheses). Ugly, but effective.
    pool.apply_async(interpolate, args=(symbol, ), callback=isempty_callback, error_callback=pool_error_callback)
  pool.close()
  # postpones the execution of next line of code until all processes in 
  # the queue are done.
  pool.join()
  nonempty_list_df = pd.DataFrame({'symbol': nonempty_list})
  nonempty_list_df.to_csv('nonempty_shortable_equity_list.csv')





def plot_loss(history):
  plt.plot(history.history['loss'], label='loss')
  plt.xlabel('Epoch')
  plt.ylabel('Error')
  plt.legend()
  plt.grid(True)
  plt.savefig('loss.png', bbox_inches='tight', dpi=300)

def perce(row):
  inpu = np.array([[row['vwap_1']]], dtype=np.float32)
  val2 = row['vwap_2']
  mean = np.squeeze(model(inpu).mean().numpy())
  stddev = np.squeeze(model(inpu).stddev().numpy())
  return abs(val2 - mean)/stddev

def main():
  global model
  bars1 = pd.read_csv('/mnt/disks/creek-1/us_equities_2022/AIRC.csv')
  bars2 = pd.read_csv('/mnt/disks/creek-1/us_equities_2022/AVB.csv')
  assert not bars1.empty
  bars1 = bars1.drop(columns=['symbol','open','high','low','close','volume','trade_count'],axis=1)
  bars1.set_index('timestamp', inplace=True)
  bars1.index = pd.to_datetime(bars1.index)
  bars2 = bars2.drop(columns=['symbol','open','high','low','close','volume','trade_count'],axis=1)
  bars2.set_index('timestamp', inplace=True)
  bars2.index = pd.to_datetime(bars2.index)
  mbars = bars1.merge(bars2, how='inner', on='timestamp', suffixes=['_1','_2'])
  bars1_np = np.array(mbars['vwap_1'], dtype='float32')
  bars1_np = np.expand_dims(bars1_np, axis=1)
  bars2_np = np.array(mbars['vwap_2'], dtype='float32')
  bars2_np = np.expand_dims(bars2_np, axis=1)
  negloglik = lambda y, rv_y: -rv_y.log_prob(y)
  # Do inference.
  model.compile(optimizer=tf.optimizers.Adam(learning_rate=0.01), loss=negloglik)
  history = model.fit(bars1_np, bars2_np, epochs=25, verbose=True);
  plot_loss(history)
  yhat = model(bars1_np)
  assert isinstance(yhat, tfd.Distribution)
  mbars['stddev'] = mbars.apply(perce, axis=1)
  mbars.to_csv('AIRC_AVB_stddev.csv')
  return

if __name__ == '__main__':
  main()