import logging
import logging.handlers
import os
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
import sys
import getopt
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



def plot_regression(x, y, m, s, symbol1, symbol2):
  plt.figure(figsize=[15, 15])  # inches
  plt.plot(x, y, 'b.', label='observed');

  plt.plot(AIRC_np, m, 'r', linewidth=4, label='mean');
  plt.plot(AIRC_np, m + 2 * s, 'g', linewidth=2, label=r'mean + 2 stddev');
  plt.plot(AIRC_np, m - 2 * s, 'g', linewidth=2, label=r'mean - 2 stddev');

  plt.ylim(np.min(y), np.max(y));
  plt.yticks(np.linspace(np.min(y), np.max(y), 20)[1:]);
  plt.xticks(np.linspace(np.min(x), np.max(x), 10)[1:]);

  ax=plt.gca();
  ax.xaxis.set_ticks_position('bottom')
  ax.yaxis.set_ticks_position('left')
  # ax.spines['left'].set_position(('data', 180))
  ax.spines['top'].set_visible(False)
  ax.spines['right'].set_visible(False)
  ax.spines['left'].set_smart_bounds(True)
  ax.spines['bottom'].set_smart_bounds(True)
  plt.legend(loc='center left', fancybox=True, framealpha=0., bbox_to_anchor=(1.05, 0.5))
  plt.savefig('regression/%s-%s.png' % (symbol1, symbol2), bbox_inches='tight', dpi=300)

def plot_loss(history, symbol1, symbol2):
  plt.plot(history.history['loss'], label='loss')
  plt.xlabel('Epoch')
  plt.ylabel('Error')
  plt.legend()
  plt.grid(True)
  plt.savefig('history/%s-%s_loss.png' % (symbol1, symbol2), bbox_inches='tight', dpi=300)

def main(argv):
  e = 25
  arg_help = "{0} -e <epochs> (default: epochs = 25)".format(argv[0])
  try:
    opts, args = getopt.getopt(argv[1:], "he:", ["help", "epochs="])
  except:
    print(arg_help)
    sys.exit(2)

  for opt, arg in opts:
    if opt in ("-h", "--help"):
      print(arg_help)
      sys.exit(2)
    elif opt in ("-e", "--epochs"):
      e = int(arg)

  symbol1 = 'AIRC'
  symbol2 = 'AVB'
  bars1 = pd.read_csv('/mnt/disks/creek-1/us_equities_2022/%s.csv' % symbol1)
  bars2 = pd.read_csv('/mnt/disks/creek-1/us_equities_2022/%s.csv' % symbol2)
  assert not bars1.empty
  assert not bars2.empty
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
  # Build probabilistic model.
  model = tf.keras.Sequential([
    tf.keras.layers.Dense(1 + 1),
    tfp.layers.DistributionLambda(
      lambda t: tfd.Normal(loc=t[..., :1],
                scale=1e-3 + tf.math.softplus(0.05 * t[...,1:]))),
  ])
  # Do inference.
  model.compile(optimizer=tf.optimizers.Adam(learning_rate=0.01), loss=negloglik)
  history = model.fit(bars1_np, bars2_np, epochs=e, verbose=True);
  plot_loss(history, symbol1, symbol2)
  yhat = model(bars1_np)
  assert isinstance(yhat, tfd.Distribution)
  m = yhat.mean()
  s = yhat.stddev()
  plot_regression(bars1_np, bars2_np, m, s, symbol1, symbol2)
  mbars['mean'] = np.squeeze(m.numpy()).tolist()
  mbars['stddev'] = np.squeeze(s.numpy()).tolist()
  mbars['dev'] = abs(mbars['vwap_2'] - mbars['mean'])/mbars['stddev']
  mbars.to_csv('%s-%s_dev.csv' % (symbol1, symbol2))
  return

if __name__ == '__main__':
  main(sys.argv)