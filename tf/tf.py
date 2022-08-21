import logging
import logging.handlers
import os
import pandas as pd
from pandarallel import pandarallel
import multiprocessing as mp # To get correct number of cpus
import matplotlib.pyplot as plt
import tensorflow.compat.v2 as tf
tf.enable_v2_behavior()
import tensorflow_probability as tfp
tfd = tfp.distributions
import numpy as np
import sys
import getopt
# tf.debugging.set_log_device_placement(True)

handler = logging.handlers.WatchedFileHandler(
    os.environ.get("LOGFILE", "tf.log"))
formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(name)s:%(message)s")
handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOGLEVEL", "INFO"))
logger.addHandler(handler)

# Default number of epochs
e = 150

def plot_regression(x, y, m, s, symbol1, symbol2):
  plt.clf()
  plt.figure(figsize=[15, 15])  # inches
  plt.plot(x, y, 'b.', label='observed')

  plt.plot(x, m, 'r', linewidth=4, label='mean')
  plt.plot(x, m + 2 * s, 'g', linewidth=2, label=r'mean + 2 stddev')
  plt.plot(x, m - 2 * s, 'g', linewidth=2, label=r'mean - 2 stddev')

  plt.ylim(np.min(y), np.max(y));
  plt.yticks(np.linspace(np.min(y), np.max(y), 20)[1:])
  plt.xticks(np.linspace(np.min(x), np.max(x), 10)[1:])

  ax=plt.gca();
  ax.xaxis.set_ticks_position('bottom')
  ax.yaxis.set_ticks_position('left')
  ax.spines['top'].set_visible(False)
  ax.spines['right'].set_visible(False)
  plt.legend(loc='center left', fancybox=True, framealpha=0., bbox_to_anchor=(1.05, 0.5))
  plt.savefig('regression/%s-%s.png' % (symbol1, symbol2), bbox_inches='tight', dpi=300)

def plot_loss(history, symbol1, symbol2, e):
  plt.clf()
  plt.plot(history.history['loss'], label='loss')
  plt.ylim(-2,15)
  plt.xlim(0,e)
  plt.yticks(np.linspace(-2, 15, 17)[1:])
  plt.xticks(np.linspace(0, e, 10)[1:])
  plt.xlabel('Epoch')
  plt.ylabel('Error')
  plt.legend()
  plt.grid(True)
  plt.savefig('history/%s-%s_loss.png' % (symbol1, symbol2), bbox_inches='tight', dpi=300)

def regress(row):
  symbol1 = row['symbol1']
  symbol2 = row['symbol2']
  title = symbol1 + '-' + symbol2
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
  merged_length = len(mbars)
  logger.info('%s has %s bars in common' % (title, merged_length))
  bars1_np = np.array(mbars['vwap_1'], dtype='float32')
  bars1_np = np.expand_dims(bars1_np, axis=1)
  bars2_np = np.array(mbars['vwap_2'], dtype='float32')
  bars2_np = np.expand_dims(bars2_np, axis=1)
  negloglik = lambda y, rv_y: -rv_y.log_prob(y)
  callback = tf.keras.callbacks.EarlyStopping(monitor='loss', min_delta=0.001, patience=10, verbose=0, mode='min', restore_best_weights=1)
  # Build probabilistic model.
  model = tf.keras.Sequential([
    tf.keras.layers.Dense(1 + 1),
    tfp.layers.DistributionLambda(
      lambda t: tfd.Normal(loc=t[..., :1],
                scale=1e-3 + tf.math.softplus(0.05 * t[...,1:]))),
  ])
  # Do inference.
  model.compile(optimizer=tf.optimizers.Adam(learning_rate=0.01), loss=negloglik)
  # loss = model.evaluate(bars1_np, bars2_np, verbose=2)
  # print("Untrained model, loss: %s" % loss)
  history = model.fit(bars1_np, bars2_np, epochs=e, callbacks=[callback], verbose=False)
  # model.load_weights('./checkpoints/%s-%s' % (symbol1, symbol2)).expect_partial()
  # loss = model.evaluate(bars1_np, bars2_np, verbose=2)
  # print("Trained model, loss: %s" % loss)
  model.save_weights('checkpoints/%s' % title)
  if len(history.history['loss'] == e):
    logger.warn('%s did not converge in %s epochs' % (title, e))
  plot_loss(history, symbol1, symbol2, e)
  yhat = model(bars1_np)
  m = yhat.mean()
  s = yhat.stddev()
  plot_regression(bars1_np, bars2_np, m, s, symbol1, symbol2)
  mbars['mean'] = np.squeeze(m.numpy()).tolist()
  mbars['stddev'] = np.squeeze(s.numpy()).tolist()
  mbars['dev'] = abs(mbars['vwap_2'] - mbars['mean'])/mbars['stddev']
  mbars.to_csv('dev/%s_dev.csv' % title)
  return

def main(argv):
  global e
  arg_help = "{0} -e <epochs> (default: epochs = 150)".format(argv[0])
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
  pearson = pd.read_csv('pearson.csv')
  logger.info('Beginning regression on %s pairs over %s epochs.' % (len(pearson), e))
  pandarallel.initialize(nb_workers = mp.cpu_count(), progress_bar = True)
  pearson.parallel_apply(regress, axis=1)
  # For testing a single symbol
  # d = {'symbol1': 'AIRC', 'symbol2': 'AVB'}
  # d = {'symbol1': 'LILA', 'symbol2': 'LILAK'}
  # d = {'symbol1': 'FVRR', 'symbol2': 'BLZE'}
  # d = {'symbol1': 'NVR', 'symbol2': 'PHM'}
  # regress(d)
  logger.info('Regression complete.')
  return

if __name__ == '__main__':
  main(sys.argv)