import logging
import logging.handlers
import os
import pandas as pd
from pandarallel import pandarallel
import multiprocessing as mp # To get correct number of cpus
import matplotlib.pyplot as plt
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
import tensorflow.compat.v2 as tf
tf.enable_v2_behavior()
import tensorflow_probability as tfp
tfd = tfp.distributions
from datetime import date
import pytz as tz
import numpy as np
import sys
import glob
import shutil
import config as g

# Default number of epochs
e = 100
frames = {}

def get_active_symbols(p):
  active_symbols = []
  active_symbols.extend(p['symbol1'].tolist())
  active_symbols.extend(p['symbol2'].tolist())
  return set(active_symbols) # remove duplicates

def get_frames(symbols):
  logger = logging.getLogger(__name__)
  logger.info('Fetching minute bars for %s symbols' % len(symbols))
  global frames
  frame = pd.DataFrame()
  for symbol in symbols:
    try:
      path = os.path.join(g.minute_bar_dir, symbol + '.csv')
      frame = pd.read_csv(path)
    except FileNotFoundError as error:
      logger.warning('%s.csv not found' % symbol)
      sys.exit(1)
    assert not frame.empty
    frame = frame.drop(columns=['symbol','open','high','low','close','volume','trade_count'],axis=1)
    frame.set_index('timestamp', inplace=True)
    frame.index = pd.to_datetime(frame.index)
    cutoff_date = date.today().replace(year=date.today().year-1)
    cutoff = pd.to_datetime(cutoff_date.strftime('%Y-%m-%d') + ' 9:30')
    cutoff = cutoff.tz_localize(tz.timezone('US/Eastern'))
    cutoff = cutoff.tz_convert('UTC')
    cutoff_str = cutoff.strftime('%Y-%m-%d %H:%M:%S%z')
    frames[symbol] = frame.loc[cutoff_str:]
  logger.info('Databases loaded')
  return
  

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
  path = os.path.join(g.tf_dir, 'regression', symbol1 + '-' + symbol2 + '.png')
  plt.savefig(path, bbox_inches='tight', dpi=300)
  plt.close()
  return

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
  path = os.path.join(g.tf_dir, 'loss', symbol1 + '-' + symbol2 + '.png')
  plt.savefig(path, bbox_inches='tight', dpi=300)
  return

def regress(row):
  logger = logging.getLogger(__name__)
  symbol1 = row['symbol1']
  symbol2 = row['symbol2']
  title = symbol1 + '-' + symbol2
  bars1 = frames[symbol1]
  bars2 = frames[symbol2]
  mbars = bars1.merge(bars2, how='inner', on='timestamp', suffixes=['_1','_2'])
  merged_length = len(mbars)
  # logger.info('%s has %s bars in common', (title, merged_length))
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
  # delete this - no callback
  history = model.fit(bars1_np, bars2_np, epochs=e, callbacks=[callback], verbose=False)
  # model.load_weights('./checkpoints/%s-%s' % (symbol1, symbol2)).expect_partial()
  # loss = model.evaluate(bars1_np, bars2_np, verbose=2)
  # print("Trained model, loss: %s" % loss)
  path = os.path.join(g.root, 'checkpoints', title)
  model.save_weights(path)
  computed_epochs = len(history.history['loss'])
  if computed_epochs == e:
    logger.warning('%s did not converge in %s epochs' % (title, e))
  plot_loss(history, symbol1, symbol2, computed_epochs)
  yhat = model(bars1_np)
  m = yhat.mean()
  s = yhat.stddev()
  plot_regression(bars1_np, bars2_np, m, s, symbol1, symbol2)
  mbars['mean'] = np.squeeze(m.numpy()).tolist()
  mbars['stddev'] = np.squeeze(s.numpy()).tolist()
  mbars['dev'] = abs(mbars['vwap_2'] - mbars['mean'])/mbars['stddev']
  path = os.path.join(g.tf_dir, 'dev', title + '.csv')
  mbars.to_csv(path)
  return

def clear_dir(path):
  files = glob.glob(path)
  for f in files:
    os.remove(f)
  return

def get_open_trades():
  path = os.path.join(g.root, 'open_trades', '*.json')
  files = glob.glob(path)
  symbol1 = []
  symbol2 = []
  for f in files:
    name = f.split('/')[-1]
    name = name.split('.')[0]
    symbol1.append(name.split('-')[0])
    symbol2.append(name.split('-')[1])
  return pd.DataFrame({'symbol1': symbol1, 'symbol2': symbol2})

def main():
  logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(name)s:%(message)s",
    handlers=[logging.handlers.WatchedFileHandler(os.environ.get("LOGFILE", "creek-tf.log"))]
  )
  path = os.path.join(g.tf_dir, 'dev', '*')
  clear_dir(path)
  path = os.path.join(g.tf_dir, 'regression', '*')
  clear_dir(path)
  path = os.path.join(g.tf_dir, 'loss', '*')
  clear_dir(path)
  path = os.path.join(g.tf_dir, 'old_checkpoints', '*')
  clear_dir(path)
  path = os.path.join(g.root, 'checkpoints', '*')
  files = glob.glob(path)
  for f in files:
    name = f.split('/')[-1]
    shutil.move(f, os.path.join(g.tf_dir, 'old_checkpoints', name))
  logger = logging.getLogger(__name__)
  path = os.path.join(g.pearson_dir, 'pearson_historical.csv')
  pearson = pd.read_csv(path)
  pearson = pearson[['symbol1','symbol2']]
  open_trades = get_open_trades()
  pearson = pd.concat([pearson, open_trades])
  symbols = get_active_symbols(pearson)
  get_frames(symbols)
  logger.info('Beginning regression on %s pairs over %s epochs.' % (len(pearson), e))
  pandarallel.initialize(nb_workers = mp.cpu_count(), progress_bar = True)
  pearson.parallel_apply(regress, axis=1)
  logger.info('Regression complete.')
  return

if __name__ == '__main__':
  main()