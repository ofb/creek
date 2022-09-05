import logging
import pandas as pd
import os
import asyncio
import glob
from datetime import datetime as dt
from datetime import timedelta as td
import pytz as tz
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
import tensorflow.compat.v2 as tf
tf.enable_v2_behavior()
import tensorflow_probability as tfp
tfd = tfp.distributions
from tensorflow.python.framework import errors_impl as tf_errors
import numpy as np
from . import config as g

# Things we want for our trade class:
# have a status
# be able to load and save itself
# have a hedge symbol
# record prices
# contain tensorflow model information
# record minute bars for as long as it's open

class Trade:
  """
  Trade class.
  Attributes:
    _model: contains tensorflow model
    _has_model (bool): whether model weights have been successfully
                       loaded
    _status: 'uninitialized, ''disabled', 'open', 'closed', 'opening', 
             'closing'.
             'closed' means ready to trade. 'open' mean a position has 
             successfully been opened and the trade is on.
  """

  def _LoadWeights(self):
    logger = logging.getLogger(__name__)
    negloglik = lambda y, rv_y: -rv_y.log_prob(y)
    self._model = tf.keras.Sequential([
    tf.keras.layers.Dense(1 + 1),
    tfp.layers.DistributionLambda(
      lambda t: tfd.Normal(loc=t[..., :1],
                scale=1e-3 + tf.math.softplus(0.05 * t[...,1:]))),
    ])
    self._model.compile(
                optimizer=tf.optimizers.Adam(learning_rate=0.01),
                loss=negloglik)
    try:
      self._model.load_weights('%s/checkpoints/%s' % 
                               (g.root,self._title)).expect_partial()
      self._has_model = True
      return 1
    except tf_errors.NotFoundError as e:
      logger.error(e)
      self._has_model = False
      self._status = 'disabled'
      return 0

  def __init__(self, symbol_list, pearson, pearson_historical):
    self._status = 'uninitialized'
    assert len(symbol_list) == 2
    self._symbols = symbol_list
    assert type(pearson) is float
    assert type(pearson_historical) is float
    self._pearson = pearson
    self._pearson_historical = pearson_historical
    self._title = self._symbols[0].symbol + '-' + self._symbols[1].symbol
    if not self._LoadWeights(): return
    if not self._symbols[0].tradable or not self._symbols[1].tradable:
      self._status = 'disabled'
    if not self._symbols[0].shortable or not self._symbols[1].shortable:
      self._status = 'disabled'
    self._sigma_series = pd.Series(dtype=np.float64)
    self._opened = None # To be set in pytz timezone US/Eastern
    # ...
    self._status = 'closed'

  def refresh_open(self, symbol_list):
    assert len(symbol_list) == 2
    self._symbols = symbol_list
    if not self._symbols[0].tradable or not self._symbols[1].tradable:
      self._status = 'disabled'
      return 0
    else: return 1
  
  def status(self): return self._status
  def pearson(self): return self._pearson
  def title(self): return self._title

  def _mean(self, x):
    val = np.array([[x]], dtype=np.float32)
    return float(np.squeeze(self._model(val).mean().numpy()))

  def _stddev(self, x):
    val = np.array([[x]], dtype=np.float32)
    return float(np.squeeze(self._model(val).stddev().numpy()))

  def _sigma(self, x, y):
    return abs(y - self._mean(x)) / self._stddev(x)

  def append_bar(self):
    if (not g.bars[self._symbols[0].symbol] or
        not g.bars[self._symbols[1].symbol]): return
    new_bars = (g.bars[self._symbols[0].symbol][-1],
                g.bars[self._symbols[1].symbol][-1])
    time = max(new_bars[0].timestamp, new_bars[1].timestamp)
    if (len(self._sigma_series) > 0) and (
     (time.astimezone(tz.timezone('US/Eastern')) - 
     self._sigma_series.index[-1].astimezone(tz.timezone('US/Eastern')))
     < td(seconds=50)): return # Nothing new
    sigma = self._sigma(new_bars[0].vwap, new_bars[1].vwap)
    s = pd.Series({time,sigma})
    self._sigma_series = pd.concat([self._sigma_series,s])

  def open_signal(self, clock):
    '''
    Returns a 4-tuple with the
    - True/False whether to open
    - deviation as a fraction of the standard deviation
    - the symbol to go long
    - the symbol to go short
    '''
    if len(self._sigma_series) == 0: return 0, None, None, None
    else:
      sigma = self._sigma_series[-1]
      if sigma > g.TO_OPEN_SIGNAL:
        logger = logging.getLogger(__name__)
        x = g.bars[self._symbols[0].symbol][-1].vwap
        y = g.bars[self._symbols[1].symbol][-1].vwap
        if y > self._mean(x):
          return (1, sigma, self._symbols[0].symbol, 
                            self._symbols[1].symbol)
          logger.info('%s sigma = %s, long %s short %s' % 
                      self._title, sigma, self._symbols[0].symbol, 
                                          self._symbols[1].symbol)
        else:
          return (1, sigma, self._symbols[1].symbol, 
                            self._symbols[0].symbol)
          logger.info('%s sigma = %s, long %s short %s' % 
                      self._title, sigma, self._symbols[1].symbol, 
                                          self._symbols[0].symbol)
      else: return 0, None, None, None

  def close_signal(self, clock):
    logger = logging.getLogger(__name__)
    if len(self._sigma_series) == 0:
      logger.error('%s is open but has no sigma series' % self._title)
      return 0
    sigma = self._sigma_series[-1]
    time = self._sigma_series.index[-1]
    delta = clock.now() - self._opened
    if sigma < 0.25: return 1
    elif sigma < 0.5 and delta > td(weeks=1): return 1
    elif sigma < 1 and delta > td(weeks=2): return 1
    elif sigma < 2 and delta > td(weels=3): return 1
    else: return 0

  async def try_close(self, latest_bar):
    pass

  async def try_open(self, latest_quote, latest_bar):
    '''
    Important To-Do:
    - Ensure bid-ask spread is a sufficiently small percentage of sigma.
    '''
    pass
  
  # To initialize already-open trades
  def open_init(dict):
    # ...
    self._status = 'open'
    pass

def equity(account): return max(account.equity - g.EXCESS_CAPITAL,1)

def cash(account): return max(account.cash - g.EXCESS_CAPITAL,0)

def account_ok():
  logger = logging.getLogger(__name__)
  account = g.tclient.get_account()
  if account.trading_blocked:
    logger.error('Trading blocked, exiting')
    return 0
  if account.account_blocked:
    logger.error('Account blocked, exiting')
    return 0
  if trade_suspended_by_user:
    logger.error('Trade suspended by user, exiting')
    return 0
  if not account.shorting_enabled:
    logger.error('Shorting disabled, exiting')
    return 0
  g.equity = equity(account)
  g.cash = cash(account)
  g.positions = g.tclient.get_all_positions()
  return 1

def set_trade_size():
  logger = logging.getLogger(__name__)
  g.trade_size = g.equity * g.MAX_TRADE_SIZE
  logger.info('trade_size = %s' % g.trade_size)
  return