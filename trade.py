import logging
import pandas as pd
import os
import asyncio
import glob
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
import tensorflow.compat.v2 as tf
tf.enable_v2_behavior()
import tensorflow_probability as tfp
tfd = tfp.distributions
from tensorflow.python.framework import errors_impl as tf_errors
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
      self._model.load_weights('%s/checkpoints/%s' % (g.root,self._title)).expect_partial()
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

  def open_signal(self):
    logger = logging.getLogger(__name__)
    if self._symbols[0].symbol == 'AIRC' and self._symbols[1].symbol == 'AVB':
      logger.info('Opening AIRC-AVB')
      return 1, 'AIRC', 'AVB'
    else: return 0, None, None
  def close_signal(self):
    return 0
  async def try_close(self):
    pass

  async def try_open(self):
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
  return 1

def set_trade_size():
  logger = logging.getLogger(__name__)
  equity = g.tclient.get_account().equity
  g.trade_size = equity * g.MAX_TRADE_SIZE
  logger.info('trade_size = %s' % g.trade_size)
  return