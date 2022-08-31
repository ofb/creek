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
from . import g
logger = logging.getLogger(__name__)

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

  def open_signal(self):
    if self._symbols[0] == 'AIRC' and self._symbols[1] == 'AVB':
      logger.info('Opening AIRC-AVB')
      return 1, 'AIRC', 'AVB'
    else: return 0
  def close_signal(self):
    return 0
  # async def close(self):
  #   pass
  # async def try_close(self):
  #   pass
  
  # To initialize already-open trades
  def open_init(dict):
    # ...
    self._status = 'open'
    pass