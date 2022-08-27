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
from creek import root
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
      self._model.load_weights('%s/checkpoints/%s' % (root,self.title)).expect_partial()
      self._has_model = True
      return 1
    except tf_errors.NotFoundError as e:
      logger.error(e)
      self._has_model = False
      self._status = 'disabled'
      return 0

  def __init__(self, symbol1, symbol2, pearson, pearson_historical):
    self._status = 'uninitialized'
    self.symbol1 = symbol1
    self.symbol2 = symbol2
    self._pearson = pearson
    self._pearson_historical = pearson_historical
    self.title = symbol1 + '-' + symbol2
    if not self._LoadWeights(): return
    # ...
    self._status = 'closed'
  
  # To initialize already-open trades
  def initialize(dict):
    pass

# To minimize API calls, it's often useful to make a request for several 
# trades at once.
class TradeTableau:
  pass

async def monitor():
  pass