import logging
import pandas as pd
import os
import asyncio
import glob
import time
from datetime import datetime as dt
from datetime import timedelta as td
import pytz as tz
from fractions import Fraction
import math
import json
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
import tensorflow.compat.v2 as tf
tf.enable_v2_behavior()
import tensorflow_probability as tfp
tfd = tfp.distributions
from tensorflow.python.framework import errors_impl as tf_errors
import numpy as np
from alpaca.trading.requests import LimitOrderRequest
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.requests import ReplaceOrderRequest
from alpaca.data.requests import StockLatestTradeRequest
from alpaca.common.exceptions import APIError
from . import config as g

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
    self._position = [{'side':None,'qty':0,'avg_entry_price':0.0},
                      {'side':None,'qty':0,'avg_entry_price':0.0}]
    self._hedge_position = {'symbol':g.HEDGE_SYMBOL, 'side':'long',
                           'notional':0.0,'qty':0,'avg_entry_price':0.0}
    self._status = 'closed'
  
  # To initialize already-open trades
  def open_init(dict, sigma_series):
    logger = logging.getLogger(__name__)
    self._status = dict['status']
    self._opened = dt.fromisoformat(dict['opened']).astimezone(tz.timezone('US/Eastern'))
    self._position = dict['position']
    self._hedge_position = dict['hedge']
    self._sigma_series = sigma_series
    if not self._symbols[0].tradable or not self._symbols[1].tradable:
      self._status = 'disabled'
      logger.error('%s is open but not tradable' % self._title)
    if not self._symbols[0].shortable or not self._symbols[1].shortable:
      self._status = 'disabled'
      logger.error('%s is open but not shortable' % self._title)
    return
  
  def status(self): return self._status
  def pearson(self): return self._pearson
  def title(self): return self._title
  def get_position(self):
    return {self._symbols[0].symbol:self._position[0],
            self._symbols[1].symbol:self._position[1],
            self._hedge_position['symbol']:self._hedge_position}
  def get_sigma_series(self):
    if self._status == 'open': return self._sigma_series[self._opened:]
    else: return self._sigma_series

  def to_dict(self):
    d = {
      'status': self._status,
      'symbols': [self._symbols[0].symbol, self._symbols[1].symbol],
      'pearson': self._pearson,
      'pearson_historical': self._pearson_historical,
      'title': self._title,
      'opened': self._opened.isoformat(),
      'position': self._position,
      'hedge': self._hedge_position,
    }
    return d

  def fill_hedge(self, price):
    self._hedge_position['avg_entry_price'] = price
    self._hedge_position['qty'] = self._hedge_position['notional']/price
    return

  def _mean(self, x):
    val = np.array([[x]], dtype=np.float32)
    return float(np.squeeze(self._model(val).mean().numpy()))

  def _stddev(self, x):
    val = np.array([[x]], dtype=np.float32)
    return float(np.squeeze(self._model(val).stddev().numpy()))

  def _sigma(self, x, y):
    return abs(y - self._mean(x)) / self._stddev(x)

  def _stddev_x(self, x):
    xpoints = np.array([[x], [2 * x]], dtype='float32')
    m = self._model(xpoints)
    ypoints = np.squeeze(m.mean().numpy())
    ydelta = self._stddev(x)
    return ( ydelta  / (ypoints[1] - ypoints[0]) ) * x 

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
    s = pd.Series({time:sigma})
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
    if self._title in g.burn_list: return 0, None, None, None
    else:
      sigma = self._sigma_series[-1]
      if sigma > g.TO_OPEN_SIGNAL:
        x = g.bars[self._symbols[0].symbol][-1].vwap
        y = g.bars[self._symbols[1].symbol][-1].vwap
        if y > self._mean(x):
          return (1, sigma, self._symbols[0].symbol, 
                            self._symbols[1].symbol)
        else:
          return (1, sigma, self._symbols[1].symbol, 
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

  def bail_out_signal(self, clock):
    '''
    We implement some bail-out checks:
    - If the mean of the standard deviation over the last 5 minute bars     
    exceeds 6
    - If the mean of the standard deviation over the last week exceeds 4
    '''
    logger = logging.getLogger(__name__)
    recent = self._sigma_series[-5:].to_list()
    if len(recent == 5) and sum(recent)/5 > 6:
      logger.info('Last 5 bars of %s have average sigma > 6, bailing out' % self._title)
      g.burn_list.append(self._title)
      return 1
    if (clock.now() - self._opened) > td(days=7):
      recent = self._sigma_series[(clock.now() - td(days=7)):].to_list()
      if sum(recent)/max(len(recent),1) > 4:
        logger.info('Last week of bars of %s have average sigma > 4, bailing out' % self._title)
        g.burn_list.append(self._title)
        return 1
    return 0

  async def bail_out(self, clock):
    '''
    Get the hell out
    '''
    logger = logging.getLogger(__name__)
    _short = 0 if self._position[0]['side'] == 'short' else 1
    _long = int(abs(1-to_short))
    self._status = 'closed'
    logger.info('Getting the hell out of %s' % self._title)
    short_request = MarketOrderRequest(
                              symbol = self._symbols[_short].symbol,
                              qty = self._position[_short]['qty'],
                              side = 'buy',
                              time_in_force = 'day')
    long_request = MarketOrderRequest(
                              symbol = self._symbols[_long].symbol,
                              qty = self._position[_long]['qty'],
                              side = 'sell',
                              time_in_force = 'day')
    g.orders[self._title] = {'buy': None, 'sell': None}
    filled = await asyncio.gather(market_qty(short_request,self._title), 
                         market_qty(long_request, self._title))
    for i in range(2):
      j = _long if i else _short
      avg_exit_price[j] = filled[i][1]
      if not filled[i][0]: logger.info('Bailed out of %s in trade %s' % (self._symbols[j].symbol, self._title))
      else: logger.error('Unable to bail out of %s in trade %s' %
                         (self._symbols[j].symbol, self._title))
    self._status = 'closed'
    logger.info('%s closed' % self._title)
    closed_self = ClosedTrade(self, clock.now(), avg_exit_price)
    g.closed_trades.append(closed_self)
    self._position = [{'side':None,'qty':0,'avg_entry_price':0.0},
                      {'side':None,'qty':0,'avg_entry_price':0.0}]
    return (self._hedge_position['symbol'],
            -1 * self._hedge_position['qty'], closed_self)



  async def try_close(self, clock, latest_quote, latest_trade):
    logger = logging.getLogger(__name__)
    if self._symbols[0].symbol not in latest_trade.keys():
      logger.warn('%s not in latest_trade.keys()' % self._symbols[0].symbol)
      return 0
    if self._symbols[1].symbol not in latest_trade.keys():
      logger.warn('%s not in latest_trade.keys()' % self._symbols[1].symbol)
      return 0
    price = (latest_trade[self._symbols[0].symbol].price,
             latest_trade[self._symbols[1].symbol].price)
    if (self._position[0]['qty'] == 0) or (self._position[1]['qty']):
      logger.error('Trying to close an empty position %s', self._title)
      hedge_qty = self._hedge_position['qty']
      self._hedge_position = {'symbol':g.HEDGE_SYMBOL, 
                              'side':'long','notional':0.0,
                              'qty':0,'avg_entry_price':0.0}
      self._status = 'closed'
      return (self._hedge_position['symbol'], -1 * hedge_qty)
    _short = 0 if self._position[0]['side'] == 'short' else 1
    _long = int(abs(1-to_short))
    bid_ask = compute_bid_ask(latest_quote, self._symbols)
    stddev = self._stddev(price[0])
    stddev_x = self._stddev_x(price[0]) # signed float
    self._status = 'closing'
    logger.info('Closing %s, long %s, short %s' %
                (self._title, self._symbols[_long],
                 self._symbols[_short]))
    short_cushion = stddev * g.SIGMA_CUSHION if _short else abs(stddev_x) * g.SIGMA_CUSHION
    short_limit = price[_short] + min(bid_ask[_short],short_cushion)
    short_limit = round(short_limit, 2)
    long_cushion = stddev * g.SIGMA_CUSHION if _long else abs(stddev_x) * g.SIGMA_CUSHION
    long_limit = price[to_long] - min(bid_ask[_long],long_cushion)
    long_limit = round(long_limit, 2)
    short_request = LimitOrderRequest(
                      symbol = self._symbols[_short].symbol,
                      qty = self._position[_short]['qty'],
                      side = 'buy',
                      time_in_force = 'day',
                      client_order_id = stamp(self._title),
                      limit_price = short_limit
                      )
    long_request = LimitOrderRequest(
                      symbol = self._symbols[_long].symbol,
                      qty = self._position[_long]['qty'],
                      side = 'sell',
                      time_in_force = 'day',
                      client_order_id = stamp(self._title),
                      limit_price = long_limit
                      )
    g.orders[self._title] = {'buy': None, 'sell': None}
    short_order = await try_submit(short_request)
    long_order = await try_submit(long_request)
    filled = await asyncio.gather(
               limit_qty(short_request, self._title, short_cushion, 
                         bid_ask[_short]),
               limit_qty(long_request, self._title, long_cushion, 
                         bid_ask[_long]))
    avg_exit_price = {}
    for i in range(2):
      j = _long if i else _short
      avg_exit_price[j] = filled[i][1]
      if filled[i][0] == self._position[j]['qty']: 
        logger.info('Successfully closed %s in trade %s' % (self._symbols[j].symbol, self._title))
      else: logger.error('Only closed %s/%s shares of %s in trade %s' %
                         (filled[i][0], 
                         self._position[_short]['qty'],
                         self._symbols[j].symbol, self._title))
    self._status = 'closed'
    logger.info('%s closed' % self._title)
    closed_self = ClosedTrade(self, clock.now(), avg_exit_price)
    g.closed_trades.append(closed_self)
    self._position = [{'side':None,'qty':0,'avg_entry_price':0.0},
                      {'side':None,'qty':0,'avg_entry_price':0.0}]
    return (self._hedge_position['symbol'],
            -1 * self._hedge_position['qty'], closed_self)






  async def try_open(self, clock, latest_quote, latest_trade):
    logger = logging.getLogger(__name__)
    if self._symbols[0].symbol not in latest_trade.keys():
      logger.warn('%s not in latest_trade.keys()' % self._symbols[0].symbol)
      return 0
    if self._symbols[1].symbol not in latest_trade.keys():
      logger.warn('%s not in latest_trade.keys()' % self._symbols[1].symbol)
      return 0

    price = (latest_trade[self._symbols[0].symbol].price,
             latest_trade[self._symbols[1].symbol].price)
    if price[0] > (g.trade_size / 2):
      logger.info('Passing on %s as one share of %s costs %s, whereas the max trade size is %s' % (self._title, self._symbols[0].symbol, price[0], g.trade_size))
      return 0
    if price[1] > g.trade_size / 2:
      logger.info('Passing on %s as one share of %s costs %s, whereas the max trade size is %s' % (self._title, self._symbols[1].symbol, price[1], g.trade_size))
      return 0
    sigma = self._sigma(price[0], price[1])
    if sigma < g.TO_OPEN_SIGNAL: return 0
    bid_ask = compute_bid_ask(latest_quote, self._symbols)
    stddev = self._stddev(price[0])
    if stddev < 10 * bid_ask[0]:
      logger.info('Passing on %s as bid-ask spread for %s = %s while stddev = %s' % (self._title, self._symbols[0].symbol, bid_ask[0], stddev))
      return 0
    stddev_x = self._stddev_x(price[0]) # signed float
    if abs(stddev_x) < 10 * bid_ask[1]:
      logger.info('Passing on %s as bid-ask spread for %s = %s while |stddev_x| = %s' % (self._title, self._symbols[1].symbol, bid_ask[1], abs(stddev_x)))
      return 0

    mean = self._mean(price[0])
    if price[1] > mean:
      to_long = 0
      to_short = 1
    else:
      to_long = 1
      to_short = 0

    self._status = 'opening'
    logger.info('Opening %s, long %s, short %s' %
                (self._title, self._symbols[to_long],
                 self._symbols[to_short]))

    if price[1] >= price[0]:
      expensive = 1
      cheap = 0
    else:
      expensive = 0
      cheap = 1
    mm = min_max(Fraction(price[cheap]/price[expensive]),
                 math.floor((g.trade_size/2) / price[cheap]))
    if cheap == to_short: # want denominator big; i.e. lower bound
      shares_to_short = mm[0][1]
      shares_to_long = mm[0][0]
    else:  # want demoninator smaller; i.e. upper bound
      shares_to_long = mm[1][1]
      shares_to_short = mm[1][0]
    if (shares_to_short * price[to_short] 
        - shares_to_long * price[to_long]) < 0:
      logger.error('Long position is larger than short position')
      return 0
    multiple = min(math.floor( (g.trade_size / 2) / 
                   (shares_to_short * price[to_short]) ),
                   math.floor( (g.trade_size / 2) / 
                   (shares_to_long * price[to_long]) ))
    if multiple > 1:
      shares_to_short = shares_to_short * multiple
      shares_to_long = shares_to_long * multiple
    qty_requested = (shares_to_short, shares_to_long)


    short_cushion = stddev * g.SIGMA_CUSHION if to_short else abs(stddev_x) * g.SIGMA_CUSHION
    short_limit = price[to_short] - min(bid_ask[to_short],short_cushion)
    short_limit = round(short_limit, 2)
    long_cushion = stddev * g.SIGMA_CUSHION if to_long else abs(stddev_x) * g.SIGMA_CUSHION
    long_limit = price[to_long] + min(bid_ask[to_long],long_cushion)
    long_limit = round(long_limit, 2)
    short_request = LimitOrderRequest(
                      symbol = self._symbols[to_short].symbol,
                      qty = shares_to_short,
                      side = 'sell',
                      time_in_force = 'day',
                      client_order_id = stamp(self._title),
                      limit_price = short_limit
                      )
    long_request = LimitOrderRequest(
                      symbol = self._symbols[to_long].symbol,
                      qty = shares_to_long,
                      side = 'buy',
                      time_in_force = 'day',
                      client_order_id = stamp(self._title),
                      limit_price = long_limit
                      )
    logger.info('Submitting long order for %s, qty=%s, limit price=%s' % (self._symbols[to_long].symbol, shares_to_long, long_limit))
    logger.info('Submitting short order for %s, qty=%s, limit price=%s' % (self._symbols[to_short].symbol, shares_to_short, short_limit))
    g.orders[self._title] = {'buy': None, 'sell': None}
    filled = await asyncio.gather(
             limit_qty(short_request, self._title, short_cushion, 
                       bid_ask[to_short]),
             limit_qty(long_request, self._title, long_cushion, 
                       bid_ask[to_long]))
    avg_exit_price = {}
    for i in range(2):
      j = to_long if i else to_short
      avg_exit_price[j] = filled[i][1]
      if filled[i][0] == qty_requested[i]:
        logger.info('Successfully opened %s in trade %s' % (self._symbols[j].symbol, self._title))
      else: logger.error('Only opened %s/%s shares of %s in trade %s' %
                         (filled[i][0], qty_requested[i],
                         self._symbols[j].symbol, self._title))

    self._position[to_short]={'side':'short', 'qty':filled[0][0],
                              'avg_entry_price':filled[0][1]}
    self._position[to_long]={'side':'long', 'qty':filled[1][0], 
                             'avg_entry_price':filled[1][1]}
    self._status = 'open'
    self._opened = clock.now()
    hedge_notional = (self._position[to_short]['qty']
                      * self._position[to_short]['avg_entry_price']
                      - self._position[to_long]['qty']
                      * self._position[to_long]['avg_entry_price'])
    if hedge_notional < 0:
      logger.error('%s: long position in %s exceeds short position in %s by %s; position will be unhedged' % (self._title, self._symbols[to_long].symbol, self._symbols[to_short].symbol, abs(hedge_notional)))
      self._hedge_position = {'symbol':g.HEDGE_SYMBOL, 
                              'side':'long','notional':0.0,
                              'qty':0,'avg_entry_price':0.0}
      return 0
    else:
      self._hedge_position = {'symbol':g.HEDGE_SYMBOL, 
                              'side':'long','notional':hedge_notional,
                              'qty':0,'avg_entry_price':0.0}
      return hedge_notional

class ClosedTrade:
  def __init__(self, trade, closed, avg_exit_price, hedge_exit_price):
    self._symbols = trade._symbols
    self._pearson = trade._pearson
    self._pearson_historical = trade._pearson_historical
    self._title = trade._title
    self._sigma_series = trade._sigma_series
    self._opened = trade._opened
    self._position = trade._position
    self._hedge_position = trade._hedge_position
    self._closed = closed
    self._position[0]['avg_exit_price'] = avg_exit_price[0]
    self._position[1]['avg_exit_price'] = avg_exit_price[1]
    self._hedge_position['avg_exit_price'] = 0.0
    for i in range(2):
      j = 1 if self._position[i]['side'] == 'long' else -1
      self._position[i]['profit-loss'] = j * ((
        self._position[i]['avg_exit_price'] -
        self._position[i]['avg_entry_price']) *
        self._position[i]['qty'])
    self._pl = 0.0
    self._percent = 0.0

  def set_hedge_exit_price(self, price):
    self._hedge_position['avg_exit_price'] = price
    
    j = 1 if self._hedge_position['side'] == 'long' else -1
    self._hedge_position['profit-loss'] = j * ((
      self._hedge_position['avg_exit_price'] -
      self._hedge_position['avg_entry_price']) *
      self._hedge_position['qty'])
    self._pl = (self._position[0]['profit-loss']
                + self._position[1]['profit-loss']
                + self._hedge_position['profit-loss'])
    self._percent = self._pl / (
      self._hedge_position['avg_entry_price'] * 
      self._hedge_position['qty'] +
      self._position[0]['avg_entry_price'] *
      self._position[0]['qty'] +
      self._position[1]['avg_entry_price'] *
      self._position[1]['qty'])
    return
  
  def closed(self): return self._closed
  def title(self): return self._title
  def get_sigma_series(self):
    return self._sigma_series[self._opened:self._closed]
  def get_pl(): return self._pl
  def to_dict(self):
    d = {
      'title': self._title,
      'profit-loss': self._pl,
      'return': self._percent,
      'opened': self._opened.isoformat(),
      'closed': self._closed.isoformat(),
      'position': self._position,
      'hedge': self._hedge_position,
      'pearson': self._pearson,
      'pearson_historical': self._pearson_historical,
      'status': self._status,
      'symbols': [self._symbols[0].symbol, self._symbols[1].symbol],
    }

def get_latest_trade(symbol_or_symbols):
  trade_request = StockLatestTradeRequest(
                  symbol_or_symbols=symbol_or_symbols)
  return g.hclient.get_stock_latest_trade(trade_request)

async def market_qty(r, title):
  logger = logging.getLogger(__name__)
  qty = r.qty
  qty_filled = 0
  qty_requested = qty
  prices = []
  for i in range(20):
    request = MarketOrderRequest(symbol = r.symbol,
                                 qty = qty_requested,
                                 side = r.side,
                                 client_order_id = stamp(title),
                                 time_in_force = 'day')
    response = await try_submit(request)
    if type(response) is int:
      if response < qty_requested:
        qty_requested = response
        continue
      else:
        logger.error('Trade in %s rejected for lack of available shares but available shares exceed request' % r.symbol)
        break
    elif response is not None:
      await asyncio.sleep(2)
      if g.orders[title][r.side].status == 'filled':
        prices.append((qty_requested,
               float(g.orders[title][r.side].filled_avg_price)))
        qty_filled = qty_filled + qty_requested
        if qty_filled < qty:
          qty_requested = qty - qty_filled
          continue
        else: break
      else:
        logger.error('Market %s order %s for %s not filled: status %s'  
                   % (r.side, g.orders[title][r.side].id,
                      r.symbol, g.orders[title][r.side].status))
        break
  if qty_filled > 0:
    return qty_filled, sum([a[0]*a[1] for a in prices])/qty_filled
  else: return 0, 0.0

async def limit_qty(r, title, cushion, bid_ask):
  logger = logging.getLogger(__name__)
  qty = r.qty
  qty_filled = 0
  qty_requested = qty
  prices = []
  sign = 1 if r.side == 'buy' else -1
  for i in range(20):
    request = LimitOrderRequest(
                    symbol = r.symbol,
                    qty = qty_requested,
                    side = r.side,
                    time_in_force = 'day',
                    client_order_id = stamp(title),
                    limit_price = r.limit_price
                    )
    response = await try_submit(request)
    if type(response) is int:
      if response < qty_requested:
        qty_requested = response
        continue
      else:
        logger.error('Trade in %s rejected for lack of available shares but available shares exceed request' % r.symbol)
        break
    elif response is not None:
      order = response
      limit = r.limit_price
      await asyncio.sleep(2)
      for i in range(g.EXECUTION_ATTEMPTS):
        if g.orders[title][r.side].status == 'filled': break
        else:
          latest_trade = get_latest_trade(r.symbol)
          new_limit=(
            latest_trade[r.symbol].price + sign
            * calc_cushion(i, g.EXECUTION_ATTEMPTS, bid_ask, cushion))
          new_limit = round(new_limit, 2)
          logger.info('Replacing trade in %s by new limit %s' % (r.symbol, new_limit))
          if new_limit != limit:
            updated_request = ReplaceOrderRequest(
                                 limit_price=new_limit,
                                 client_order_id = stamp(title))
            order_try = try_replace(order.id, updated_request)
            logger.info('Replace order_try:')
            logger.info(order_try)
            order = order_try if order_try is not None else order
            limit = new_limit
          await asyncio.sleep(2)
      if g.orders[title][r.side].status == 'filled':
        prices.append((qty_requested,
                      float(g.orders[title][r.side].filled_avg_price)))
        qty_filled = qty_filled + qty_requested
      else:
        logger.warning('%s order unfilled after %s attempts, proceeding with market execution' % (title, g.EXECUTION_ATTEMPTS))
        fap = float(g.orders[title][r.side].filled_avg_price) if g.orders[title][r.side].filled_avg_price is not None else 0
        prices.append(
          (int(g.orders[title][r.side].filled_qty), fap))
        try_cancel(order.id)
        qty_remaining = qty_requested - prices[-1][0]
        request = MarketOrderRequest(
                     symbol = r.symbol,
                     qty = qty_remaining,
                     side = r.side,
                     client_order_id = stamp(title),
                     time_in_force = 'day')
        market_filled = await market_qty(request, self._title)
        prices.append(market_filled)
        if market_filled[0] != qty_remaining:
          logger.error('Only %s/%s shares of market order for %s filled' % (market_filled[0], qty_remaining, r.symbol))
        qty_filled = prices[-2][0] + prices[-1][0]
      if qty_filled < qty:
        qty_requested = qty - qty_filled
        continue
      else: break
  if qty_filled > 0:
    return qty_filled, sum([a[0]*a[1] for a in prices])/qty_filled
  else: return 0, 0.0

async def try_submit(request):
  logger = logging.getLogger(__name__)
  for i in range(45):
    try:
      o = g.tclient.submit_order(request)
      return o
    except APIError as e:
      if e.status_code == 403:
        logger.error('APIError 403 when submitting a %s order for %s:' % (request.side, request.symbol))
        logger.error(e)
        error = APIError_d(e)
        if 'available' in error.keys(): return int(error['available'])
        else:
          await asyncio.sleep(2)
          continue
      else:
        logger.error('Non-403 APIError encountered during try_submit')
        logger.error(er)
        print(er)
        sys.exit(1)
  return None

def try_replace(oid, request):
  logger = logging.getLogger(__name__)
  try:
    o = g.tclient.replace_order_by_id(order_id=oid, order_data=request)
    return o
  except APIError as e:
    logger.error('There was an error when replacing order %s' % oid)
    logger.error(e)
    return None

def try_cancel(oid):
  logger = logging.getLogger(__name__)
  try:
    cancel_response = g.tclient.cancel_order_by_id(oid)
    logger.info(cancel_response)
  except APIError as e:
    logger.error('There was an error when canceling order %s' % oid)
    logger.error(e)
  return

def APIError_d(e):
  if type(e._error) is dict: return e._error
  elif type(e._error) is str: return json.loads(e._error)
  else:
    logger = logging.getLogger(__name__)
    logger.error('APIError._error is neither a dictionary nor a string')
    sys.exit(1)

# See https://github.com/python/cpython/blob/3.10/Lib/fractions.py
def min_max(fraction, max_denominator):
  p0, q0, p1, q1 = 0, 1, 1, 0
  n, d = fraction.numerator, fraction.denominator
  while True:
    a = n//d
    q2 = q0+a*q1
    if q2 > max_denominator: break
    p0, q0, p1, q1 = p1, q1, p0+a*p1, q2
    n, d = d, n-a*d
  k = (max_denominator-q0)//q1
  # Sometimes lower bound, sometimes upper bound!
  bound1 = (p0+k*p1, q0+k*q1)
  bound2 = (p1, q1)
  # Return lower bound, upper bound
  if bound1[0]/bound1[1] < bound2[0]/bound2[1]: return bound1, bound2
  else: return bound2, bound1

def compute_bid_ask(latest_quote, symbols):
  ba = []
  for s in symbols:
    ba.append(min(0,latest_quote[s.symbol].ask_price - 
                    latest_quote[s.symbol].bid_price))
  return ba[0], ba[1]

def calc_cushion(i, attempts, bid_ask, cushion):
  if bid_ask == 0:
    logger = logging.getLogger(__name__)
    logger.warning('bid_ask is 0')
    bid_ask = 0.01
  return min((i+1) * bid_ask, (i+1) * cushion)

# We don't need to worry about not having enough shares available to
# buy or sell because we will always have a position >= 0 in the hedge
# symbols.
async def hedge(n):
  logger = logging.getLogger(__name__)
  g.orders['hedge'] = {'buy': None}
  fractional_long_request = MarketOrderRequest(
                            symbol = g.HEDGE_SYMBOL,
                            notional = n,
                            side = 'buy',
                            client_order_id = stamp('hedge'),
                            time_in_force = 'day')
  await try_submit(fractional_long_request)
  await asyncio.sleep(2)
  if g.orders['hedge']['buy'].status == 'filled':
    logger.info('%s hedged notional %s' % (g.HEDGE_SYMBOL, n))
    return g.orders['hedge']['buy'].filled_avg_price
  else:
    logger.error('Market buy order %s for %s not filled with status %s' %
                 (g.orders['hedge']['buy'].id,
                  g.HEDGE_SYMBOL,
                  g.orders['hedge']['buy'].status))
    return 0

async def hedge_close(symbol, qty, closed_trades_by_hedge):
  logger = logging.getLogger(__name__)
  g.orders[symbol] = {'sell': None}
  fractional_sell_request = MarketOrderRequest(
                            symbol = symbol,
                            qty = abs(qty),
                            side = 'sell',
                            client_order_id = stamp(symbol),
                            time_in_force = 'day')
  await try_submit(fractional_sell_request)
  await asyncio.sleep(2)
  if g.orders[symbol]['sell'].status == 'filled':
    logger.info('%s hedge reduced by qty %s' % (symbol, abs(qty)))
    for t in closed_trades_by_hedge[symbol]:
      t.set_hedge_exit_price(float(g.orders[symbol]['sell'].filled_avg_price))
  else:
    logger.error('Market sell order %s for %s not filled with status %s' %
                 (g.orders[symbol]['sell'].id, symbol,
                  g.orders[symbol]['sell'].status))
  return

async def fix_position(symbol, qty):
  logger = logging.getLogger(__name__)
  side = 'buy' if qty > 0 else 'sell'
  request = MarketOrderRequest(symbol = symbol, qty = abs(qty),
                               side = side,
                               time_in_force = 'day')
  g.orders[symbol] = {side: None}
  filled_qty, filled_avg_price = await market_qty(request, symbol)
  if filled_qty == abs(qty):
    logger.info('Position in %s repaired' % symbol)
  else:
    logger.error('Market %s order %s for %s only %s/%s filled with status %s' %
                 (side, g.orders[symbol][side].id, symbol,
                  filled_qty, abs(qty),
                  g.orders[symbol][side].status))
  return

def equity(account):
  return max(float(account.equity) - g.EXCESS_CAPITAL,1)

def cash(account):
  return max(float(account.cash) - g.EXCESS_CAPITAL,0)

def account_ok():
  logger = logging.getLogger(__name__)
  account = g.tclient.get_account()
  if account.trading_blocked:
    logger.error('Trading blocked, exiting')
    return 0
  if account.account_blocked:
    logger.error('Account blocked, exiting')
    return 0
  if account.trade_suspended_by_user:
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

def stamp(s):
  return s + '_' + str(int(time.time()*1000000))

# The following two methods are the methods for buying and selling
# fractional shares. As of 9/22/22 it's not possible to short fractional
# shares on alpaca. Therefore if a long trade in one symbol results in
# you buying 7.5 shares, and another trade asks you to go short 20,
# there's no way to be short 12.5 shares. For this reason, currently we
# don't try to trade fractional shares.
async def fractional_try_close_obsolete():
  if False:
    if type(self._position[_long]['qty']) is float:
      short_cushion = stddev * g.SIGMA_CUSHION if _short else abs(stddev_x) * g.SIGMA_CUSHION
      short_limit = price[to_short] + min(bid_ask[_short],
                                          short_cushion)
      short_limit = round(short_limit, 2)
      short_request = LimitOrderRequest(
                      symbol = self._symbols[_short].symbol,
                      qty = self._position[_short]['qty'],
                      side = 'buy',
                      time_in_force = 'day',
                      client_order_id = stamp(self._title),
                      limit_price = short_limit
                      )
      g.orders[self._title] = {'buy': None, 'sell': None}
      short_order = await try_submit(short_request)
      sigma_box_short = stddev * g.SIGMA_BOX if to_short else abs(stddev_x) * g.SIGMA_BOX
      await asyncio.sleep(2)
      for i in range(g.EXECUTION_ATTEMPTS):
        if g.orders[self._title]['buy'].status != 'filled':
          latest_trade = get_latest_trade(self._symbols[_short].symbol)
          if (latest_trade[self._symbols[_short].symbol].price
              > price[_short] + sigma_box_short): break
          new_short_limit=(
            latest_trade[self._symbols[_short].symbol].price 
            + calc_cushion(i, g.EXECUTION_ATTEMPTS, bid_ask[_short],
                           short_cushion))
          new_short_limit = round(new_short_limit, 2)
          if new_short_limit != short_limit:
            updated_short_request = ReplaceOrderRequest(
                                    limit_price=new_short_limit,
                                    client_order_id=stamp(self._title))
            order_try = try_replace(short_order.id,
                                    updated_short_request)
            short_order = order_try if order_try is not None else short_order
            short_limit = new_short_limit
        elif g.orders[self._title]['buy'].status == 'filled': break
        await asyncio.sleep(2)
      if g.orders[self._title]['buy'].status == 'filled':
        self._status = 'closed'
        fractional_long_request = MarketOrderRequest(
                              symbol = self._symbols[_long].symbol,
                              qty = self._position[_long]['qty'],
                              side = 'sell',
                              client_order_id = stamp(self._title),
                              time_in_force = 'day')
        await try_submit(fractional_long_request)
        await asyncio.sleep(2)
        if g.orders[self._title]['sell'].status == 'filled':
          logger.info('%s closed' % self._title)
        else:
          logger.error('Market sell order %s for %s not filled: status %s' %
                       (g.orders[self._title]['sell'].id,
                        self._symbols[_long].symbol,
                        g.orders[self._title]['sell'].status))
        if _short:
          exit_price = (
            float(g.orders[self._title]['sell'].filled_avg_price),
            float(g.orders[self._title]['buy'].filled_avg_price))
        else:
          exit_price = (
            float(g.orders[self._title]['buy'].filled_avg_price),
            float(g.orders[self._title]['sell'].filled_avg_price))
        g.closed_trades.append(ClosedTrade(self, clock.now(),
                                           exit_price))
        self._position = [{'side':None,'qty':0,'avg_entry_price':0.0},
                          {'side':None,'qty':0,'avg_entry_price':0.0}]
        return 0
      else:
        logger.warning('%s order unfilled after %s attempts' %
                       (self._title, g.EXECUTION_ATTEMPTS))
        self._status = 'open'
        qty_covered = float(g.orders[self._title]['buy'].filled_qty)
        notional_covered = (
          qty_covered * float(g.orders[self._title]['buy'].filled_avg_price))
        try_cancel(short_order.id)
        if qty_covered == 0: return 0
        else:
           # In this case our P/L calculations will be off since we will
           # not be recording the price of the shares partially covered, 
           # but this shouldn't be a problem as this should never happen 
          self._position[_short]['qty'] = (
            self._position[_short]['qty'] - qty_covered)
          adjust_long_request = MarketOrderRequest(
                              symbol = self._symbols[_long].symbol,
                              notional = notional_covered,
                              side = 'sell',
                              client_order_id = stamp(self._title),
                              time_in_force = 'day')
        adjust_long_order = await try_submit(adjust_long_request)
        await asyncio.sleep(2)
        if g.orders[self._title]['sell'].status != 'filled':
          logger.error('Market buy order for %s not filled (status %s); position remains open and is unbalanced' %
                       (self._symbols[_long].symbol,
                        g.orders[self._title]['sell'].status))
        return 0

async def fractional_try_open_obsolete():
  if False:
    if self._symbols[to_long].fractionable:
      short_cushion = stddev * g.SIGMA_CUSHION if to_short else abs(stddev_x) * g.SIGMA_CUSHION
      short_limit = price[to_short] - min(bid_ask[to_short],
                                          short_cushion)
      short_limit = round(short_limit,2)
      short_request = LimitOrderRequest(
                      symbol = self._symbols[to_short].symbol,
                      qty = math.floor((g.trade_size/2) / short_limit),
                      side = 'sell',
                      time_in_force = 'day',
                      client_order_id = stamp(self._title),
                      limit_price = short_limit
                      )
      logger.info('Submitting short request for %s, qty=%s, limit_price=%s' % (self._symbols[to_short].symbol, math.floor((g.trade_size/2) / short_limit), short_limit))
      g.orders[self._title] = {'buy': None, 'sell': None}
      short_order = await try_submit(short_request)
      sigma_box_short = stddev * g.SIGMA_BOX if to_short else abs(stddev_x) * g.SIGMA_BOX
      await asyncio.sleep(2)
      for i in range(g.EXECUTION_ATTEMPTS):
        if g.orders[self._title]['sell'].status != 'filled':
          latest_trade = get_latest_trade(self._symbols[to_short].symbol)
          if (latest_trade[self._symbols[to_short].symbol].price
              < price[to_short] - sigma_box_short): break
          new_short_limit=(
            latest_trade[self._symbols[to_short].symbol].price 
            - calc_cushion(i, g.EXECUTION_ATTEMPTS,
                           bid_ask[to_short], short_cushion))
          new_short_limit = round(new_short_limit, 2)
          if new_short_limit != short_limit:
            updated_short_request = ReplaceOrderRequest(
                                  limit_price=new_short_limit,
                                  client_order_id = stamp(self._title))
            logger.info('Replacing short request for %s with limit price %s' % (self._symbols[to_short].symbol, new_short_limit))
            order_try = try_replace(short_order.id,
                                    updated_short_request)
            short_order = order_try if order_try is not None else short_order
            short_limit = new_short_limit
        elif g.orders[self._title]['sell'].status == 'filled': break
        await asyncio.sleep(2)
      if g.orders[self._title]['sell'].status == 'filled':
        fractional_long_notional = int(g.orders[self._title]['sell'].filled_qty) * float(g.orders[self._title]['sell'].filled_avg_price)
        fractional_long_request = MarketOrderRequest(
                              symbol = self._symbols[to_long].symbol,
                              notional = fractional_long_notional,
                              side = 'buy',
                              client_order_id = stamp(self._title),
                              time_in_force = 'day')
        await try_submit(fractional_long_request)
        logger.info('Submitting fractional long request for %s, notional=%s' % (self._symbols[to_long].symbol, fractional_long_notional))
        await asyncio.sleep(2)
        if g.orders[self._title]['buy'].status == 'filled':
          self._position[to_short]={'side':'short',
            'qty':int(g.orders[self._title]['sell'].filled_qty),
            'avg_entry_price':g.orders[self._title]['sell'].filled_avg_price}
          self._position[to_long]={'side':'long',
            'qty':float(g.orders[self._title]['buy'].filled_qty),
            'avg_entry_price':float(g.orders[self._title]['buy'].filled_avg_price)}
          self._hedge_position = {'symbol':g.HEDGE_SYMBOL, 
                                  'side':'long','notional':0.0,
                                  'qty':0,'avg_entry_price':0.0}
          self._status = 'open'
          self._opened = clock.now()
          return 0
        else:
          self._status = 'closed'
          logger.error('Market buy order %s for %s not filled: status %s' %
                       (g.orders[self._title]['buy'].id,
                        self._symbols[to_short].symbol,
                        g.orders[self._title]['buy'].status))
        return 0
      else:
        logger.warning('%s order unfilled after %s attempts' %
                       (self._title, g.EXECUTION_ATTEMPTS))
        self._status = 'closed'
        short_qty_filled = int(g.orders[self._title]['sell'].filled_qty)
        try_cancel(short_order.id)
        if short_qty_filled == 0: return 0
        cover_short_request = MarketOrderRequest(
                              symbol = self._symbols[to_short].symbol,
                              qty = short_qty_filled,
                              side = 'buy',
                              client_order_id = stamp(self._title),
                              time_in_force = 'day')
        cover_short_order = await try_submit(cover_short_request)
        await asyncio.sleep(2)
        if g.orders[self._title]['buy'].status != 'filled':
          logger.error('Market buy order %s for %s not filled: status %s' %
                       (g.orders[self._title]['buy'].id,
                        self._symbols[to_short].symbol,
                        g.orders[self._title]['buy'].status))
        return 0