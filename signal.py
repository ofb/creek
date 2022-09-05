import logging
import asyncio
from  datetime import datetime as dt
from datetime import timedelta as td
import math
import time
import pytz as tz
import pandas as pd
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestBarRequest
from alpaca.data.requests import StockLatestQuoteRequest
from . import trade
from . import signal
from . import config as g

class Clock():
  '''
  Uses machine time if it's wihin one second of market time, otherwise
  uses market time via alpaca api.
  _local(bool): whether to use the local clock or query the API.
                If the two times are within a second of each other, use
                the machine time.
  '''
  def __init__(self):
    ac_clock = g.tclient.get_clock()
    delta = dt.now(tz=tz.timezone('US/Eastern')) - ac_clock.timestamp
    if abs(delta.total_seconds()) < 1: self._local = True
    else: self._local = False
    logger = logging.getLogger(__name__)
    if self._local: logger.info('Using local clock')
    else: logger.info('Using Alpaca clock')
    self.is_open = ac_clock.is_open
    self.next_open = ac_clock.next_open
    self.next_close = ac_clock.next_close

  def refresh(self):
    ac_clock = g.tclient.get_clock()
    self.is_open = ac_clock.is_open
    self.next_open = ac_clock.next_open
    self.next_close = ac_clock.next_close

  def now(self):
    if self._local: return dt.now(tz=tz.timezone('US/Eastern'))
    else: return g.tclient.get_clock().timestamp

  def rest(self):
    logger = logging.getLogger(__name__)
    delta = self.next_open - self.now()
    if delta.seconds > 0:
      s = self.next_open.strftime("%m/%d/%Y %H:%M")
      logger.info('Market next open at %s; sleeping for %s' % (s,delta))
      time.sleep(delta.seconds)
    self.refresh()

'''
Most trades will barely exceed the threshold, and those should be sorted
by their pearson coefficients. Some, however, will materially exceed the 
threshold, and those should be sorted by their deviation.
'''
def sort_trades(to_open):
  to_open_outliers = to_open[to_open_df['dev'] > 1.1 * g.TO_OPEN_SIGNAL]
  to_open_bulk = to_open[to_open_df['dev'] <= 1.1 * g.TO_OPEN_SIGNAL]
  to_open_outliers.sort_values(by='dev',ascending=False,inplace=True)
  to_open_bulk.sort_values(by='pearson',ascending=False,inplace=True)
  return pd.concat([to_open_outliers,to_open_bulk])

'''
We want to make sure we don't become overconcentrated in one symbol
across several trades. The global parameter MAX_SYMBOL determines the
maximum allowed exposure to any one symbol as a percentage of total
equity. We go by cost basis rather than market value because if we have
a pair that undergoes a large tandem price movement, the cost basis is
a better measure of exposure.
'''
def remove_concentration(to_open):
  logger = logging.getLogger(__name__)
  positions_d = {}
  for p in g.positions:
    if p.symbol in positions_d.keys():
      logger.error('Multiple positions in the same symbol.')
    positions_d[p.symbol] = p
  for key in g.active_symbols.keys():
    if key in positions_d.keys():
      position = positions_d[key]
      sign = 1 if position.side == 'long' else -1
      frac_position = sign * position.cost_basis / g.equity
    else: frac_position = 0
    # frac_position + longs * g.MAX_TRADE_SIZE / 2 <= g.MAX_SYMBOL
    longs = math.floor((g.MAX_SYMBOL
                             -frac_position)*2/g.MAX_TRADE_SIZE)
    # frac_position - shorts * g.MAX_TRADE_SIZE / 2 >= -g.MAX_SYMBOL
    shorts = math.floor((g.MAX_SYMBOL
                              +frac_position)*2/g.MAX_TRADE_SIZE)
    l = to_open['long'].to_list().count(key)
    s = to_open['short'].to_list().count(key)
    net = l-s
    if net > longs:
      # diff = net - longs
      mask = ((df['long']!=key) |
              (to_open.groupby('long').cumcount() <= l - (net - longs)))
      to_open = to_open[mask]
    elif net < -shorts:
      # diff = -(net - (-shorts)) = -net - shorts
      mask = ((df['short']!=key) |
              (to_open.groupby('short').cumcount() <= s -(-net-shorts)))
      to_open = to_open[mask]
  return to_open

'''
The following method determines the number of trades that can be opened
by dividing free capital by the max position size, which is defined as
total capital times the global parameter MAX_TRADE_SIZE and is set in
trade.set_trade_size() at the beginning of the day.
'''
def available_trades(): return math.floor(g.cash / g.trade_size)

'''
The following method compares utilization and missed trades to determine
whether the sigma threshold (global parameter TO_OPEN_SIGNAL) should be 
raised or lowered.
- A missed trade is a trade where the signal fired but which wasn't 
opened due to lack of available free capital.
- Utilization is measured as a percentage of free capital.
'''
def retarget(clock):
  logger = logging.getLogger(__name__)
  now = clock.now()
  if (30 <= now.minute < 40) and len(g.retarget['missed']) > 15 and len(g.retarget['util']) > 15:
    util = sum(g.retarget['util'])/len(g.retarget['util'])
    missed = sum(g.retarget['missed'])
    if util + missed * g.MAX_TRADE_SIZE < 0.95:
      logger.info('Last hour util: %s. Last hour missed trades: %s. Lowering TO_OPEN_SIGNAL from %s to %s' % (util, missed, g.TO_OPEN_SIGNAL, g.TO_OPEN_SIGNAL - 0.1))
      g.TO_OPEN_SIGNAL = g.TO_OPEN_SIGNAL - 0.1
      g.retarget['missed'].clear()
      g.retarget['util'].clear()
    elif util + missed * g.MAX_TRADE_SIZE > 1.05:
      logger.info('Last hour util: %s. Last hour missed trades: %s. Raising TO_OPEN_SIGNAL from %s to %s' % (util, missed, g.TO_OPEN_SIGNAL, g.TO_OPEN_SIGNAL + 0.1))
      g.TO_OPEN_SIGNAL = g.TO_OPEN_SIGNAL + 0.1
      g.retarget['missed'].clear()
      g.retarget['util'].clear()
  return

async def main(clock):
  logger = logging.getLogger(__name__)
  logger.info('Entering main; len(AIRC) = %s' % len(g.bars['AIRC']))
  start = time.time()
  to_close = []
  to_open = {}
  for key, trade in g.trades.items():
    trade.append_bar()
    if trade.status() == 'open':
      if trade.close_signal(clock): to_close.append(key)
    elif trade.status() == 'closed':
      o, d, l, s = trade.open_signal(clock)
      if o: to_open[key] = [abs(trade.pearson()), d, l, s]
  to_open_df = pd.DataFrame.from_dict(to_open, orient='index',
               columns=['pearson','dev','long','short'])
  to_open_df = sort_trades(to_open_df)
  to_open_df = remove_concentration(to_open_df)
  n = available_trades()
  symbols = list(set(to_open_df['long'][:n].to_list +
                to_open_df['short'][:n].to_list + to_close))
  quote_request = StockLatestQuoteRequest(symbol_or_symbols=symbols)
  bar_request = StockLatestBarRequest(symbol_or_symbols=symbols)
  latest_quote = g.client.get_stock_latest_quote(quote_request)
  latest_bar = g.client.get_stock_latest_bar(bar_request)
  await asyncio.gather(
    *(g.trades[k].try_close(latest_bar) for k in to_close),
    *(g.trades[k].try_open(latest_quote, latest_bar)
      for k in to_open_df[:n].index))
  
  # Give a moment for positions to update from the recent trades
  if time.time() - start < 55: time.sleep(2)
  g.retarget['missed'].append(max(0,len(to_open_df) - n))
  account = g.tclient.get_account()
  g.equity = trade.equity(account)
  g.cash = trade.cash(account)
  g.retarget['util'].append(1 - g.cash / g.equity)
  retarget(clock)
  # Get positions after trades have settled
  g.positions = g.tclient.get_all_positions() # List[Position]
  for p in g.positions:
    if p.symbol not in g.active_symbols.keys():
    logger.warning('There is an unknown position in %s' % p.symbol)

  
  if time.time() - start < 2: time.sleep(2)
  now = clock.now()
  if ((time.time() - start) > 60) and ((clock.next_close
      - now) >= td(seconds=59)): return
  elif ((clock.next_close - now) >= td(seconds=58)):
    if now.second==0: delta = 1-now.microsecond/1000000
    elif now.second<=2: return
    else: delta = 61-now.second-now.microsecond/1000000
    logger.info('Sleeping for %s seconds' % delta)
    time.sleep(delta)
    return