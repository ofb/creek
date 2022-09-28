import logging
import asyncio
from  datetime import datetime as dt
from datetime import timedelta as td
import math
import time
import pytz as tz
import pandas as pd
from alpaca.data.requests import StockLatestTradeRequest
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

def cancel_all():
  logger = logging.getLogger(__name__)
  cancel_response = g.tclient.cancel_orders()
  if len(cancel_response) > 0:
    logger.info('There were canceled orders with the following HTTP statuses')
    for s in cancel_response:
      logger.info(s)

def num(s):
  try:
    q = int(s)
  except ValueError as e:
    q = float(s)
  return q

'''
Compare open positions to actual positions and return the difference
'''
async def resolve_positions():
  logger = logging.getLogger(__name__)
  expected_positions = {}
  excess_positions = {}
  for key, t in g.trades.items():
    if t.status() == 'open':
      # side, qty, avg_entry_price
      for s, p in t.get_position().items():
        if p['qty'] < 0: logger.error('%s has position in %s with negative quantity' % (self._title, s))
        if s in expected_positions.keys():
          expected_positions[s] = expected_positions[s] + p['qty'] if p['side'] == 'long' else expected_positions[s] - p['qty']
        else: expected_positions[s] = p['qty'] if p['side'] == 'long' else - p['qty']
  g.positions = g.tclient.get_all_positions() # List[Position]
  for p in g.positions:
    # p.qty is already signed
    qty = num(p.qty)
    if p.symbol not in expected_positions.keys():
      logger.warning('There is an unknown position in %s' % p.symbol)
      excess_positions[p.symbol] = qty
    else:
      if abs(expected_positions[p.symbol] - qty) > 0.1:
        logger.warning('Expected position in %s = %s; actual position = %s' % (p.symbol, expected_positions[p.symbol], qty))
        excess_positions[p.symbol] = qty - expected_positions[p.symbol]
  
  await asyncio.gather(*(trade.fix_position(s, -q)
                         for s, q in excess_positions.items()))
  logger.info('Positions resolved')
  return

'''
Most trades will barely exceed the threshold, and those should be sorted
by their pearson coefficients. Some, however, will materially exceed the 
threshold, and those should be sorted by their deviation.
'''
def sort_trades(to_open):
  to_open_outliers = to_open[to_open['dev'] > 1.1 * g.TO_OPEN_SIGNAL]
  to_open_bulk = to_open[to_open['dev'] <= 1.1 * g.TO_OPEN_SIGNAL]
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
      frac_position = sign * float(position.cost_basis) / g.equity
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
      mask = ((to_open['long']!=key) |
              (to_open.groupby('long').cumcount() <= l - (net - longs)))
      to_open = to_open[mask]
    elif net < -shorts:
      # diff = -(net - (-shorts)) = -net - shorts
      mask = ((to_open['short']!=key) |
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
  logger.info('Entering main')
  start = time.time()
  to_close = []
  to_bail_out = []
  to_open = {}
  for key, t in g.trades.items():
    t.append_bar()
    if t.status() == 'open':
      if t.bail_out_signal(clock): to_bail_out.append(key)
      elif t.close_signal(clock): to_close.append(key)
    elif t.status() == 'closed':
      o, d, l, s = t.open_signal(clock)
      if o: to_open[key] = [abs(t.pearson()), d, l, s]
  to_open_df = pd.DataFrame.from_dict(to_open, orient='index',
               columns=['pearson','dev','long','short'])
  to_open_df = sort_trades(to_open_df)
  to_open_df = remove_concentration(to_open_df)
  n = available_trades()
  symbols = list(set(to_open_df['long'][:n].to_list() +
                 to_open_df['short'][:n].to_list() + to_close))
  quote_request = StockLatestQuoteRequest(symbol_or_symbols=symbols)
  trade_request = StockLatestTradeRequest(symbol_or_symbols=symbols)
  latest_quote = g.hclient.get_stock_latest_quote(quote_request)
  latest_trade = g.hclient.get_stock_latest_trade(trade_request)
  hedge = await asyncio.gather(
    *(g.trades[k].bail_out(clock) for k in to_bail_out),
    *(g.trades[k].try_close(clock, latest_quote, latest_trade)
      for k in to_close),
    *(g.trades[k].try_open(clock, latest_quote, latest_trade)
      for k in to_open_df[:n].index))
  # Need to buy a fraction of an index for the remainder.
  # Remember to add this index to active_symbols manually.
  hedge_notional = 0.0 # To go long (in dollars)
  hedge_d = {} # To cover (negative fractional quantity)
  closed_trades_by_hedge = {} # Indexed by hedging symbol
  for h in hedge:
    if h == 0: continue
    if type(h) is float:
       if h < 0: logger.error('Negative hedge passed without symbol')
       else: hedge_notional = hedge_notional + h
    if type(h) is tuple:
      if h[1] > 0:
        logger.error('Positive hedge passed with symbol')
        continue
      elif h[0] in hedge_d.keys(): hedge_d[h[0]] = hedge_d[h[0]] + h[1]
      else: hedge_d[h[0]] = h[1]
      if h[0] in closed_trades_by_hedge.keys():
        closed_trades_by_hedge[h[0]].append(h[2])
      else: closed_trades_by_hedge[h[0]] = [h[2]]
  logger.info('Trying to close the following hedges:')
  logger.info(hedge_d)
  hedged = await asyncio.gather(trade.hedge(hedge_notional),
    *(trade.hedge_close(symbol, qty, closed_trades_by_hedge)
      for symbol, qty in hedge_d.items()))
  hedged_price = 0.0
  for h in hedged:
    if type(h) is float:
      hedged_price = h
      break
  for k in to_open_df[:n].index:
    if g.trades[k].status() == 'open':
      g.trades[k].fill_hedge(hedged_price)
  # Give a moment for positions to update from the recent trades
  time.sleep(2)
  g.retarget['missed'].append(max(0,len(to_open_df) - n))
  account = g.tclient.get_account()
  g.equity = trade.equity(account)
  g.cash = trade.cash(account)
  g.retarget['util'].append(1 - g.cash / g.equity)
  retarget(clock)
  cancel_all()
  await resolve_positions()
  logger.info('signal.main() finished after %s seconds' % (time.time() - start))
  if time.time() - start < 2: time.sleep(2)
  now = clock.now()
  if (time.time() - start) > 60: return
  elif ((clock.next_close - now) >= td(minutes=1, seconds=58)):
    if now.second==0: delta = 1-now.microsecond/1000000
    elif now.second<=2: return
    else: delta = 61-now.second-now.microsecond/1000000
    logger.info('Sleeping for %s seconds' % delta)
    time.sleep(delta)
    return
  return