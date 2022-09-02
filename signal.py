import logging
import asyncio
from  datetime import datetime as dt
from datetime import timedelta as td
import math
import time
import pytz as tz
import pandas as pd
import alpaca.trading.client as ac
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
We want to make sure we don't become overconcentrated in one symbol
across several trades. The global parameter MAX_SYMBOL determines the
maximum allowed exposure to any one symbol as a percentage of total
equity.
'''
def remove_concentration(to_open):
  return to_open

'''
The following method determines the number of trades that can be opened
by dividing free capital by the max position size, which is defined as
total capital times the global parameter MAX_TRADE_SIZE and is set in
trade.set_trade_size() at the beginning of the day.
'''
def available_trades():
  cash = g.tclient.get_account().cash
  return math.floor(cash / g.trade_size)

'''
The following method compares utilization and missed trades to determine
whether the sigma threshold (global parameter TO_OPEN_SIGNAL) should be 
raised or lowered.
- A missed trade is a trade where the signal fired but which wasn't 
opened due to lack of available free capital.
- Utilization is measured as a percentage of free capital.
'''
def retarget():
  pass

async def main(clock):
  logger = logging.getLogger(__name__)
  logger.info('Entering main; len(AIRC) = %s' % len(g.bars['AIRC']))
  start = time.time()
  to_close = []
  to_open = {}
  for key, trade in g.trades.items():
    if trade.status() == 'open':
      if trade.close_signal(bars): to_close.append(key)
    elif trade.status() == 'closed':
      o, l, s = trade.open_signal()
      if o: to_open[key] = [trade.pearson(), l, s]
  to_open_df = pd.DataFrame.from_dict(to_open, orient='index',
                                     columns=['pearson','long','short'])
  to_open_df.sort_values(by='pearson', ascending=False, inplace=True)
  remove_concentration(to_open_df)
  n = available_trades()
  await asyncio.gather(*(g.trades[k].try_close() for k in to_close),
    *(g.trades[k].try_open() for k in to_open_df[:n].index))

  retarget()
  
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