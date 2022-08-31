import logging
import asyncio
from  datetime import datetime as dt
import time
import pytz as tz
import pandas as pd
import alpaca.trading.client as ac
from . import trade
from . import signal
from . import g

class Clock():
  '''
  Uses machine time if it's wihin one second of market time, otherwise
  uses market time via alpaca api.
  _local(bool): whether to use the local clock or query the API.
                If the two times are within a second of each other, use
                the machine time.
  '''
  def __init__(self):
    self._client = ac.TradingClient(g.key, g.secret_key)
    ac_clock = self._client.get_clock()
    delta = dt.now(tz=tz.timezone('US/Eastern')) - ac_clock.timestamp
    if abs(delta.total_seconds()) < 1: self._local = True
    else: self._local = False
    self.is_open = ac_clock.is_open
    self.next_open = ac_clock.next_open
    self.next_close = ac_clock.next_close

  def refresh(self):
    ac_clock = self._client.get_clock()
    self.is_open = ac_clock.is_open
    self.next_open = ac_clock.next_open
    self.next_close = ac_clock.next_close

  def now(self):
    if self._local: return dt.now(tz=tz.timezone('US/Eastern'))
    else: return self._client.get_clock().timestamp

  def rest(self):
    logger = logging.getLogger(__name__)
    delta = self.next_open - self.now()
    if delta.seconds > 0:
      s = self.next_open.strftime("%m/%d/%Y %H:%M")
      logger.info('Market next open at %s; sleeping for %s' % (s,delta))
      time.sleep(delta.seconds)
    self.refresh()

def remove_concentration(to_open):
  return to_open

def available_trades():
  return 1000

async def main(clock):
  logger = logging.getLogger(__name__)
  logger.info('Entering main')
  start = time.time()
  now = clock.now()
  to_close = []
  to_open = {}
  for key, trade in g.trades:
    if trade.status() == 'open':
      if trade.close_signal(bars): to_close.append(key)
    elif trade.status() == 'closed':
      o, l, s = trade.open_signal(bars)
      if o: to_open[key] = [trade.pearson(), l, s]
  to_open_df = pd.DataFrame.from_dict(to_open, orient='index',
                                     columns=['pearson','long','short'])
  to_open_df.sort_values(by='pearson', ascending=False, inplace=True)
  remove_concentration(to_open_df)
  n = available_trades()
  await asyncio.gather(*(g.trades[k].close() for k in to_close),
    *(g.trades[k].try_open() for k in to_open_s[:n].index))
    
  
  now = clock.now()
  if ((time.time() - start) > 60) and ((clock.next_close
      - now) >= dt.timedelta(seconds=59)): return
  elif ((clock.next_close - now)
        >= dt.timedelta(seconds=58)):
    if now.second==0: delta = 1-now.microsecond/1000000
    elif now.second<=2: return
    else: delta = 61-now.second-now.microsecond/1000000
    time.sleep(delta)
    return