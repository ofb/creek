import logging
import asyncio
from  datetime import datetime as dt
import time
import pytz as tz
import alpaca.trading.client as ac
from . import trade
from . import signal
from creek import key
from creek import secret_key

class Clock():
  '''
  Uses machine time if it's wihin one second of market time, otherwise
  uses market time via alpaca api.
  _local(bool): whether to use the local clock or query the API.
                If the two times are within a second of each other, use
                the machine time.
  '''
  def __init__(self):
    self.client = ac.TradingClient(key, secret_key)
    ac_clock = self.client.get_clock()
    delta = dt.now(tz=tz.timezone('US/Eastern')) - ac_clock.timestamp
    if abs(delta.total_seconds()) < 1: self._local = True
    else: self._local = False
    self.is_open = ac_clock.is_open
    self.next_open = ac_clock.next_open
    self.next_close = ac_clock.next_close

  def refresh(self):
    ac_clock = self.client.get_clock()
    self.is_open = ac_clock.is_open
    self.next_open = ac_clock.next_open
    self.next_close = ac_clock.next_close

  def now(self):
    if self._local: return dt.now(tz=tz.timezone('US/Eastern'))
    else: return self.client.get_clock().timestamp

  def rest(self):
    logger = logging.getLogger(__name__)
    delta = self.next_open - self.now()
    if delta.seconds > 0:
      s = self.next_open.strftime("%m/%d/%Y %H:%M")
      logger.info('Market next open at %s; sleeping for %s' % (s,delta))
      time.sleep(delta.seconds)
    self.refresh()

async def main(trades, clock):
  pass