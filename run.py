import asyncio
from . import trade
from . import signal
from . import io
'''
We run three concurrent asyncronous coroutines:
1) get minute bars at two seconds past the minute
2) check for signals at five seconds past the minute and initiate trades
3) monitor account trade status and update trades as necessary
''' 
async def main(symbols, trades, closed_trades):
  await asyncio.gather(io.get_bars(symbols), signal.main(trades), 
                       trade.monitor(trades, closed_trades))