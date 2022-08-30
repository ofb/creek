import os
import logging
import logging.handlers
import asyncio
from alpaca.data.live import StockDataStream
from . import trade
from . import io
from . import signal
from . import key
from . import secret_key

logging.basicConfig(
  level=logging.INFO,
  format="%(asctime)s:%(levelname)s:%(name)s:%(message)s",
  handlers=[logging.handlers.WatchedFileHandler(os.environ.get("LOGFILE", "creek.log"))]
)
# Load trade objects including open trade objects
t = trade.Trade('AVB','AIRC', 0.9, 0.94, (1,1), (1,1), (1,1), (1,1))
active_symbols, trades = io.load_trades()
closed_trades = []
bars = {}
for symbol in active_symbols:
  bars[symbol] = []
clock = signal.Clock()
while not clock.is_open: clock.rest()
'''
We run two concurrent asyncronous coroutines:
1) get minute bars at two seconds past the minute, check for signals at
five seconds past the minute and initiate trades
2) monitor account trade status and update trades as necessary
'''
wss_client = StockDataStream(key, secret_key)
wss_client.subscribe_bars(io.bar_data_handler, *active_symbols)
wss_client.run()
async def main():
  await asyncio.gather(wss_client.run(), signal.main(), 
                       trade.monitor())
asyncio.run(main())
io.archive(closed_trades)
io.report(trades, closed_trades)
io.save(trades)

'''
To-do/notes
- need to pass bars as a list to every method that needs them
- make closed_trade class inherit trade class
'''