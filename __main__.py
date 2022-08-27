import os
import logging
import logging.handlers
import asyncio
from . import trade
from . import io
from . import signal
from . import run
from . import key
from . import secret_key

logging.basicConfig(
  level=logging.INFO,
  format="%(asctime)s:%(levelname)s:%(name)s:%(message)s",
  handlers=[logging.handlers.WatchedFileHandler(os.environ.get("LOGFILE", "creek.log"))]
)
# Load trade objects including open trade objects
t = trade.Trade('AVB','AIRC','0.9','0.94')
active_symbols, trades = io.load_trades()
closed_trades = []
clock = signal.Clock()
while not clock.is_open: clock.rest()
asyncio.run(run.main(active_symbols, trades, closed_trades, clock))
io.archive(closed_trades)
io.report(trades, closed_trades)
io.save(trades)

'''
To-do/notes
- need to pass bars as a list to every method that needs them
- make closed_trade class inherit trade class
'''