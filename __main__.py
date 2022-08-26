import os
import glob
import logging
import logging.handlers
import asyncio
import datetime
import pandas as pd
import alpaca.trading.client as alpacaClient
from . import trade
from . import key
from . import secret_key

logging.basicConfig(
  level=logging.INFO,
  format="%(asctime)s:%(levelname)s:%(name)s:%(message)s",
  handlers=[logging.handlers.WatchedFileHandler(os.environ.get("LOGFILE", "creek.log"))]
)
# Load trade objects
# Also need to load open trade objects that may not be in pearson.csv
pearson  = pd.read_csv('%s/pearson.csv' % root)
active_sumbols = []
trades = {}
open_trade_list = glob.glob('root/open_trades/*.csv' % root)
for open_trade in open_trade_list:
  open_trade_df = pd.read_csv(open_trade)
  trades[title]
for index, row in pearson.iterrows():
  active_symbols.extend([row['symbol1'],row['symbol2']])
  title = row['symbol1'] + '-' + row['symbol2']
  trades[title] = trade.Trade(row['symbol1'], row['symbol2'], 
                  row['pearson'], row['pearson_historical'])





client = alpacaClient.TradingClient(key, secret_key)
clock = client.get_clock()