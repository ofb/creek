import logging
import pandas as pd
import asyncio
import glob
from creek import root
from . import trade

async def get_bars():
  pass

'''
Saving/restoring trade objects
'''
def read_trade(path):
  open_trade_df = pd.read_csv(open_trade, index_col=0)
  open_trade_df.set_index('index', inplace=True)
  open_trade = open_trade_df.to_dict()[0]
  t = trade.Trade(open_trade['symbol1'], 
                  open_trade['symbol2'], open_trade['pearson'],
                  open_trade['pearson_historical'])
  t.initialize(open_trade)
  return t

def load_trades():
  logger = logging.getLogger(__name__)
  active_symbols = []
  trades = {}
  pearson = pd.read_csv('%s/pearson.csv' % root, index_col=0)
  open_trade_list = glob.glob('%s/open_trades/*.csv' % root)
  logger.info('Loading open trades')
  for open_trade in open_trade_list:
    title = open_trade.split('/')[-1]
    title = title.split('.')[0]
    symbols = title.split('-')
    active_symbols.extend(symbols)
    trades[title] = trade.read_trade(open_trade)
  logger.info('Loading remaining tensorflow models')
  for index, row in pearson.iterrows():
    title = row['symbol1'] + '-' + row['symbol2']
    if title not in trades.keys():
      active_symbols.extend([row['symbol1'],row['symbol2']])
      trades[title] = trade.Trade(row['symbol1'], row['symbol2'], 
                      float(row['pearson']), 
                      float(row['pearson_historical']))
  return set(active_symbols), trades

def archive(closed_trades):
  pass

def report(trades, closed_trades):
  pass

def save(trades):
  pass