import logging
import pandas as pd
import asyncio
import glob
from datetime import datetime as dt
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass
from creek import root
from creek import key
from creek import secret_key
import __main__
from . import trade
from . import signal

def get_assets():
  trading_client = TradingClient(
    'PK52PMRHMCY15OZGMZLW','F8270IxVZS3hXdghv7ChIyQUalFRIZZxYYqMKfUh')
  search_params = GetAssetsRequest(asset_class=AssetClass.US_EQUITY)
  assets = trading_client.get_all_assets(search_params)
  assets_dict = {}
  for a in assets:
    assets_dict[a.symbol] = a
  return assets_dict
'''
Saving/restoring trade objects
'''
def read_trade(path):
  open_trade_df = pd.read_csv(open_trade, index_col=0)
  open_trade_df.set_index('index', inplace=True)
  open_trade = open_trade_df.to_dict()[0]
  tradable = (bool(eval(open_trade['symbol1_tradable'])),
              bool(eval(open_trade['symbol2_tradable'])))
  marginable = (bool(eval(open_trade['symbol1_marginable'])),
                bool(eval(open_trade['symbol2_marginable'])))
  shortable = (bool(eval(open_trade['symbol1_shortable'])),
               bool(eval(open_trade['symbol2_shortable'])))
  fractionable = (bool(eval(open_trade['symbol1_fractionable'])),
                  bool(eval(open_trade['symbol2_fractionable'])))

  t = trade.Trade(open_trade['symbol1'], 
                  open_trade['symbol2'], float(open_trade['pearson']),
                  float(open_trade['pearson_historical']),
                  tradable, marginable, shortable, fractionable)
  t.open_init(open_trade)
  return t

def load_trades():
  logger = logging.getLogger(__name__)
  active_symbols = []
  trades = {}
  assets = get_assets()
  pearson = pd.read_csv('%s/pearson.csv' % root, index_col=0)
  open_trade_list = glob.glob('%s/open_trades/*.csv' % root)
  logger.info('Loading open trades')
  for open_trade in open_trade_list:
    title = open_trade.split('/')[-1]
    title = title.split('.')[0]
    symbols = title.split('-')
    active_symbols.extend(symbols)
    trades[title] = read_trade(open_trade)
    trades[title].tradable = (assets[symbols[0]].tradable,
                              assets[symbols[1]].tradable)
    trades[title].marginable = (assets[symbols[0]].marginable,
                                assets[symbols[1]].marginable)
    trades[title].shortable = (assets[symbols[0]].shortable,
                               assets[symbols[1]].shortable)
    trades[title].fractionable = (assets[symbols[0]].fractionable,
                                  assets[symbols[1]].fractionable)
  logger.info('Loading remaining tensorflow models')
  for index, row in pearson.iterrows():
    title = row['symbol1'] + '-' + row['symbol2']
    if title in trades.keys():
      trades[title].pearson = float(row['pearson'])
      trades[title].pearson_historical= float(row['pearson_historical'])
    if title not in trades.keys():
      active_symbols.extend([row['symbol1'],row['symbol2']])
      tradable = (assets[row['symbol1']].tradable,
                  assets[row['symbol2']].tradable)
      marginable = (assets[row['symbol1']].marginable,
                    assets[row['symbol2']].marginable)
      shortable = (assets[row['symbol1']].shortable,
                   assets[row['symbol2']].shortable)
      fractionable = (assets[row['symbol1']].fractionable,
                      assets[row['symbol2']].fractionable)
      trades[title] = trade.Trade(row['symbol1'], row['symbol2'], 
        float(row['pearson']), float(row['pearson_historical']),
        tradable, marginable, shortable, fractionable)
  return set(active_symbols), trades

async def bar_data_handler(bar):
  logger = logging.getLogger(__name__)
  __main__.bars[bar.symbol].append(bar)
  logger.info('%s received' % bar.symbol)

def archive(closed_trades):
  logger = logging.getLogger(__name__)
  logger.info('Archiving closed trades')
  pass

def report(trades, closed_trades):
  logger = logging.getLogger(__name__)
  logger.info('Generating today\'s report')
  pass

def save(trades):
  logger = logging.getLogger(__name__)
  logger.info('Saving open trades')
  pass