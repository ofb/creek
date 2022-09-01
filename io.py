import logging
import pandas as pd
import asyncio
import glob
from datetime import datetime as dt
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass
from alpaca.data.live import StockDataStream
from alpaca.trading.stream import TradingStream
from alpaca.trading.models import Asset
from . import config as g
from . import trade
from . import signal

def get_assets():
  trading_client = TradingClient(g.key, g.secret_key)
  search_params = GetAssetsRequest(asset_class=AssetClass.US_EQUITY)
  assets = trading_client.get_all_assets(search_params)
  assets_dict = {}
  for a in assets:
    assets_dict[a.symbol] = a
  return assets_dict
'''
Saving/restoring trade objects
'''
def construct_symbol(id_,class_,exchange,symbol,status,tradable,
                marginable,shortable,etb,fractionable):
  d = {
    'id': id_,
    'class': class_,
    'exchange': exchange,
    'symbol': symbol,
    'status': status,
    'tradable': bool(eval(tradable)),
    'marginable': bool(eval(marginable)),
    'shortable': bool(eval(shortable)),
    'easy_to_borrow': bool(eval(etb)),
    'fractionable': bool(eval(fractionable)),
  }
  return Asset(**d)

def read_trade(path):
  open_trade_df = pd.read_csv(open_trade, index_col=0)
  open_trade_df.set_index('index', inplace=True)
  open_trade = open_trade_df.to_dict()[0]
  symbol1 = construct_symbol(open_trade['symbol1_id'],
                             open_trade['symbol1_class'],
                             open_trade['symbol1_exchange'],
                             open_trade['symbol1_symbol'],
                             open_trade['symbol1_status'],
                             open_trade['symbol1_tradable'],
                             open_trade['symbol1_marginable'],
                             open_trade['symbol1_shortable'],
                             open_trade['symbol1_etb'],
                             open_trade['symbol1_fractionable'])
  symbol2 = construct_symbol(open_trade['symbol2_id'],
                             open_trade['symbol2_class'],
                             open_trade['symbol2_exchange'],
                             open_trade['symbol2_symbol'],
                             open_trade['symbol2_status'],
                             open_trade['symbol2_tradable'],
                             open_trade['symbol2_marginable'],
                             open_trade['symbol2_shortable'],
                             open_trade['symbol2_etb'],
                             open_trade['symbol2_fractionable'])
  t = trade.Trade([symbol1, symbol2], float(open_trade['pearson']),
                  float(open_trade['pearson_historical']))
  t.open_init(open_trade)
  return t

def load_trades():
  logger = logging.getLogger(__name__)
  symbol_list = []
  trades = {}
  assets = get_assets()
  pearson = pd.read_csv('%s/pearson.csv' % g.root, index_col=0)
  open_trade_list = glob.glob('%s/open_trades/*.csv' % g.root)
  logger.info('Loading open trades')
  for open_trade in open_trade_list:
    title = open_trade.split('/')[-1]
    title = title.split('.')[0]
    symbols = title.split('-')
    symbol_list.extend(symbols)
    trades[title] = read_trade(open_trade)
    if not trades[title].refresh_open([assets[symbols[0]],
                                       assets[symbols[1]]]):
      logger.error('%s is open but no longer tradable' % title)
  logger.info('Loading remaining tensorflow models')
  for index, row in pearson.iterrows():
    title = row['symbol1'] + '-' + row['symbol2']
    if title in trades.keys():
      trades[title].pearson = float(row['pearson'])
      trades[title].pearson_historical= float(row['pearson_historical'])
    if title not in trades.keys():
      symbol_list.extend([row['symbol1'],row['symbol2']])
      trades[title] = trade.Trade([assets[row['symbol1']], 
                                  assets[row['symbol2']]],
                                  float(row['pearson']), 
                                  float(row['pearson_historical']))
  asset_dict = {}
  for symbol in set(symbol_list):
    asset_dict[symbol] = assets[symbol]
  return asset_dict, trades

def stock_wss():
  wss_client = StockDataStream(g.key, g.secret_key)
  wss_client.subscribe_bars(bar_data_handler, 
                            *g.active_symbols.keys())
  wss_client.run()

async def bar_data_handler(bar):
  logger = logging.getLogger(__name__)
  g.bars[bar.symbol].append(bar)
  if bar.symbol == 'AIRC':
    logger.info('bars received')

def account_wss():
  trading_stream = TradingStream(g.key, g.secret_key, paper=g.is_paper)
  trading_stream.subscribe_trade_updates(trading_stream_handler)
  trading_stream.run()

async def trading_stream_handler(data):
  logger.info(data)

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