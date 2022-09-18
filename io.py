import sys
import os
import logging
import pandas as pd
import asyncio
import glob
import json
from datetime import datetime as dt
import pytz as tz
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass
from alpaca.data.live import StockDataStream
from alpaca.trading.stream import TradingStream
from alpaca.trading.models import Asset
from . import config as g
from . import trade
from . import signal

def get_assets():
  search_params = GetAssetsRequest(asset_class=AssetClass.US_EQUITY)
  assets = g.tclient.get_all_assets(search_params)
  assets_dict = {}
  for a in assets:
    assets_dict[a.symbol] = a
  return assets_dict

'''
Saving/restoring trade objects
'''
def read_trade(path, assets):
  with open(path, 'r') as f:
    trade_dict = json.load(f)
  t = trade.Trade([assets[trade_dict['symbols'][0]],
      assets[trade_dict['symbols'][1]]], trade_dict['pearson'], 
      trade_dict['pearson_historical'])
  path = path.split('.')[0] + '.csv'
  sigma_series = pd.read_csv(path, index_col=0,
                             squeeze=True, parse_dates=True)
  sigma_series.index = sigma_series.index.tz_convert(tz='US/Eastern')
  t.open_init(trade_dict, sigma_series)
  return t

def load_trades():
  logger = logging.getLogger(__name__)
  symbol_list = []
  trades = {}
  assets = get_assets()
  pearson = pd.read_csv('%s/pearson.csv' % g.root, index_col=0)
  open_trade_list = glob.glob('%s/open_trades/*.json' % g.root)
  logger.info('Loading open trades')
  for open_trade in open_trade_list:
    title = open_trade.split('/')[-1]
    title = title.split('.')[0]
    symbols = title.split('-')
    symbol_list.extend(symbols)
    trades[title] = read_trade(open_trade, assets)
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
  symbol_list.extend(g.HEDGE_SYMBOL_LIST)
  for p in g.positions:
    if p.symbol not in symbol_list:
      logger.warning('There is an unknown position in %s' % p.symbol)
  asset_dict = {}
  for symbol in set(symbol_list):
    asset_dict[symbol] = assets[symbol]
  for i in range(i):
    if assets_dict[g.HEDGE_SYMBOLS_LIST[i]].fractionable):
      g.HEDGE_SYMBOL = g.HEDGE_SYMBOLS_LIST[i]
      logger.info('g.HEDGE_SYMBOL = %s', g.HEDGE_SYMBOL)
      break
    if i == len(g.HEDGE_SYMBOLS_LIST)-1:
      logger.error('None of the symbols in g.HEDGE_SYMBOL_LIST are fractionable, exiting')
      sys.exit(1)
  return asset_dict, trades

def stock_wss():
  wss_client = StockDataStream(g.key, g.secret_key)
  wss_client.subscribe_bars(bar_data_handler, 
                            *g.active_symbols.keys())
  wss_client.run()

async def bar_data_handler(bar):
  g.bars[bar.symbol].append(bar)

def account_wss():
  trading_stream = TradingStream(g.key, g.secret_key, paper=g.is_paper)
  trading_stream.subscribe_trade_updates(trading_stream_handler)
  trading_stream.run()

# update is class alpaca.trading.models TradeUpdate
async def trading_stream_handler(update):
  logger = logging.getLogger(__name__)
  if update.order.client_order_id not in g.orders.keys():
    logger.error('TradeUpdate for a trade not in g.orders:')
    logger.error(update)
  else:
    if update.order.side == 'buy':
      g.orders[update.order.client_order_id]['buy'] = update.order
      logger.info(update)
    elif update.order.side == 'sell':
      g.orders[update.order.client_order_id]['sell'] = update.order
      logger.info(update)
    else:
      logger.warning('TradeUpdate for a trade with unknown side')
      logger.warning(update)

def load_config():
  logger = logging.getLogger(__name__)
  path = g.root + 'config.json'
  try:
    with open(path, 'r') as f:
      d = json.load(f)
      g.TO_OPEN_SIGNAL = d['TO_OPEN_SIGNAL']
      g.burn_list = d['burn_list']
  except IOError as error:
    logger.error(error)
    logger.info('Could not load config; using defaults')
    return
  logger.info('Loaded config')
  return

def save():
  logger = logging.getLogger(__name__)
  logger.info('Saving TO_OPEN_SIGNAL + burn_list')
  config_data = {'TO_OPEN_SIGNAL': g.TO_OPEN_SIGNAL, 'burn_list': g.burn_list}
  path = g.root + 'config.json'
  try:
    with open(path, 'w') as f:
      json.dump(config_data, f, indent=2)
  except IOError as error: logger.error(error)
  path = g.root + '/open_trades/*'
  logger.info('Emptying %s/open_trades' % g.root)
  files = glob.glob(path)
  for f in files: os.remove(f)
  logger.info('Saving open trades')
  for title, trade in g.trades.items():
    path = g.root + '/open_trades/' + title + '.json'
    try:
      with open(path, 'w') as f:
        json.dump(trade.to_dict(), f, indent=2)
    except IOError as error:
      logger.error('%s save failed:' % title)
      logger.error(error)
    path = g.root + '/open_trades/' + title + '.csv'
    trade.get_sigma_series().to_csv(path)
  logger.info('Save complete')
  return

def archive():
  logger = logging.getLogger(__name__)
  logger.info('Archiving closed trades')
  pass

def report():
  logger = logging.getLogger(__name__)
  logger.info('Generating today\'s report')
  pass