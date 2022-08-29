import logging
import pandas as pd
import asyncio
import glob
import requests
from creek import root
from creek import key
from creek import secret_key
from creek import url_v2
from . import trade
from . import signal

def get_assets():
  headers = {'APCA-API-KEY-ID': key, 'APCA-API-SECRET-KEY': secret_key}
  url = url_v2
  r = requests.get(url + 'assets', headers=headers)
  d = {}
  for element in r.json():
    d[r['symbol']] = element
  return d
'''
Saving/restoring trade objects
'''
def read_trade(path):
  open_trade_df = pd.read_csv(open_trade, index_col=0)
  open_trade_df.set_index('index', inplace=True)
  open_trade = open_trade_df.to_dict()[0]
  t = trade.Trade(open_trade['symbol1'], 
                  open_trade['symbol2'], float(open_trade['pearson']),
                  float(open_trade['pearson_historical']),
                  (bool(eval(open_trade['symbol1_tradable'])),
                   bool(eval(open_trade['symbol2_tradable'])))
                  (bool(eval(open_trade['symbol1_marginable'])),
                   bool(eval(open_trade['symbol2_marginable'])))
                  (bool(eval(open_trade['symbol1_shortable'])),
                   bool(eval(open_trade['symbol2_shortable'])))
                  (bool(eval(open_trade['symbol1_fractionable'])),
                   bool(eval(open_trade['symbol2_fractionable']))))
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
    trades[title].tradable = (
      bool(eval(assets[symbols[0]]['tradable'])),
      bool(eval(assets[symbols[1]]['tradable'])))
    trades[title].marginable = (
      bool(eval(assets[symbols[0]]['marginable'])),
      bool(eval(assets[symbols[1]]['marginable'])))
    trades[title].shortable = (
      bool(eval(assets[symbols[0]]['shortable'])),
      bool(eval(assets[symbols[1]]['shortable'])))
    trades[title].fractionable = (
      bool(eval(assets[symbols[0]]['fractionable'])),
      bool(eval(assets[symbols[1]]['fractionable'])))
  logger.info('Loading remaining tensorflow models')
  for index, row in pearson.iterrows():
    title = row['symbol1'] + '-' + row['symbol2']
    if title in trades.keys():
      trades[title].pearson = float(row['pearson'])
      trades[title].pearson_historical= float(row['pearson_historical'])
    if title not in trades.keys():
      active_symbols.extend([row['symbol1'],row['symbol2']])
      trades[title] = trade.Trade(row['symbol1'], row['symbol2'], 
        float(row['pearson']), float(row['pearson_historical']),
        (bool(eval(assets[row['symbol1']]['tradable'])),
        bool(eval(assets[row['symbol2']]['tradable'])))
        (bool(eval(assets[row['symbol1']]['marginable'])),
        bool(eval(assets[row['symbol2']]['marginable'])))
        (bool(eval(assets[row['symbol1']]['shortable'])),
        bool(eval(assets[row['symbol2']]['shortable'])))
        (bool(eval(assets[row['symbol1']]['fractionable'])),
        bool(eval(assets[row['symbol2']]['fractionable']))))
  return set(active_symbols), trades

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