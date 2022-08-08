import logging
import logging.handlers
import os
import time
import requests
from datetime import date
from datetime import timedelta
import pandas as pd
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.common.exceptions import APIError
# keys required for stock historical data client
# this are the paper trading keys
client = StockHistoricalDataClient('PK52PMRHMCY15OZGMZLW', 'F8270IxVZS3hXdghv7ChIyQUalFRIZZxYYqMKfUh')
headers = {
      'APCA-API-KEY-ID':'PK52PMRHMCY15OZGMZLW',
      'APCA-API-SECRET-KEY':'F8270IxVZS3hXdghv7ChIyQUalFRIZZxYYqMKfUh'
}

url_v2 = 'https://paper-api.alpaca.markets/v2/'

handler = logging.handlers.WatchedFileHandler(
    os.environ.get("LOGFILE", "historical_data.log"))
formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(name)s:%(message)s")
handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOGLEVEL", "INFO"))
logger.addHandler(handler)

def dayshift_string(n):
  yesterday = date.today() - timedelta(days=n)
  return yesterday.strftime('%Y-%m-%d')

def fetch_bars(stock_request):
  while True:
    try:
      bars = client.get_stock_bars(stock_request)
      return bars.df
    except APIError as api_error:
      logger.error('APIError code %s', api_error.status_code)
      if api_error.status_code == 429:
        logger.info('Sleeping for 60 seconds')
        time.sleep(60)
      else:
        return pd.DataFrame()
    except AttributeError as error:
      logger.warning('Empty request')
      return pd.DataFrame()

def compile_bars(symbol,number_of_years):
  current_year = date.today().year
  compiled_bars = pd.DataFrame()
  cutoff = 2015
  if current_year - number_of_years + 1 < cutoff:
    number_of_years = current_year - cutoff + 1
  if number_of_years > 0:
    request_params = StockBarsRequest(
                          symbol_or_symbols=symbol,
                          timeframe=TimeFrame.Minute,
                          limit=None,
                          start="%s-01-01" % current_year,
                          end=dayshift_string(5),
                          adjustment="all"
                     )
    new_bars = fetch_bars(request_params)
    compiled_bars = pd.concat([new_bars,compiled_bars])
    logger.info('Fetched %s year %s' % (symbol, current_year))
  for i in range(1,number_of_years):
    year = current_year - i
    request_params = StockBarsRequest(
                          symbol_or_symbols=symbol,
                          timeframe=TimeFrame.Minute,
                          limit=None,
                          start="%s-01-01" % year,
                          end="%s-12-31" % year,
                          adjustment="all"
                     )
    new_bars = fetch_bars(request_params)
    compiled_bars = pd.concat([new_bars,compiled_bars])
    logger.info('Fetched %s year %s' % (symbol, year))
  return compiled_bars

def get_shortable_equity_list():
  asset_list = requests.get(url_v2 + 'assets', headers=headers)
  assets_raw = asset_list.json()
  shortable_equity_list = []
  for element in assets_raw:
    if element['tradable']==True and element['class']=='us_equity' and element['shortable']==True:
      shortable_equity_list.append(element['symbol'])
  shortable_equity_symbols = {'symbol': shortable_equity_list}
  return pd.DataFrame(shortable_equity_symbols)


def main():
#  shortable_list = get_shortable_equity_list();
#  shortable_list.to_csv('shortable_equity_list.csv')
  shortable_list_df = pd.read_csv('shortable_equity_list_todo.csv')
  shortable_list = shortable_list_df['symbol'].tolist()
  processed_list_df = pd.read_csv('shortable_equity_list_processed.csv')
  processed_list = processed_list_df['symbol'].tolist()
  for i in range(min(10,len(shortable_list))):
    symbol = shortable_list.pop(0)
    logger.info('Fetching symbol %s', symbol)
    bars = compile_bars(symbol,10)
    bars.to_csv('/mnt/disks/creek-1/us_equities/%s.csv' % symbol)
    processed_list.append(symbol)
  updated_symbol_list = pd.DataFrame({'symbol': shortable_list})
  updated_symbol_list.to_csv('shortable_equity_list_todo.csv')
  processed_symbol_list = pd.DataFrame({'symbol': processed_list})
  processed_symbol_list.to_csv('shortable_equity_list_processed.csv')


if __name__ == '__main__':
  main()