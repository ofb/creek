import logging
import logging.handlers
import os
import time
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

handler = logging.handlers.WatchedFileHandler(
    os.environ.get("LOGFILE", "historical_data.log"))
formatter = logging.Formatter(logging.BASIC_FORMAT)
handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOGLEVEL", "INFO"))
logger.addHandler(handler)

def yesterday_string():
  yesterday = date.today() - timedelta(days=1)
  return yesterday.strftime('%Y-%m-%d')

def fetch_bars(stock_request):
  while True:
    try:
      bars = client.get_stock_bars(stock_request)
      return bars
    except APIError as api_error:
      logger.error('APIError code %s', api_error.status_code)
      if api_error.status_code == 429:
        logger.info('Sleeping for 60 seconds')
        time.sleep(60)
      else:
        return pd.DataFrame()

def compile_bars(bars,number_of_years):
  current_year = date.today().year
  compiled_bars = bars
  cutoff = 2015
  if current_year - number_of_years + 1 < cutoff:
    number_of_years = current_year - cutoff + 1
  if number_of_years > 0:
    request_params = StockBarsRequest(
                          symbol_or_symbols="AAPL",
                          timeframe=TimeFrame.Day,
                          limit=None,
                          start="%s-01-01" % current_year,
                          end=yesterday_string(),
                          adjustment="all"
                     )
    new_bars = fetch_bars(request_params)
    compiled_bars = pd.concat([new_bars.df,compiled_bars])
    logger.info('Fetched year %s', current_year)
  for i in range(1,number_of_years):
    year = current_year - i
    request_params = StockBarsRequest(
                          symbol_or_symbols="AAPL",
                          timeframe=TimeFrame.Day,
                          limit=None,
                          start="%s-01-01" % year,
                          end="%s-12-31" % year,
                          adjustment="all"
                     )
    new_bars = fetch_bars(request_params)
    compiled_bars = pd.concat([new_bars.df,compiled_bars])
    logger.info('Fetched year %s', year)
  return compiled_bars

def main():
  bars = pd.DataFrame()
  bars = compile_bars(bars,20)
  bars.to_csv('AAPL.csv')

if __name__ == '__main__':
  main()