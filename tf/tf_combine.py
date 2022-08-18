import logging
import logging.handlers
import os
import pandas as pd

handler = logging.handlers.WatchedFileHandler(
    os.environ.get("LOGFILE", "tf.log"))
formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(name)s:%(message)s")
handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOGLEVEL", "INFO"))
logger.addHandler(handler)

p = pd.DataFrame()
indices = []

def get_frame(row):
  global p
  global indices
  symbol1 = row['symbol1']
  symbol2 = row['symbol2']
  title = symbol1 + '-' + symbol2
  frame = pd.read_csv('dev/%s_dev.csv' % (title))
  assert not frame.empty
  frame_length = len(frame)
  if (frame_length < 15000):
    logger.warning('%s has only %s rows, skipping' % (title, frame_length))
    return
  frame[title] = frame['dev']
  frame = frame.drop(columns=['vwap_1','vwap_2','mean','stddev','dev'],axis=1)
  frame.set_index('timestamp', inplace=True)
  frame.index = pd.to_datetime(frame.index)
  if p.empty:
    p = frame
  else:
    p = p.merge(frame, how='outer', on='timestamp', suffixes=(None,None))
    indices.append(title)
  return

def summarize(row):
  counter = 0
  for i in indices:
    if row[i] >= 2: counter += 1
  return counter

def main():
  global p
  pearson = pd.read_csv('pearson.csv')
  pearson.apply(get_frame, axis=1)
  logger.info('Summarizing')
  p['summary'] = p.apply(lambda row: summarize(row), axis=1)
  p.to_csv('list_dev.csv')
  p = p['summary']
  p.to_csv('summary_dev.csv')
  logger.info('Done')
  return

if __name__ == '__main__':
  main()