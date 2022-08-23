import logging
import logging.handlers
import os
import pandas as pd
import sys
import getopt

handler = logging.handlers.WatchedFileHandler(
    os.environ.get("LOGFILE", "tf.log"))
formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(name)s:%(message)s")
handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOGLEVEL", "INFO"))
logger.addHandler(handler)

p = pd.DataFrame()
indices = []
dev_directory = '/mnt/disks/creek-1/tf/dev'
sigma = float(2)

def get_frame(row):
  global p
  global indices
  symbol1 = row['symbol1']
  symbol2 = row['symbol2']
  title = symbol1 + '-' + symbol2
  frame = pd.read_csv('%s/%s_dev.csv' % (dev_directory,title))
  assert not frame.empty
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

def get_summarized_frame(row):
  global p
  global indices
  symbol1 = row['symbol1']
  symbol2 = row['symbol2']
  title = symbol1 + '-' + symbol2
  frame = pd.read_csv('%s/%s_dev.csv' % (dev_directory,title))
  assert not frame.empty
  frame = frame.drop(columns=['vwap_1','vwap_2','mean','stddev'],axis=1)
  frame.set_index('timestamp', inplace=True)
  frame.index = pd.to_datetime(frame.index)
  column_title = '>%.1f_sigma' % sigma
  if p.empty:
    p = frame 
    p[column_title] = p.apply(lambda row: 1 if row['dev'] >= sigma else 0, axis=1)
    p = p.drop(columns=['dev'], axis=1)
  else:
    p = p.merge(frame, how='outer', on='timestamp', suffixes=(None,None))
    p['temp'] = p.apply(lambda row: 1 if row['dev'] >= sigma else 0, axis=1)
    p[column_title] = p[column_title] + p['temp']
    p = p.drop(columns=['dev', 'temp'], axis=1)
  return

def summarize(row):
  counter = 0
  for i in indices:
    if row[i] >= sigma: counter += 1
  return counter

def main(argv):
  global sigma
  terse = 1
  arg_help = "{0} -s <sigma> -t <terse> (default: sigma = 2, terse = 1)".format(argv[0])
  try:
    opts, args = getopt.getopt(argv[1:], "hs:t:", ["help", "sigma=", "terse="])
  except:
    print(arg_help)
    sys.exit(2)

  for opt, arg in opts:
    if opt in ("-h", "--help"):
      print(arg_help)
      sys.exit(2)
    elif opt in ("-s", "--sigma"):
      sigma = float(arg)
    elif opt in ("-t", "--terse"):
      terse = bool(eval(arg))
  global p
  pearson = pd.read_csv('pearson.csv')
  if terse:
    pearson.apply(get_summarized_frame, axis=1)
  else:
    pearson.apply(get_frame, axis=1)
    logger.info('Summarizing')
    column_title = '>%.1f_sigma' % sigma
    p[column_title] = p.apply(lambda row: summarize(row), axis=1)
    p.to_csv('list_dev.csv')
    p = p['summary']
  p.to_csv('summary_dev_minute_%.1f_sigma.csv' % sigma)
  # Now we bin by hour ('H') and by day ('D')
  p = p.resample('H').sum()
  p.to_csv('summary_dev_hour_%.1f_sigma.csv' % sigma)
  p = p.resample('D').sum()
  p.to_csv('summary_dev_day_%.1f_sigma.csv' % sigma)
  return

if __name__ == '__main__':
  main(sys.argv)