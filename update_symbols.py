import refresh_bars as rb
import creek_tf as ctf
import sys
import logging
import os

def update_symbols(*argv):
  logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(name)s:%(message)s",
    handlers=[logging.handlers.WatchedFileHandler(os.environ.get("LOGFILE", "creek-update_symbols.log"))]
  )
  logger = logging.getLogger(__name__)
  if not rb.sanity_check(): return
  symbol_list = [*argv]
  error_counter = False
  for i in range(len(symbol_list)):
    logger.info('Refreshing %s, %s/%s' %
                (symbol_list[i], i+1, len(symbol_list)))
    rb.refresh_bars(symbol_list[i],error_counter)
  if error_counter:
    logger.warn('Some symbols were not updated')
  ctf.refresh_symbols(*argv)
  return

if __name__ == '__main__':
  update_symbols(*(sys.argv[1:]))