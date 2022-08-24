import os
import logging
import logging.handlers
from . import trade

logging.basicConfig(
  level=logging.INFO,
  format="%(asctime)s:%(levelname)s:%(name)s:%(message)s",
  handlers=[logging.handlers.WatchedFileHandler(os.environ.get("LOGFILE", "creek.log"))]
)
trade.Trade('AIRC', 'AVB')
trade.Trade('AVB', 'AIRC')