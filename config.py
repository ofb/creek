from alpaca.trading.client import TradingClient

root = '/mnt/disks/creek-1/creek'
key = 'PK52PMRHMCY15OZGMZLW'
secret_key = 'F8270IxVZS3hXdghv7ChIyQUalFRIZZxYYqMKfUh'
tclient = TradingClient(key, secret_key)

is_paper = True
active_symbols = {}
trades = {}
closed_trades = []
bars = {}
trade_size = 0.0

'''
MAX_SYMBOL is the upper bound for the overall position in any one symbol
across all trades as a fraction of total equity.
'''
MAX_SYMBOL = 0.05
'''
TO_OPEN_SYMBOL is the threshold above which the signal is triggered to
open a trade.
'''
TO_OPEN_SIGNAL = 3.0
'''
MAX_TRADE_SIZE is the upper bound for the size of any one trade as a 
fraction of total capital.
'''
MAX_TRADE_SIZE = 0.05
'''
EXCESS_CAPITAL is subtracted from absolute total capital to determine 
(usable) total capital. (Useful when needing to take money out.)
'''
EXCESS_CAPITAL = 0.0