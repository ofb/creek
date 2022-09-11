from alpaca.trading.client import TradingClient

root = '/mnt/disks/creek-1/creek'
key = 'PK52PMRHMCY15OZGMZLW'
secret_key = 'F8270IxVZS3hXdghv7ChIyQUalFRIZZxYYqMKfUh'
is_paper = True
tclient = TradingClient(key, secret_key)

active_symbols = {} # 'SYMBOL': <Symbol_Object>
trades = {} # 'SYMBOL1-SYMBOL2': <Trade_Object>
closed_trades = []
bars = {} # 'SYMBOL': [<Bar>]
orders = {} # 'SYMBOL1-SYMBOL2': {'long': <Order>, 'short': <Order>}
trade_size = 0.0
retarget = {'missed':[],'util':[]}
cash = 0.0
equity = 0.0
positions = []

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
'''
SIGMA_CUSHION determines how far off the last trade price to try to
enter or exit a new position. The initial extra cushion is the smaller 
of the bid-ask spread or 1 sigma * SIGMA_CUSHION.
'''
SIGMA_CUSHION = 0.025
'''
SIGMA_BOX is the most sigma to sacrifice getting in or out of a 
position before executing a market order. The way the execution works is
by initiating a series of limit orders with gradually larger deltas off
the last trade price. (SIGMA_CUSHION determines the initial delta.)
After EXECUTION_ATTEMPTS attempts or after the last price has left the
SIGMA_BOX we execute a market order to resolve the trade.
'''
SIGMA_BOX = 0.2
'''
EXECUTION_ATTEMPTS determines how many tries to attempt to get in or out
of a position before resolving via a market order.
'''
EXECUTION_ATTEMPTS = 10