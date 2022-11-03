from alpaca.trading.client import TradingClient
from alpaca.data.historical import StockHistoricalDataClient

root = '/mnt/disks/creek-1/creek'
minute_bar_dir = '/mnt/disks/creek-1/us_equities'
hour_bar_dir = '/mnt/disks/creek-1/us_equities_hourly'
interpolated_bars_dir = '/mnt/disks/creek-1/us_equities_interpolated'
pearson_dir = '/mnt/disks/creek-1/pearson'
tf_dir = '/mnt/disks/creek-1/tf'
historical_year = 2015
key = 'PKN5IDVMKEVZT6BV5IQ0'
secret_key = 'gIod7zEIQ947E7qGryA6plPehXBYXkmXh1IH67S8'
is_paper = True
tclient = TradingClient(key, secret_key)
hclient = StockHistoricalDataClient(api_key=key, secret_key=secret_key)

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
burn_list = [] # This list contains those positions we bailed out of

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
MAX_TRADE_SIZE = 0.025
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
EXECUTION_ATTEMPTS = 20
'''
The symbol of the ETF used to hedge our positions.
Must be fractionable.
'''
HEDGE_SYMBOL = 'VXF'
'''
Other symbols to try if HEDGE_SYMBOL is not fractionable
List taken from https://www.forbes.com/sites/baldwin/2018/08/02/best-etfs-for-trading-small-and-mid-cap/
'''
HEDGE_SYMBOL_LIST = ['VXF', 'SMMD', 'IJH', 'VO', 'SCHM', 'IJR', 'IWM', 'VB', 'VTI']