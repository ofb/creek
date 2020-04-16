import alpaca_trade_api as alpaca
import asyncio
import pandas as pd
import sys

import logging

logger = logging.getLogger()


class PriceActionAlgo:

    def __init__(self, api, symbol, lot, timeDelta):
        self._api = api
        self._symbol = symbol
        self._lot = lot
        self._asset = self._api.get_asset(self._symbol)
        self._isTradable = self._asset.tradable
        self._isShortable = self._asset.easy_to_borrow and self._asset.shortable
        self._account = self._api.get_account()
        self._canShort = self._account.shorting_enabled
        self._timeDelta = timeDelta
        self._mbars = pd.DataFrame()
        self._sbars = pd.DataFrame()
        self._lastTrade = self._api.polygon.last_trade(self._symbol)
        self._l = logger.getChild(self._symbol)

        now = pd.Timestamp.now(tz='America/New_York').floor('1min')
        market_open = now.replace(hour=9, minute=30)
        today = now.strftime('%Y-%m-%d')
        tomorrow = (now + pd.Timedelta('1day')).strftime('%Y-%m-%d')
        data = api.polygon.historic_agg_v2(
            symbol, 1, 'minute', today, tomorrow, unadjusted=False).df
        mbars = data[market_open:]
        self._mbars = mbars

        self._init_state()

    def _init_state(self):
        now = self._now()
        symbol = self._symbol
        order = [o for o in self._api.list_orders() if o.symbol == symbol]
        position = [p for p in self._api.list_positions()
                    if p.symbol == symbol]
        self._order = order[0] if len(order) > 0 else None
        self._position = position[0] if len(position) > 0 else None
        if self._position is not None:
            self._positionTimestamp = now
            if self._order is None:
                if self._position.side == 'long':
                    self._state = 'LONG'
                elif self._position.side == 'short':
                    self._state = 'SHORT'
                else:
                    self._state = 'NEUTRAL'
                    self._l.warn(f'position {self._position} is neither short nor long, '
                        f'state {self._state} mismatch position {self._position}')
            else:
                if self._position.side == 'long' and self._order.side == 'sell':
                    self._state = 'SELL_SUBMITTED'
                elif self._position.side == 'short' and self._order.side == 'buy':
                    self._state = 'COVER_SUBMITTED'
                else:
                    self._state = 'NEUTRAL'
                    self._l.warn(f'order {self._order} is neither sell nor buy, '
                        f'position {self._position} mismatch order {self._order}, '
                        f'state {self._state} mismatch order {self._order}')
        else:
            self._positionTimestamp = None
            if self._order is None:
                self._state = 'NEUTRAL'
            elif self._order.side == 'buy':
                self._state = 'BUY_SUBMITTED'
            elif self._order.side == 'sell':
                self._state = 'SHORT_SUBMITTED'
            else:
                self._state = 'NEUTRAL'
                self._l.warn(
                    f'order {self._order} is neither sell nor buy, '
                    f'state {self._state} mismatch order {self._order}'
                )

    def _now(self):
        return pd.Timestamp.now(tz='America/New_York')

    def _outofmarket(self):
        return self._now().time() >= pd.Timestamp('15:55').time()

    def checkup(self):
        now = self._now()
        order = self._order
        if order is not None:
            elapsed = int((now - self._order.created_at.tz_convert(tz='America/New_York')).total_seconds())
        if self._state == 'SELL_SUBMITTED':
            if order is None:
                self._l.warn(f'state {self._state} mismatch order {self._order}')
                return
            if order.type != 'limit':
                return
            elif elapsed > self._timeDelta:
                self._cancel_order()
            else:
                new_price = self._lastTrade.price - elapsed/100
                self._replace_order_price(new_price)
        if self._state == 'COVER_SUBMITTED':
            if order is None:
                self._l.warn(f'state {self._state} mismatch order {self._order}')
                return
            if order.type != 'limit':
                return
            elif elapsed > self._timeDelta:
                self._cancel_order()
            else:
                new_price = self._lastTrade.price + elapsed/100
                self._replace_order_price(new_price)
        if self._position is not None and self._outofmarket():
            if self._position == 'long':
                self._submit_sell(bailout=True)
            elif self._position == 'short':
                self._submit_buy(bailout=True)
            else:
                self._l.warn(f'out of market but position {self._position} side unknown')

    def _cancel_order(self):
        if self._order is not None:
            try:
                self._api.cancel_order(self._order.id)
            except Exception as e:
                if hasattr(e,'status_code'):
                    self._l.info(f'Cancel failed with error {e.status_code}: {e}')
                else:
                    self._l.info(f'Cancel failed with error {e}')

    def _calc_cover_signal(self):
        position = self._position
        entered = self._positionTimestamp
        if (position is None or entered is None or
            position.side != 'short' or self._state != 'SHORT'):
            return False
        ep = float(position.avg_entry_price)
        if ep < self._sbars.close[-1]:
            return True
        min = self._calc_position_min(entered)
        delta = ep - min
        if delta < 0:
            return True
        ceiling = self._calc_cover_threshold(ep, delta)
        if ceiling < self._sbars.close[-1]:
            return True
        else:
            return False

    def _calc_cover_threshold(self, ep, delta):
        min = ep - delta
        pct_delta = delta / ep
        if delta < 0:
            return ep
        elif 0 <= pct_delta < 0.001:
            ceil = min + (.5 * delta)
            return ceil
        elif 0.001 <= pct_delta <= 0.025:
            ceil = min + (0.0005 * ep)
            return ceil
        elif pct_delta > 0.025:
            ceil = min + (delta / 50)
            return ceil
        else:
            return ep

    def _calc_long_signal(self):
        for i in range(1,self._timeDelta+1):
            if self._sbars.open[-i] >= self._sbars.close[-i]:
                return False
            if self._sbars.low[-1] > self._sbars.high[-i]:
                self._l.info(
                    f'buy signal: low {self._sbars.low[-1]} > high {i} seconds ago {self._sbars.high[-i]}'
                )
                return True
        else:
            return False

    def _calc_position_max(self, entered):
        bars = self._sbars[entered:]
        max = bars['high'].max()
        return max

    def _calc_position_min(self, entered):
        bars = self._sbars[entered:]
        min = bars['low'].min()
        return min

    def _calc_sell_signal(self):
        position = self._position
        entered = self._positionTimestamp
        if (position is None or entered is None or
            position.side != 'long' or self._state != 'LONG'):
            return False
        ep = float(position.avg_entry_price)
        if ep > self._sbars.close[-1]:
            return True
        max = self._calc_position_max(entered)
        delta = max - ep
        if delta < 0:
            return True
        floor = self._calc_sell_threshold(ep, delta)
        if floor > self._sbars.close[-1]:
            return True
        else:
            return False

    def _calc_sell_threshold(self, ep, delta):
        max = ep + delta
        pct_delta = delta / ep
        if delta < 0:
            return ep
        elif 0 <= pct_delta < 0.001:
            floor = max - (.5 * delta)
            return floor
        elif 0.001 <= pct_delta <= 0.025:
            floor = max - (0.0005 * ep)
            return floor
        elif pct_delta > 0.025:
            floor = max - (delta / 50)
            return floor
        else:
            return ep

    def _calc_short_signal(self):
        for i in range(1,self._timeDelta+1):
            if self._sbars.open[-i] <= self._sbars.close[-i]:
                return False
            if self._sbars.high[-1] < self._sbars.low[-i]:
                self._l.info(
                    f'short signal: high {self._sbars.high[-1]} < low {i} seconds ago {self._sbars.low[-i]}'
                )
                return True
        else:
            return False

    def on_mbar(self, mbar):
        self._mbars = self._mbars.append(pd.DataFrame({
            'open': mbar.open,
            'high': mbar.high,
            'low': mbar.low,
            'close': mbar.close,
            'volume': mbar.volume,
        }, index=[mbar.start]))

        self._l.info(
            f'received minute bar start = {mbar.start}, close = {mbar.close}, len(mbars) = {len(self._mbars)}')

    def on_order_update(self, event, order):
        now = self._now()
        self._l.info(f'order update: {event} = {order}')
        if event == 'fill':
            self._order = None
            if self._state == 'BUY_SUBMITTED':
                self._position = self._api.get_position(self._symbol)
                if self._positionTimestamp is None:
                    self._positionTimestamp = now
                self._transition('LONG')
                return
            elif self._state == 'SELL_SUBMITTED':
                self._position = None
                self._positionTimestamp = None
                self._transition('NEUTRAL')
                return
            elif self._state == 'SHORT_SUBMITTED':
                self._position = self._api.get_position(self._symbol)
                if self._positionTimestamp is None:
                    self._positionTimestamp = now
                self._transition('SHORT')
                return
            elif self._state == 'COVER_SUBMITTED':
                self._position = None
                self._positionTimestamp = None
                self._transition('NEUTRAL')
                return
        elif event == 'partial_fill':
            self._position = self._api.get_position(self._symbol)
            if ((self._state == 'BUY_SUBMITTED' or
                self._state == 'SHORT_SUBMITTED') and
                self._positionTimestamp is None):
                self._positionTimestamp = now
            self._order = self._api.get_order(order['id'])
            return
        elif event == 'replaced':
            self._order = self._api.get_order(order['id'])
            return
        elif event in ('canceled', 'rejected'):
            if event == 'rejected':
                self._l.warn(f'order rejected: current order = {self._order}')
            self._order = None
            if self._state == 'BUY_SUBMITTED':
                if self._position is not None:
                    self._transition('LONG')
                else:
                    self._transition('NEUTRAL')
            elif self._state == 'SELL_SUBMITTED':
                self._transition('TO_SELL')
                self._submit_sell(bailout=True)
            elif self._state == 'SHORT_SUBMITTED':
                if self._position is not None:
                    self._transition('SHORT')
                else:
                    self._transition('NEUTRAL')
            elif self._state == 'COVER_SUBMITTED':
                self._transition('TO_COVER')
                self._submit_buy(bailout=True)
            else:
                self._l.warn(f'unexpected state for {event}: {self._state}')

    def on_sbar(self, sbar):
        self._sbars = self._sbars.append(pd.DataFrame({
            'open': sbar.open,
            'high': sbar.high,
            'low': sbar.low,
            'close': sbar.close,
            'volume': sbar.volume,
        }, index=[sbar.start]))
        now = self._now()
        # self._sbars = self._sbars[(now - pd.Timedelta(self._timeDelta,units="seconds")):]

        if len(self._sbars) < self._timeDelta:
            return
        if self._outofmarket():
            return
        long_signal = self._calc_long_signal()
        short_signal = self._calc_short_signal()
        order = self._order
        if self._state == 'NEUTRAL':
            if long_signal:
                self._transition('TO_BUY')
                self._submit_buy()
                return
            elif short_signal:
                self._transition('TO_SHORT')
                self._submit_sell()
                return
            else:
                return
        elif self._state == 'BUY_SUBMITTED':
            if (long_signal and order is not None
                and order.side == 'buy'
                and sbar.close > float(order.limit_price)):
                self._l.info(
                    f'adjusting long order {order.id} at {order.limit_price} '
                    f'(current price = {self._lastTrade.price})')
                self._replace_order_price(self._lastTrade.price)
                return
            elif not long_signal:
                self._l.info(
                    f'long order {order.id} no longer good; '
                    f'attempting to cancel')
                self._cancel_order()
                return
            else: return
        elif self._state == 'SHORT_SUBMITTED':
            if (short_signal and order is not None
                and order.side == 'sell'
                and sbar.close < float(order.limit_price)):
                self._l.info(
                    f'adjusting short order {order.id} at {order.limit_price} '
                    f'(current price = {self._lastTrade.price})')
                self._replace_order_price(self._lastTrade.price)
                return
            elif not short_signal:
                self._l.info(
                    f'short order {order.id} no longer good; '
                    f'attempting to cancel')
                self._cancel_order()
                return
            else: return
        elif self._state == 'LONG':
            sell_signal = self._calc_sell_signal()
            if sell_signal:
                self._transition('TO_SELL')
                self._submit_sell()
                return
            else: return
        elif self._state == 'SHORT':
            cover_signal = self._calc_cover_signal()
            if cover_signal:
                self._transition('TO_COVER')
                self._submit_buy()
                return
            else: return


    def on_trade(self, trade):
        self._lastTrade = trade
    
    def _replace_order_price(self, new_price):
        if self._order is None:
            return
        old_order = self._order
        try:
            order = self._api.replace_order(
                order_id=self._order.id,
                limit_price=new_price,
            )
        except Exception as e:
            if hasattr(e,'status_code'):
                self._l.info(f'Replace submit failed with error {e.status_code}: {e}')
            else:
                self._l.info(f'Replace submit failed with error {e}')
            return
        self._order = order
        self._l.info(
            f'order {order} submitted to replace {order.replaces}; '
            f'new limit price {new_price} to replace old limit price {old_order.price}'
        )
    
    def _submit_buy(self, bailout=False):
        if not self._isTradable:
            self._l.info(f'{self._symbol} is not tradable')
            self._transition('NEUTRAL')
            return
        if self._position is not None and self._position.side == 'short':
            amount = abs(self._position.qty)
        else:
            amount = int(self._lot / self._lastTrade.price)
        params = dict(
            symbol=self._symbol,
            side='buy',
            qty=amount,
            time_in_force='day',
        )
        if bailout:
            params['type'] = 'market'
        else:
            params.update(dict(
                type='limit',
                limit_price=self._lastTrade.price,
            ))
        try:
            order = self._api.submit_order(**params)
        except Exception as e:
            if hasattr(e,'status_code'):
                self._l.info(f'Buy submit failed with error {e.status_code}: {e}')
            else:
                self._l.info(f'Buy submit failed with error {e}')
            if self._position is not None and self._position.side == 'short':
                self._transition('SHORT')
            else:
                self._transition('NEUTRAL')
            return

        self._order = order
        self._l.info(f'submitted {order.type} buy {order}')
        if self._state == 'TO_BUY':
            self._transition('BUY_SUBMITTED')
        elif self._state == 'SHORT':
            self._transition('COVER_SUBMITTED')
        else:
            self._l.warn(f'state {self._state} mismatch order {order}')

    def _submit_sell(self, bailout=False):
        if not self._isTradable:
            self._l.info(f'{self._symbol} is not tradable')
            self._transition('NEUTRAL')
            return
        if not self._canShort:
            self._l.info(f'account shorting disabled')
            self._transition('NEUTRAL')
            return
        if self._position is None and not self._isShortable:
            self._l.info(f'{self._symbol} is not shortable')
            self._transition('NEUTRAL')
            return
        if self._position is not None and self._position.side == 'long':
            amount = self._position.qty
        else:
            amount = int(self._lot / self._lastTrade.price)
        params = dict(
            symbol=self._symbol,
            side='sell',
            qty=amount,
            time_in_force='day',
        )
        if bailout:
            params['type'] = 'market'
        else:
            params.update(dict(
                type='limit',
                limit_price=self._lastTrade.price,
            ))
        try:
            order = self._api.submit_order(**params)
        except Exception as e:
            if hasattr(e,'status_code'):
                self._l.info(f'Sell submit failed with error {e.status_code}: {e}')
            else:
                self._l.info(f'Sell submit failed with error {e}')
            if self._position is not None and self._position.side == 'long':
                self._transition('LONG')
            else:
                self._transition('NEUTRAL')
            return

        self._order = order
        self._l.info(f'submitted {order.type} sell {order}')
        if self._state == 'TO_SHORT':
            self._transition('SHORT_SUBMITTED')
        elif self._state == 'LONG':
            self._transition('SELL_SUBMITTED')
        else:
            self._l.warn(f'state {self._state} mismatch order {order}')

    def _transition(self, new_state):
        self._l.info(f'transition from {self._state} to {new_state}')
        self._state = new_state


def main(args):
    api = alpaca.REST()
    stream = alpaca.StreamConn()

    fleet = {}
    symbols = args.symbols
    for symbol in symbols:
        algo = PriceActionAlgo(api, symbol, lot=args.lot, timeDelta=args.timeDelta)
        fleet[symbol] = algo

    @stream.on(r'^status$')
    async def on_status(conn, channel, data):
        logger.info(f'polygon status update {data}')

    @stream.on(r'^account_updates$')
    async def on_account_updates(conn, channel, account):
        logger.info(f'account {account}')

    @stream.on(r'^AM$')
    async def on_mbars(conn, channel, data):
        if data.symbol in fleet:
            fleet[data.symbol].on_mbar(data)

    @stream.on(r'^A$')
    async def on_sbars(conn, channel, data):
        if data.symbol in fleet:
            fleet[data.symbol].on_sbar(data)

    @stream.on(r'^T$')
    async def on_trades(conn, channel, data):
        if data.symbol in fleet:
            fleet[data.symbol].on_trade(data)

    @stream.on(r'^trade_updates$')
    async def on_trade_updates(conn, channel, data):
        logger.info(f'trade_updates {data}')
        symbol = data.order['symbol']
        if symbol in fleet:
            fleet[symbol].on_order_update(data.event, data.order)

    async def in_market_check():
        while True:
            if not api.get_clock().is_open:
                logger.info('exit as market is not open')
                sys.exit(0)
            await asyncio.sleep(60)

    async def position_check():
        while True:
            positions = api.list_positions()
            for symbol, algo in fleet.items():
                pos = [p for p in positions if p.symbol == symbol]
                if len(pos) > 0:
                    algo.checkup()
            await asyncio.sleep(1)

    channels = ['trade_updates'] + ['account_updates'] + [
        'AM.' + symbol for symbol in symbols
    ] + [
        'A.' + symbol for symbol in symbols
    ] + [
        'T.' + symbol for symbol in symbols
    ]

    loop = stream.loop
    loop.run_until_complete(asyncio.gather(
        stream.subscribe(channels),
        in_market_check(),
        position_check(),
    ))
    loop.close()


if __name__ == '__main__':
    import argparse

    fmt = '%(asctime)s:%(filename)s:%(lineno)d:%(levelname)s:%(name)s:%(message)s'
    logging.basicConfig(level=logging.INFO, format=fmt)
    fh = logging.FileHandler('console.log')
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter(fmt))
    logger.addHandler(fh)

    parser = argparse.ArgumentParser()
    parser.add_argument('symbols', nargs='+')
    parser.add_argument('--lot', type=float, default=2000)
    parser.add_argument('--timeDelta', type=int, default=10, help="integer time delta in seconds (default: 10)")

    main(parser.parse_args())
