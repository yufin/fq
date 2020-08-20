import pandas as pd
from data_module.data_api import DataApi
from tqdm import tqdm


class TradesRecorder:
    def __init__(self, period, beg_t, end_t, init_cash=1000000):
        self.__d_api = DataApi(db_path='../data_module/DATA/stocks.db')
        self.__period = period
        self.__begin_time: str = beg_t
        self.__end_time: str = end_t
        self.__cash = init_cash
        self.__trading_calendar = self.__d_api.query_trading_calender(
            self.begin_time, self.end_time, only_trading_time=True)
        self.__time_coord = self.__trading_calendar.iloc[0]
        self.__net_worth_curve = pd.DataFrame(columns=['time_index', 'total_asset', 'unit_share', 'net_worth'])
        self.__asset_pool = pd.DataFrame(columns=['time_index', 'entity', 'cost', 'volume', 'price'])
        self.__trading_log = pd.DataFrame()
        self.__asset_section_log = pd.DataFrame()
        self.__total_asset = init_cash
        self.__unit_share = init_cash
        self.next()

    @property
    def d_api(self):
        return self.__d_api

    @property
    def trading_calendar(self):
        return self.__trading_calendar

    @property
    def period(self):
        return self.__period

    @property
    def begin_time(self):
        return self.__begin_time

    @property
    def end_time(self):
        return self.__end_time

    @property
    def asset_pool(self):
        return self.__asset_pool

    @property
    def cash(self):
        return self.__cash

    @property
    def time_coord(self):
        return self.__time_coord

    @property
    def net_worth_curve(self):
        return self.__net_worth_curve

    @property
    def net_worth(self):
        return self.__net_worth_curve.iloc[-1]['net_worth']

    @property
    def total_asset(self):
        return self.__total_asset

    @property
    def trading_log(self):
        return self.__trading_log

    @property
    def asset_section_log(self):
        return self.__asset_section_log

    def trans_cash(self, trans_cash_value):
        # trans cash operation can only to be operated right after/before next function call
        self.__unit_share += trans_cash_value / self.net_worth
        self.__cash += trans_cash_value

    def place_order(self, entity: str, volume: float, cost: float = None, price: float = None):
        """update holding entity info of asset pool
            cost and volume should be positive number for buy option, negative number for sell option"""
        if not cost and not price:
            raise Exception("Error: At least one variable is required in params[cost, price]")
        if not price:
            price = cost / volume
        elif not cost:
            cost = price * volume

        if entity in self.__asset_pool['entity'].tolist():
            self.__asset_pool.loc[self.__asset_pool['entity'] == entity, ('cost', 'volume')] += (cost, volume)
            self.__asset_pool.loc[self.__asset_pool['entity'] == entity, ('price', 'time_index')] = (price, self.__time_coord)
        else:
            self.__asset_pool = self.__asset_pool.append(
                [{'time_index': self.__time_coord, 'entity': entity,
                  'cost': cost, 'volume': volume, 'price': price}], ignore_index=True)

        self.__cash -= cost
        self.__asset_pool.reset_index(drop=True, inplace=True)
        self.__trading_log = self.__trading_log.append(
                [{'time_index': self.__time_coord, 'entity': entity, 'cost': cost,
                  'volume': volume, 'price': price, 'cash': self.__cash}], ignore_index=True)

    def next(self):
        # update price of each entity from asset pool
        if not self.__asset_pool.empty:
            for i in range(len(self.__asset_pool)):
                self.__asset_pool.at[i, 'price'] = self.__d_api.query_data(
                    ['close'], beg_t=self.__time_coord, end_t=self.__time_coord,
                    code=self.__asset_pool.at[i, 'entity']).at[0, 'close']

        # update worth and time index
        self.__asset_pool['worth'] = self.__asset_pool['volume'] * self.__asset_pool['price']
        self.__asset_pool['time_index'] = self.__time_coord
        self.__total_asset = self.__asset_pool['worth'].sum() + self.__cash
        self.__net_worth_curve = self.__net_worth_curve.append(
            [{'time_index': self.__time_coord, 'total_asset': self.__total_asset,
              'unit_share': self.__unit_share, 'net_worth': self.__total_asset / self.__unit_share}], ignore_index=True)
        # drop all sold entity
        self.__asset_pool = self.__asset_pool[self.__asset_pool['volume'] != 0]
        # move time index to next trade day
        self.__trading_calendar.drop(index=0, inplace=True)
        self.__trading_calendar.reset_index(inplace=True, drop=True)
        self.__time_coord = self.__trading_calendar.iloc[0]
        self.__asset_section_log = self.__asset_section_log.append(self.__asset_pool, ignore_index=True)


class SimAgent(TradesRecorder):
    """Back testing environment simulation agent for Stock_China_A market"""
    def __init__(self, beg_t, end_t, init_cash=1000000, period='1d', market='Stock_China_A'):
        super(SimAgent, self).__init__(period, beg_t, end_t, init_cash)
        self._market = market
        self.__orders_queue = pd.DataFrame()

    def loc_delta_bar(self, time_coord, period_delta:int):
        if period_delta is not 0:
            _temp_calendar = self.d_api.query_trading_calender()
            _idx = _temp_calendar[_temp_calendar == time_coord].index[0]
            return _temp_calendar.loc[_idx - period_delta]
        else:
            return time_coord

    def market_order(self, entity, volume, order_at=(1, 'open'), commission=0.3/100):
        """
        Function to react order signal, order price depends on the ordering time,
        order is allowed to be deal at the end of the bar (only recommend in 'days' frequency)
        deal by close price, or by the open price of the next bar(in all frequency)

        :param entity: code of entity, with format like sh.000001.
        :param volume: order volume.
        :param order_at: tuple(delta for order occur bar, deal on what price),
                        eg:(0, 'close) means order deal by close price of current bar
        :param commission: commission rate for trading operation.
        """
        # detected future function
        if (order_at[0] == 0 and order_at[1] is not 'close') or (order_at[0] < 0):
            raise Exception("Future function detected!")

        if volume > 0:
            commission_x = 1 + commission
        elif volume < 0:
            commission_x = 1 - commission - 0.1/100
        else:
            raise Exception(f'Meaningless signal: order volume = {volume}')

        order_time_coord, price_of = order_at
        order_bar = self.loc_delta_bar(self.time_coord, order_time_coord)
        price = self.d_api.query_data(
            [price_of], beg_t=order_bar, code=entity).iloc[0]['open']
        cost = volume * price * commission_x

        if self.time_coord == order_time_coord and price_of == 'close':
            self.place_order(entity, cost, volume)
        else:
            self.__orders_queue.append(
                [{'order_coord': order_time_coord, 'entity': entity, 'cost': cost, 'volume': volume}], ignore_index=True)

    def next_bar(self):
        """
        Move time_coord to next bar,
        then check planned orders in queue and place orders.
        """
        self.next()
        if not self.__orders_queue.empty:
            ready_orders = self.__orders_queue.loc[self.__orders_queue['date'] == self.time_coord]
            for i in ready_orders.index:
                self.place_order(entity=ready_orders.loc[i]['entity'],
                                 cost=ready_orders.loc[i]['cost'],
                                 volume=ready_orders.loc[i]['volume'])
                self.__orders_queue.drop(index=i, inplace=True)
            self.__orders_queue.reset_index(drop=True, inplace=True)

    def regulate_data_api(self, columns: list, beg_t: str, end_t: str = None, code=None,):
        """Detect future function in back testing env."""
        if end_t > self.time_coord:
            raise Exception("Future function detected!")
        data = self.d_api.query_data(columns, beg_t, end_t, code)
        return data


if __name__ == '__main__':
    pass