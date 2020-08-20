import pandas as pd
import sqlite3
import baostock as bs
import time
pd.set_option('display.max_columns', 1000)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 1000)


def trans_data(rs) -> pd.DataFrame:
    data_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        data_list.append(rs.get_row_data())
    result = pd.DataFrame(data_list, columns=rs.fields)
    return result


class SqlApi:
    def __init__(self, db_path='DATA/stocks.db'):
        self.conn = sqlite3.connect(db_path)
        self.cur = self.conn.cursor()

    def insert_data(self, df, table, if_exists='append'):
        df.to_sql(name=table, con=self.conn, if_exists=if_exists, index=False)

    def sql_query(self, sql) -> pd.DataFrame():
        return pd.read_sql_query(sql=sql, con=self.conn)

    def list_tables(self) -> pd.DataFrame():
        # _sql = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
        _sql = "SELECT * FROM sqlite_master;"
        return pd.read_sql(sql=_sql, con=self.conn)

    def shell(self):
        buffer = ""
        print("Enter your SQL commands(end by ;) to execute in sqlite3.")
        print("Enter a blank line to exit.")

        while True:
            line = input()
            st = time.time()
            if line == "":
                break
            buffer += line
            if sqlite3.complete_statement(buffer):
                try:
                    buffer = buffer.strip()
                    self.cur.execute(buffer)
                    if buffer.lstrip().upper().startswith("SELECT"):
                        print(self.cur.fetchall())
                except sqlite3.Error as e:
                    print("An error occurred:", e.args[0])
                buffer = ""
            et = time.time()
            print('-'*10, f'Done in {et-st} sec', '-'*10)


class DataApi(SqlApi):
    def __init__(self, market='China_Stock_A', db_path='DATA/stocks.db'):
        super(DataApi, self).__init__(db_path)
        self.time_index = 'date'
        self.__market = market

    @property
    def market(self):
        return self.__market

    def query_data(self, columns: list, beg_t: str, end_t: str = None, code=None, table='China_A_stocks') -> pd.DataFrame:
        """
        :param columns: columns to return from database.
        :param beg_t: start time for database query request.
        :param end_t: end time for database query request, only return data wheres date = beg_t when set to None.
        :param code: entity code, select all entity when set to None.
        :param table: table name.
        :return: pd.DataFrame.
        """
        columns.insert(0, self.time_index)
        columns.insert(0, 'code')
        columns_str = ", ".join(columns)

        if not end_t:
            end_t = beg_t

        if code:
            _sql = f"SELECT {columns_str} FROM {table} WHERE code is '{code}'" \
                   f" and date >= '{beg_t}' and date <= '{end_t}' ORDER BY {self.time_index};"
        else:
            _sql = f"SELECT {columns_str} FROM {table} WHERE " \
                   f"date >= '{beg_t}' and date <= '{end_t}' ORDER BY {self.time_index};"

        return self.sql_query(_sql)

    def query_trading_calender(self, beg_t=None, end_t=None, only_trading_time=True):
        """
        :param beg_t: start time of trading calendar
        :param end_t: end time of trading calendar, return all when both beg_t and end_t set to None
        :param only_trading_time: return only trading days in pd.Series format
        :return: pd.DataFrame for only_trading_time = False, pd.Series for only_trading_time = True
        """
        if beg_t is None and end_t is None:
            _sql = "SELECT * FROM China_A_TradingCalender ORDER BY calendar_date;"
        else:
            _sql = f"SELECT * FROM China_A_TradingCalender WHERE calendar_date >= '{beg_t}'" \
                   f" and calendar_date <= '{end_t}' ORDER BY calendar_date;"

        trading_calendar = self.sql_query(_sql)
        if only_trading_time:
            trading_calendar = trading_calendar[trading_calendar['is_trading_day'] == 1]['calendar_date']
            trading_calendar.reset_index(drop=True, inplace=True)

        return trading_calendar


if __name__ == '__main__':
    pass

