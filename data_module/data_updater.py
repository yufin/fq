import baostock as bs
import pandas as pd
from data_module.data_api import SqlApi
from tqdm import tqdm
import warnings
warnings.filterwarnings("ignore")
pd.set_option('display.max_columns', 1000)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 1000)

# TODAY = time.strftime("%Y-%m-%d", time.localtime())
TODAY = '2020-08-14'


class BsLogger:
    def __enter__(self):
        bs.login()

    def __exit__(self, exc_type, exc_val, exc_tb):
        bs.logout()


def trans_data(rs):
    data_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        data_list.append(rs.get_row_data())
    result = pd.DataFrame(data_list, columns=rs.fields)
    return result


def convert_to_numerical(df: pd.DataFrame) -> pd.DataFrame:
    for column in df.columns.values:
        df[column] = pd.to_numeric(df[column], errors='ignore')
    return df


def update_stock_data(star_t, end_t):
    sql = SqlApi()
    with BsLogger:
        symbol_list = trans_data(bs.query_all_stock(day=end_t))
        for code in tqdm(symbol_list['code']):
            # name = getattr(row, 'code_name')
            df = trans_data(
                bs.query_history_k_data_plus(code,
                                             "date,code,open,high,low,close,preclose,volume,"
                                             "amount,adjustflag,turn,tradestatus,pctChg,isST,"
                                             "peTTM,pbMRQ,psTTM,pcfNcfTTM",
                                             start_date=star_t, end_date=end_t,
                                             frequency="d", adjustflag="2")
            )
            df = convert_to_numerical(df)
            sql.insert_data(df=df, table='China_A_stocks')


def update_trading_calender(start_t, end_t):
    sql = SqlApi()
    with BsLogger():
        trading_calender = trans_data(bs.query_trade_dates(start_date=start_t, end_date=end_t))
        trading_calender = convert_to_numerical(trading_calender)
        # sql.insert_data(df=trading_calender, table='China_A_TradingCalender', if_exists='replace')
        print(trading_calender)
        print(trading_calender.dtypes)


if __name__ == '__main__':
    update_trading_calender('2010-01-01', '2020-08-20')
    update_trading_calender('2010-01-01', '2020-08-20')