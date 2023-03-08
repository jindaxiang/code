import time

import akshare as ak
import pandas as pd
import requests
from redis import Redis, ConnectionPool

stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
stock_code = stock_zh_a_spot_em_df['代码'].tolist()[:10]

# 采用连接池
pool = ConnectionPool(host="web-redis", port=6379, db=0, password='king')
rdb_conn = Redis(connection_pool=pool, decode_responses=True, encoding="utf-8")
for item in stock_code:
    rdb_conn.sadd("stock_array", item)


def get_latest_day() -> str:
    stock_zh_a_hist_min_em_df = ak.stock_zh_a_hist_min_em(symbol='000001', period='1')
    latest_date_str = stock_zh_a_hist_min_em_df['时间'].iloc[-1].split(" ")[0]
    return latest_date_str


retention_time = 1000 * 60


def fetch_realtime_stock_min_data(stock: str = "000001"):
    latest_date_str = get_latest_day()
    retries = 5
    while retries:
        try:
            stock_zh_a_hist_min_em_df = ak.stock_zh_a_hist_min_em(symbol=stock, period='1')
            stock_zh_a_hist_min_em_df = stock_zh_a_hist_min_em_df[[
                "时间",
                "开盘",
                "最高",
                "最低",
                "收盘",
                "成交量",
            ]]
            stock_zh_a_hist_min_em_df.rename(columns={
                "时间": "time",
                "开盘": "open",
                "最高": "high",
                "最低": "low",
                "收盘": "close",
                "成交量": "volume",
            }, inplace=True)
            stock_zh_a_hist_min_em_df.fillna(0, inplace=True)
            stock_zh_a_hist_min_em_df.index = pd.to_datetime(stock_zh_a_hist_min_em_df['time'])
            stock_zh_a_hist_min_em_df['time'] = pd.to_datetime(stock_zh_a_hist_min_em_df['time'])
            need_df = stock_zh_a_hist_min_em_df.loc[latest_date_str].copy()
            need_df.reset_index(inplace=True, drop=True)
            # 应对开盘价为 0 的情况
            if need_df.iat[0, 1] == 0:
                need_df['open'] = (need_df['high'] + need_df['low']) * 0.5
            return need_df.to_json(orient='records', date_format="iso")
        except requests.exceptions.JSONDecodeError as exc:
            if retries == 0:
                raise exc
            retries -= 1
            time.sleep(0.5)


def save_data_to_rds(rdb):
    while True:
        if rdb.smembers("stock_array"):
            stock_list = rdb.smembers("stock_array")
        else:
            stock_list = []
        for stock in stock_list:
            print(stock.decode())
            stock_zh_a_spot_em_df = fetch_realtime_stock_min_data(stock=stock.decode())
            rdb.set(stock, stock_zh_a_spot_em_df)
        time.sleep(6)


if __name__ == '__main__':
    save_data_to_rds(rdb=rdb_conn)
