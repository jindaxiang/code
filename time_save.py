import json
import time

from redis import Redis, ConnectionPool
import akshare as ak


# 采用连接池
pool = ConnectionPool(host="web-redis", port=6379, db=0, password='king')
rdb_conn = Redis(connection_pool=pool, decode_responses=True)


def save_data_to_rds(rdb: Redis):
    retries = 2
    while retries:
        stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
        stock_zh_a_spot_em_df = stock_zh_a_spot_em_df[['代码', "最新价", "涨跌幅", "涨跌额"]]
        stock_zh_a_spot_em_df.rename(
            columns={"代码": "code", "最新价": "price", "涨跌幅": "price_pct", "涨跌额": "price_chg"}, inplace=True)
        stock_zh_a_spot_em_df.fillna(0, inplace=True)
        # 保存股票代码和最新价
        big_dict = dict()
        for item in stock_zh_a_spot_em_df.itertuples():
            big_dict.update({item[1] : {"price": item[2], "price_pct": item[3], "price_chg": item[4]}})
        obj = rdb.set("code_price", json.dumps(big_dict))
        retries -= 1
    return obj


if __name__ == '__main__':
    save_data_to_rds(rdb=rdb_conn)
