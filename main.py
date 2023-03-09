"""
主文件
"""
import http.client
import json
from functools import lru_cache
import pandas as pd

import akshare as ak
import numpy as np
import uvicorn
from fastapi import Depends, FastAPI
from fastapi.responses import JSONResponse
from redis import Redis, ConnectionPool
from pydantic import BaseModel
import requests
from functools import lru_cache

from redis_stock_save_min import fetch_realtime_stock_min_data

app = FastAPI()


@lru_cache()
def code_id_map_em() -> dict:
    """
    东方财富-股票和市场代码
    http://quote.eastmoney.com/center/gridlist.html#hs_a_board
    :return: 股票和市场代码
    :rtype: dict
    """
    url = "http://80.push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1",
        "pz": "50000",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:1 t:2,m:1 t:23",
        "fields": "f12",
        "_": "1623833739532",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    if not data_json["data"]["diff"]:
        return dict()
    temp_df = pd.DataFrame(data_json["data"]["diff"])
    temp_df["market_id"] = 1
    temp_df.columns = ["sh_code", "sh_id"]
    code_id_dict = dict(zip(temp_df["sh_code"], temp_df["sh_id"]))
    params = {
        "pn": "1",
        "pz": "50000",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:0 t:6,m:0 t:80",
        "fields": "f12",
        "_": "1623833739532",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    if not data_json["data"]["diff"]:
        return dict()
    temp_df_sz = pd.DataFrame(data_json["data"]["diff"])
    temp_df_sz["sz_id"] = 0
    code_id_dict.update(dict(zip(temp_df_sz["f12"], temp_df_sz["sz_id"])))
    params = {
        "pn": "1",
        "pz": "50000",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:0 t:81 s:2048",
        "fields": "f12",
        "_": "1623833739532",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    if not data_json["data"]["diff"]:
        return dict()
    temp_df_sz = pd.DataFrame(data_json["data"]["diff"])
    temp_df_sz["bj_id"] = 0
    code_id_dict.update(dict(zip(temp_df_sz["f12"], temp_df_sz["bj_id"])))
    return code_id_dict


class RealtimeModel(BaseModel):
    code: str
    name: str
    price: float
    open: float
    high: float
    low: float
    volume: float
    chg_value: int
    chg_pct: float


class BidAskModel(BaseModel):
    buy_5: float
    buy_4: float
    buy_3: float
    buy_2: float
    buy_1: float
    sell_1: float
    sell_2: float
    sell_3: float
    sell_4: float
    sell_5: float
    buy_5_vol: int
    buy_4_vol: int
    buy_3_vol: int
    buy_2_vol: int
    buy_1_vol: int
    sell_1_vol: int
    sell_2_vol: int
    sell_3_vol: int
    sell_4_vol: int
    sell_5_vol: int


def get_rdb() -> Redis:
    """
    获取 Redis 连接池
    :return: 外汇 symbol 和代码映射
    :rtype: dict
    """
    pool = ConnectionPool(host="web-redis", port=6379, db=0, password="king")
    # pool = ConnectionPool(host="139.9.158.100", port=6379, db=0, password="king")
    rdb = Redis(connection_pool=pool, decode_responses=True, encoding="utf-8")
    try:
        yield rdb
    finally:
        rdb.close()


def get_http_conn() -> http.client.HTTPSConnection:
    """
    获取 Redis 连接池
    :return: 外汇 symbol 和代码映射
    :rtype: dict
    """
    conn = http.client.HTTPSConnection("finance.pae.baidu.com")
    try:
        yield conn
    finally:
        conn.close()


@lru_cache()
def get_latest_day() -> str:
    """
    获取最近的交易日
    :return: 返回最近的交易日
    :rtype: str
    """
    stock_zh_a_hist_min_em_df = ak.stock_zh_a_hist_min_em(symbol="000001", period="1")
    latest_date_str = stock_zh_a_hist_min_em_df["时间"].iloc[-1].split(" ")[0]
    return latest_date_str


def _stock_price_gene(mean_value: float = 14.02, num: int = 5) -> list:
    """
    生成随机数
    :param mean_value:
    :param num:
    :return:
    """
    while 1:
        buy_price_list = sorted(
            np.random.choice(
                list(
                    set(
                        [
                            round(mean_value + np.random.random() / 10, 2)
                            for i in range(100)
                        ]
                    )
                ),
                num,
                replace=False,
            )
        )
        if len(buy_price_list) == 10:
            return buy_price_list
        else:
            continue


def init_realtime_data(rdb: Redis = Depends(get_rdb)) -> None:
    """
    获取实时数据到 Redis 数据库
    :param rdb: Redis 数据库对象
    :return: None
    """
    stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
    stock_zh_a_spot_em_df = stock_zh_a_spot_em_df[["代码", "最新价", "涨跌幅", "涨跌额"]]
    stock_zh_a_spot_em_df.rename(
        columns={"代码": "code", "最新价": "price", "涨跌幅": "price_pct", "涨跌额": "price_chg"},
        inplace=True,
    )
    stock_zh_a_spot_em_df.fillna(0, inplace=True)
    # 保存股票代码和最新价
    big_dict = dict()
    for item in stock_zh_a_spot_em_df.itertuples():
        big_dict.update(
            {item[1]: {"price": item[2], "price_pct": item[3], "price_chg": item[4]}}
        )
    rdb.set("code_price", json.dumps(big_dict), 6)


@app.get("/stock_tick_data_old", deprecated=True)
async def get_stock_tick_data_old(
        symbol: str = "000001", rdb: Redis = Depends(get_rdb)
) -> JSONResponse:
    """
    生成模拟数据-tick 数据 \n
    利用最近 `ak.stock_zh_a_spot_em` 的真实数据来生成模拟数据
    :param symbol: 股票代码
    :param rdb: Redis 数据库对象
    :return: JSONResponse
    """
    # 此处需要在盘中进行数据模拟
    obj = rdb.get("code_price")
    if not obj:
        init_realtime_data()
    obj = rdb.get("code_price")
    code_price_dict = json.loads(obj)
    try:
        mean_value = round(code_price_dict[symbol]["price"], 2)
    except:
        return JSONResponse({})
    stock_price_list = _stock_price_gene(mean_value=mean_value, num=10)
    stock_price_list = [round(item, 2) for item in stock_price_list]

    stock_zh_a_hist_min_em_df = ak.stock_zh_a_hist_min_em(symbol=symbol, period="1")
    temp_se = stock_zh_a_hist_min_em_df.loc[
        len(stock_zh_a_hist_min_em_df) - 1, ["时间", "开盘", "最高", "最低", "收盘", "成交量"]
    ]
    temp_dict = temp_se.to_dict()

    if temp_dict["成交量"] > 20:
        stock_volume_list = np.random.randint(
            1, int(temp_dict["成交量"] / 10), 10
        ).tolist()
    else:
        stock_volume_list = [1] * 10
    result_list = stock_price_list + stock_volume_list
    name_list = [
        "buy_5",
        "buy_4",
        "buy_3",
        "buy_2",
        "buy_1",
        "sell_1",
        "sell_2",
        "sell_3",
        "sell_4",
        "sell_5",
        "buy_5_vol",
        "buy_4_vol",
        "buy_3_vol",
        "buy_2_vol",
        "buy_1_vol",
        "sell_1_vol",
        "sell_2_vol",
        "sell_3_vol",
        "sell_4_vol",
        "sell_5_vol",
    ]
    result_dict = dict(zip(name_list, result_list))
    return result_dict


@app.get("/stock_tick_data_baidu", response_model=BidAskModel, deprecated=True)
async def get_stock_tick_data_baidu(
        symbol: str = "000001", conn: http.client.HTTPSConnection = Depends(get_http_conn), rdb: Redis = Depends(get_rdb)
):
    obj = rdb.get(f"tick_{symbol}")
    if obj:
        return json.loads(obj)
    conn.request(
        "GET",
        f"/selfselect/getstockquotation?all=1&code={symbol}&isIndex=false&isBk=false&isBlock=false&isFutures=false&isStock=true&newFormat=1&group=quotation_minute_ab&finClientType=pc",
    )
    res = conn.getresponse()
    data = res.read()
    data_json = json.loads(data.decode("utf-8"))
    ask_dict = data_json["Result"]["askinfos"]
    bid_dict = data_json["Result"]["buyinfos"]
    big_dict = dict()
    for item in range(1, 6):
        big_dict[f"buy_{item}"] = bid_dict[item - 1]["bidprice"]
    for item in range(1, 6):
        big_dict[f"sell_{6 - item}"] = ask_dict[item - 1]["askprice"]
    for item in range(1, 6):
        big_dict[f"buy_{item}_vol"] = bid_dict[item - 1]["bidvolume"]
    for item in range(1, 6):
        big_dict[f"sell_{6 - item}_vol"] = ask_dict[item - 1]["askvolume"]
    rdb.set(f"tick_{symbol}", json.dumps(big_dict), 3)
    return big_dict


@app.get("/stock_tick_data_em", response_model=BidAskModel, deprecated=True)
async def get_stock_tick_data_em(
        symbol: str = "000001", rdb: Redis = Depends(get_rdb)
) -> JSONResponse:
    obj = rdb.get(f"tick_{symbol}")
    if obj:
        return json.loads(obj)
    else:
        url = "https://push2.eastmoney.com/api/qt/stock/get"
        code_id_map_em_dict = code_id_map_em()
        params = {
            "fltt": "2",
            "invt": "2",
            "fields": "f120,f121,f122,f174,f175,f59,f163,f43,f57,f58,f169,f170,f46,f44,f51,f168,f47,f164,f116,f60,f45,f52,f50,f48,f167,f117,f71,f161,f49,f530,f135,f136,f137,f138,f139,f141,f142,f144,f145,f147,f148,f140,f143,f146,f149,f55,f62,f162,f92,f173,f104,f105,f84,f85,f183,f184,f185,f186,f187,f188,f189,f190,f191,f192,f107,f111,f86,f177,f78,f110,f262,f263,f264,f267,f268,f255,f256,f257,f258,f127,f199,f128,f198,f259,f260,f261,f171,f277,f278,f279,f288,f152,f250,f251,f252,f253,f254,f269,f270,f271,f272,f273,f274,f275,f276,f265,f266,f289,f290,f286,f285,f292,f293,f294,f295",
            "secid": f"{code_id_map_em_dict[symbol]}.{symbol}",
        }
        r = requests.get(url, params=params)
        data_json = r.json()
        tick_dict = {
            "sell_5": data_json['data']['f31'],
            "sell_5_vol": data_json['data']['f32'] * 100,
            "sell_4": data_json['data']['f33'],
            "sell_4_vol": data_json['data']['f34'] * 100,
            "sell_3": data_json['data']['f35'],
            "sell_3_vol": data_json['data']['f36'] * 100,
            "sell_2": data_json['data']['f37'],
            "sell_2_vol": data_json['data']['f38'] * 100,
            "sell_1": data_json['data']['f39'],
            "sell_1_vol": data_json['data']['f40'] * 100,
            "buy_1": data_json['data']['f19'],
            "buy_1_vol": data_json['data']['f20'] * 100,
            "buy_2": data_json['data']['f17'],
            "buy_2_vol": data_json['data']['f18'] * 100,
            "buy_3": data_json['data']['f15'],
            "buy_3_vol": data_json['data']['f16'] * 100,
            "buy_4": data_json['data']['f13'],
            "buy_4_vol": data_json['data']['f14'] * 100,
            "buy_5": data_json['data']['f11'],
            "buy_5_vol": data_json['data']['f12'] * 100,

        }
        rdb.set(f"tick_{symbol}", json.dumps(tick_dict), 3)
        return tick_dict


@app.get("/stock_tick_data", response_model=BidAskModel)
async def get_stock_tick_data(
        symbol: str = "000001", rdb: Redis = Depends(get_rdb), conn: http.client.HTTPSConnection = Depends(get_http_conn)
):
    try:
        result_dict = await get_stock_tick_data_em(symbol=symbol, rdb=rdb)
        return result_dict
    except:
        try:
            result_dict = await get_stock_tick_data_baidu(symbol=symbol, conn=conn)
            return result_dict
        except:
            result_dict = await get_stock_tick_data_old(symbol=symbol, rdb=rdb)
            return result_dict


@app.get("/stock_min_data")
async def get_stock_min_data(symbol: str = "000001", rdb: Redis = Depends(get_rdb)):
    """
    获取分钟数据 \n
    利用最近 `fetch_realtime_stock_min_data` 来获取分钟的真实数据
    :param symbol: 股票代码
    :param rdb: Redis 数据库对象
    :return: JSONResponse
    """
    obj = rdb.get(symbol)
    if obj:
        # print("缓存获取")
        data_json = json.loads(obj)
        return JSONResponse(data_json)
    else:
        # print("访问获取")
        try:
            stock_zh_a_spot_em_df = fetch_realtime_stock_min_data(stock=symbol)
        except:
            return JSONResponse({})
        rdb.set(symbol, stock_zh_a_spot_em_df, 30)
        obj = rdb.get(symbol)
        data_json = json.loads(obj)
        return JSONResponse(data_json)


@app.get("/tick")
async def get_tick_data(symbol: str = "000001", rdb: Redis = Depends(get_rdb)):
    """
    获取实时数据数据 \n
    获取一个或者多个股票的行情数据
    :param symbol: 股票代码
    :param rdb: Redis 数据库对象
    :return: JSONResponse
    """
    big_dict = dict()
    try:
        code_list = symbol.split(",")
    except:
        return JSONResponse({})

    obj = rdb.get("code_price")
    if not obj:
        init_realtime_data()
        obj = rdb.get("code_price")
        if not obj:
            return JSONResponse({})
    obj_dict = json.loads(obj)

    for item in code_list:
        try:
            big_dict.update({item: obj_dict[item]})
        except:
            return JSONResponse({})
    if big_dict:
        return JSONResponse(big_dict)
    else:
        return JSONResponse({})


@app.get("/realtime")
async def get_realtime_data(rdb: Redis = Depends(get_rdb)):
    obj = rdb.get(f"realtime")
    if obj:
        obj_dict = json.loads(obj)
        return JSONResponse(obj_dict)
    stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
    stock_zh_a_spot_em_df = stock_zh_a_spot_em_df[
        ["代码", "名称", "最新价", "今开", "最高", "最低", "成交量", "涨跌额", "涨跌幅"]
    ]
    stock_zh_a_spot_em_df.rename(
        columns={
            "代码": "code",
            "名称": "name",
            "最新价": "price",
            "今开": "open",
            "最高": "high",
            "最低": "low",
            "成交量": "volume",
            "涨跌额": "chg_value",
            "涨跌幅": "chg_pct"
        },
        inplace=True,
    )
    stock_zh_a_spot_em_df.sort_values(["code"], inplace=True, ignore_index=True)
    stock_zh_a_spot_em_df.dropna(inplace=True)
    stock_zh_a_spot_em_df.reset_index(inplace=True, drop=True)
    data_json = stock_zh_a_spot_em_df.to_dict(orient="records")
    rdb.set("realtime", json.dumps(data_json), 6)
    return JSONResponse(data_json)


@app.get("/trade_date")
async def get_trade_date(date: str = "20230307", rdb: Redis = Depends(get_rdb)):
    init_trade_date = rdb.get("trade_date")
    if not init_trade_date:
        tool_trade_date_hist_sina_df = ak.tool_trade_date_hist_sina()
        date_str_list = [
            item.isoformat() for item in tool_trade_date_hist_sina_df["trade_date"]
        ]
        rdb.set("trade_date", json.dumps(date_str_list))
    else:
        trade_date = json.loads(init_trade_date)
        date_str = f"{date[:4]}-{date[4:6]}-{date[6:]}"
        print(date_str)
        if date_str in trade_date:
            return {"result": "1"}
        else:
            return {"result": "0"}


if __name__ == "__main__":
    uvicorn.run(app=app)
