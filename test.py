import akshare as ak

stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
stock_zh_a_spot_em_df = stock_zh_a_spot_em_df[
    ["代码", "名称", "最新价", "今开", "最高", "最低", "成交量", "涨跌额", "涨跌幅"]
]
stock_zh_a_spot_em_df.rename(
    columns={
        "今开": "开盘价",
        "最高": "最高价",
        "最低": "最低价",
    },
    inplace=True,
)
stock_zh_a_spot_em_df.sort_values(["代码", "最新价"], inplace=True, ignore_index=True)
stock_zh_a_spot_em_df.dropna(inplace=True)
stock_zh_a_spot_em_df.reset_index(inplace=True, drop=True)


if __name__ == "__main__":
    stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
    print(stock_zh_a_spot_em_df)
