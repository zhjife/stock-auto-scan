import akshare as ak
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from ta.trend import MACD
from ta.momentum import StochasticOscillator
from datetime import datetime
import os

# =====================
# 参数区
# =====================
MIN_DAYS = 120
VOL_RATIO = 1.5   # 放量倍数
SAVE_DIR = "K线图"
os.makedirs(SAVE_DIR, exist_ok=True)

# =====================
# 技术指标计算
# =====================
def add_indicators(df):
    df = df.copy()

    # 均线
    for ma in [5, 10, 20, 60]:
        df[f"MA{ma}"] = df["close"].rolling(ma).mean()

    # MACD
    macd = MACD(df["close"])
    df["DIF"] = macd.macd()
    df["DEA"] = macd.macd_signal()

    # KDJ
    kdj = StochasticOscillator(
        high=df["high"], low=df["low"], close=df["close"]
    )
    df["K"] = kdj.stoch()
    df["D"] = kdj.stoch_signal()

    # 成交量均值
    df["VOL_MA5"] = df["volume"].rolling(5).mean()

    return df

# =====================
# 金叉判断
# =====================
def is_macd_golden_cross(df):
    return df["DIF"].iloc[-2] < df["DEA"].iloc[-2] and df["DIF"].iloc[-1] > df["DEA"].iloc[-1]

def is_kdj_golden_cross(df):
    return df["K"].iloc[-2] < df["D"].iloc[-2] and df["K"].iloc[-1] > df["D"].iloc[-1]

# =====================
# 均线多头
# =====================
def is_ma_bullish(df):
    last = df.iloc[-1]
    return last["MA5"] > last["MA10"] > last["MA20"] > last["MA60"]

# =====================
# 放量
# =====================
def is_volume_expanded(df):
    return df["volume"].iloc[-1] > VOL_RATIO * df["VOL_MA5"].iloc[-1]

# =====================
# 画 K 线
# =====================
def plot_kline(df, code, name, period):
    plt.figure(figsize=(10, 5))
    plt.plot(df["close"], label="Close")
    plt.plot(df["MA5"], label="MA5")
    plt.plot(df["MA10"], label="MA10")
    plt.plot(df["MA20"], label="MA20")
    plt.plot(df["MA60"], label="MA60")
    plt.title(f"{code} {name} {period}")
    plt.legend()
    path = f"{SAVE_DIR}/{code}_{name}_{period}.png"
    plt.savefig(path)
    plt.close()

# =====================
# 主逻辑
# =====================
def main():
    print("开始选股...")

    stock_list = ak.stock_info_a_code_name()
    results = []

    for _, row in stock_list.iterrows():
        code = row["code"]
        name = row["name"]

        # 只要主板
        if not (code.startswith("60") or code.startswith("00")):
            continue

        try:
            df = ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date="20220101",
                adjust="qfq"
            )
        except:
            continue

        if df is None or len(df) < MIN_DAYS:
            continue

        df.rename(columns={
            "开盘": "open",
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "成交量": "volume"
        }, inplace=True)

        df = add_indicators(df)

        # 条件组合
        if (
            is_macd_golden_cross(df)
            and is_kdj_golden_cross(df)
            and is_ma_bullish(df)
            and is_volume_expanded(df)
        ):
            results.append([code, name])

            # 日线图
            plot_kline(df, code, name, "日线")

            # 周线
            df_week = df.resample("W", on="日期").last().dropna()
            plot_kline(df_week, code, name, "周线")

    # 输出结果
    result_df = pd.DataFrame(results, columns=["代码", "名称"])
    today = datetime.now().strftime("%Y%m%d")
    result_df.to_excel(f"选股结果_{today}.xlsx", index=False)

    print("完成，选中股票数：", len(result_df))

if __name__ == "__main__":
    main()
