import akshare as ak
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from ta.trend import MACD
from ta.momentum import StochasticOscillator
from datetime import datetime
import os

plt.switch_backend("Agg")  # GitHub Actions 必须

SAVE_DIR = "K线图"
os.makedirs(SAVE_DIR, exist_ok=True)

MIN_DAYS = 120
VOL_RATIO = 1.5


def add_indicators(df):
    for ma in [5, 10, 20, 60]:
        df[f"MA{ma}"] = df["close"].rolling(ma).mean()

    macd = MACD(df["close"])
    df["DIF"] = macd.macd()
    df["DEA"] = macd.macd_signal()

    kdj = StochasticOscillator(df["high"], df["low"], df["close"])
    df["K"] = kdj.stoch()
    df["D"] = kdj.stoch_signal()

    df["VOL_MA5"] = df["volume"].rolling(5).mean()
    return df


def is_macd_golden(df):
    return df["DIF"].iloc[-2] < df["DEA"].iloc[-2] and df["DIF"].iloc[-1] > df["DEA"].iloc[-1]


def is_kdj_golden(df):
    return df["K"].iloc[-2] < df["D"].iloc[-2] and df["K"].iloc[-1] > df["D"].iloc[-1]


def is_ma_bullish(df):
    r = df.iloc[-1]
    return r["MA5"] > r["MA10"] > r["MA20"] > r["MA60"]


def is_volume_ok(df):
    return df["volume"].iloc[-1] > VOL_RATIO * df["VOL_MA5"].iloc[-1]


def plot_kline(df, code, name, tag):
    plt.figure(figsize=(10, 4))
    plt.plot(df["close"], label="Close")
    for ma in [5, 10, 20, 60]:
        plt.plot(df[f"MA{ma}"], label=f"MA{ma}")
    plt.legend()
    plt.title(f"{code} {name} {tag}")
    plt.tight_layout()
    plt.savefig(f"{SAVE_DIR}/{code}_{name}_{tag}.png")
    plt.close()


def main():
    stock_list = ak.stock_info_a_code_name()
    result = []

    for _, s in stock_list.iterrows():
        code = s["code"]
        name = s["name"]

        if not (code.startswith("60") or code.startswith("00")):
            continue

        try:
            df = ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date="20220101",
                adjust="qfq"
            )

            if df is None or len(df) < MIN_DAYS:
                continue

            # 字段统一
            df.rename(columns={
                "日期": "date",
                "开盘": "open",
                "收盘": "close",
                "最高": "high",
                "最低": "low",
                "成交量": "volume"
            }, inplace=True)

            df["date"] = pd.to_datetime(df["date"])
            df.set_index("date", inplace=True)

            df = add_indicators(df)

            if (
                is_macd_golden(df)
                and is_kdj_golden(df)
                and is_ma_bullish(df)
                and is_volume_ok(df)
            ):
                result.append([code, name])

                plot_kline(df, code, name, "日线")

                df_week = df.resample("W").last().dropna()
                plot_kline(df_week, code, name, "周线")

        except Exception as e:
            print(f"{code} 出错，跳过：{e}")
            continue

    out = pd.DataFrame(result, columns=["代码", "名称"])
    today = datetime.now().strftime("%Y%m%d")
    out.to_excel(f"选股结果_{today}.xlsx", index=False)

    print("完成，选中：", len(out))


if __name__ == "__main__":
    main()
