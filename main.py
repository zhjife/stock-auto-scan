import akshare as ak
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from ta.trend import MACD
from ta.momentum import StochasticOscillator
from datetime import datetime
import os
import urllib.request
from tqdm import tqdm

plt.switch_backend("Agg")
SAVE_DIR = "KLine_Charts"
os.makedirs(SAVE_DIR, exist_ok=True)

MIN_DAYS = 120
VOL_RATIO = 1.5

def config_font():
    font_path = "SimHei.ttf"
    if not os.path.exists(font_path):
        print("下载中文字体...")
        url = "https://github.com/StellarCN/scp_zh/raw/master/fonts/SimHei.ttf"
        try:
            urllib.request.urlretrieve(url, font_path)
        except:
            return None
    if os.path.exists(font_path):
        return fm.FontProperties(fname=font_path)
    return None

my_font = config_font()

def add_indicators(df):
    df["MA5"] = df["close"].rolling(5).mean()
    df["MA10"] = df["close"].rolling(10).mean()
    df["MA20"] = df["close"].rolling(20).mean()
    df["MA60"] = df["close"].rolling(60).mean()
    
    macd = MACD(df["close"])
    df["DIF"] = macd.macd()
    df["DEA"] = macd.macd_signal()
    
    kdj = StochasticOscillator(df["high"], df["low"], df["close"])
    df["K"] = kdj.stoch()
    df["D"] = kdj.stoch_signal()
    
    df["VOL_MA5"] = df["volume"].rolling(5).mean()
    return df

def check_conditions(df):
    if len(df) < 60: return False
    # 1. MACD 金叉 (最后一天 DIF > DEA，前一天 DIF < DEA)
    macd_gold = df["DIF"].iloc[-2] < df["DEA"].iloc[-2] and df["DIF"].iloc[-1] > df["DEA"].iloc[-1]
    # 2. KDJ 金叉
    kdj_gold = df["K"].iloc[-2] < df["D"].iloc[-2] and df["K"].iloc[-1] > df["D"].iloc[-1]
    # 3. 均线多头
    ma_bull = df["MA5"].iloc[-1] > df["MA10"].iloc[-1] > df["MA20"].iloc[-1] > df["MA60"].iloc[-1]
    # 4. 放量
    vol_ok = df["volume"].iloc[-1] > VOL_RATIO * df["VOL_MA5"].iloc[-1]
    
    return macd_gold and kdj_gold and ma_bull and vol_ok

def plot_kline(df, code, name, tag):
    plt.figure(figsize=(10, 5))
    plt.plot(df.index, df["close"], label="Close")
    for ma in [5, 10, 20, 60]:
        if f"MA{ma}" in df.columns:
            plt.plot(df.index, df[f"MA{ma}"], label=f"MA{ma}")
    plt.legend()
    title = f"{code} {name} {tag}"
    if my_font:
        plt.title(title, fontproperties=my_font)
    else:
        plt.title(f"{code} {tag}")
    plt.tight_layout()
    plt.savefig(f"{SAVE_DIR}/{code}_{tag}.png")
    plt.close()

def main():
    print("获取股票列表...")
    try:
        # 获取所有A股
        stock_list = ak.stock_info_a_code_name()
        # 筛选 60 和 00 开头
        targets = stock_list[stock_list["code"].str.startswith(("60", "00"))]
        # 【重要】为了测试，先只跑前 50 只。如果想跑全量，请删除下面这行 .head(50)
        # targets = targets.head(50) 
    except:
        print("列表获取失败")
        return

    print(f"开始扫描 {len(targets)} 只股票...")
    result = []

    for _, s in tqdm(targets.iterrows(), total=len(targets)):
        code = s["code"]
        name = s["name"]
        try:
            df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date="20230101", adjust="qfq")
            if df is None or len(df) < MIN_DAYS: continue
            
            df.rename(columns={"日期":"date","开盘":"open","收盘":"close","最高":"high","最低":"low","成交量":"volume"}, inplace=True)
            df["date"] = pd.to_datetime(df["date"])
            df.set_index("date", inplace=True)
            
            df = add_indicators(df)
            
            if check_conditions(df):
                result.append([code, name])
                plot_kline(df.tail(100), code, name, "Daily")
        except:
            continue

    # 保存结果
    dt = datetime.now().strftime("%Y%m%d")
    df_res = pd.DataFrame(result, columns=["代码", "名称"])
    df_res.to_excel(f"Result_{dt}.xlsx", index=False)
    print(f"完成！选中 {len(df_res)} 只。")

if __name__ == "__main__":
    main()
