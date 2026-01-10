import akshare as ak
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from ta.trend import MACD
from ta.momentum import StochasticOscillator
from datetime import datetime, timedelta
import os
import urllib.request
import time
import sys

# 1. 基础配置
# PyInstaller 打包后的资源路径处理
if getattr(sys, 'frozen', False):
    application_path = os.path.dirname(sys.executable)
else:
    application_path = os.path.dirname(os.path.abspath(__file__))

# 切换工作目录到 exe 所在目录，确保文件保存在正确位置
os.chdir(application_path)

plt.switch_backend("Agg")
SAVE_DIR = "KLine_Charts"
os.makedirs(SAVE_DIR, exist_ok=True)

# 创建占位文件
with open(os.path.join(SAVE_DIR, "init.txt"), "w") as f:
    f.write("Folder initialized.")

MIN_DAYS = 60
VOL_RATIO = 1.5

# 2. 字体配置 (静默模式)
def config_font():
    font_path = "SimHei.ttf"
    try:
        if not os.path.exists(font_path):
            url = "https://github.com/StellarCN/scp_zh/raw/master/fonts/SimHei.ttf"
            try:
                urllib.request.urlretrieve(url, font_path)
            except:
                pass 
        if os.path.exists(font_path):
            return fm.FontProperties(fname=font_path)
    except:
        pass
    return None

my_font = config_font()

# 3. 网络重试函数
def get_data_with_retry(code, start_date):
    for i in range(3):
        try:
            df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start_date, adjust="qfq")
            return df
        except:
            if i == 2: return None
            time.sleep(2)
    return None

# 4. 指标计算
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
    curr = df.iloc[-1]
    prev = df.iloc[-2]
    if pd.isna(curr['MA60']) or pd.isna(curr['VOL_MA5']): return False
    
    macd_gold = prev["DIF"] < prev["DEA"] and curr["DIF"] > curr["DEA"]
    kdj_gold = prev["K"] < prev["D"] and curr["K"] > curr["D"]
    ma_bull = curr["MA5"] > curr["MA10"] > curr["MA20"] > curr["MA60"]
    vol_ok = curr["volume"] > VOL_RATIO * curr["VOL_MA5"]
    return macd_gold and kdj_gold and ma_bull and vol_ok

def plot_kline(df, code, name, tag):
    try:
        plt.figure(figsize=(10, 5))
        plt.plot(df.index, df["close"], label="Close")
        for ma in [5, 10, 20, 60]:
            if f"MA{ma}" in df.columns:
                plt.plot(df.index, df[f"MA{ma}"], label=f"MA{ma}")
        plt.legend()
        title = f"{code} {name if my_font else 'Stock'} {tag}"
        if my_font: plt.title(title, fontproperties=my_font)
        else: plt.title(title)
        plt.tight_layout()
        plt.savefig(f"{SAVE_DIR}/{code}_{tag}.png")
        plt.close()
    except: pass

def main():
    print("Code running on local PC via GitHub Actions...")
    result = []
    try:
        stock_list = ak.stock_info_a_code_name()
        targets = stock_list[stock_list["code"].str.startswith(("60", "00"))]
        
        # --- 测试开关: 首次建议保留，测试成功后删除这行 ---
        targets = targets.head(20) 
        # ---------------------------------------------
        
        start_dt = (datetime.now() - timedelta(days=200)).strftime("%Y%m%d")
        print(f"Scanning {len(targets)} stocks...")

        for _, s in targets.iterrows():
            code = s["code"]
            name = s["name"]
            df = get_data_with_retry(code, start_dt)
            if df is None or len(df) < MIN_DAYS: continue
            
            try:
                df.rename(columns={"日期":"date","开盘":"open","收盘":"close","最高":"high","最低":"low","成交量":"volume"}, inplace=True)
                df["date"] = pd.to_datetime(df["date"])
                df.set_index("date", inplace=True)
                df = add_indicators(df)
                if check_conditions(df):
                    result.append([code, name])
                    plot_kline(df.tail(100), code, name, "Daily")
            except: continue
            time.sleep(0.5)

    except Exception as e:
        print(f"Error: {e}")

    dt_str = datetime.now().strftime("%Y%m%d")
    pd.DataFrame(result if result else [["None", "No Data"]], columns=["Code", "Name"]).to_excel(f"Result_{dt_str}.xlsx", index=False)

if __name__ == "__main__":
    main()
