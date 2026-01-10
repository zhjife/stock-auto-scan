import akshare as ak
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from ta.trend import MACD
from ta.momentum import StochasticOscillator
from datetime import datetime, timedelta
import os
import urllib.request
import traceback
import time  # 用于延时

# --- 1. 基础配置 ---
plt.switch_backend("Agg")
SAVE_DIR = "KLine_Charts"
os.makedirs(SAVE_DIR, exist_ok=True)

# 创建占位文件
with open(os.path.join(SAVE_DIR, "init.txt"), "w") as f:
    f.write("Folder initialized.")

MIN_DAYS = 60  # 最少需要的数据天数
VOL_RATIO = 1.5

# --- 2. 字体配置 (静默模式) ---
def config_font():
    font_path = "SimHei.ttf"
    try:
        if not os.path.exists(font_path):
            # 备用：如果Github下载慢，这里不强求，直接返回None用英文
            url = "https://github.com/StellarCN/scp_zh/raw/master/fonts/SimHei.ttf"
            # 设置超时时间为 5 秒，下载不下来就算了
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

# --- 3. 网络重试函数 (关键修改) ---
def get_data_with_retry(code, start_date):
    """尝试获取数据，如果失败自动重试3次"""
    for i in range(3):  # 最多尝试3次
        try:
            df = ak.stock_zh_a_hist(
                symbol=code, 
                period="daily", 
                start_date=start_date, 
                adjust="qfq"
            )
            return df
        except Exception as e:
            if i == 2: # 最后一次也失败
                # print(f"{code} 获取失败: {e}")
                return None
            time.sleep(2) # 休息2秒再重试
    return None

# --- 4. 指标计算 ---
def add_indicators(df):
    df["MA5"] = df["close"].rolling(5).mean()
    df["MA10"] = df["close"].rolling(10).mean()
    df["MA20"] = df["close"].rolling(20).mean()
    df["MA60"] = df["close"].rolling(60).mean()
    
    macd = MACD(df["close"], window_slow=26, window_fast=12, window_sign=9)
    df["DIF"] = macd.macd()
    df["DEA"] = macd.macd_signal()
    
    kdj = StochasticOscillator(df["high"], df["low"], df["close"], window=9, smooth_window=3)
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
        
        safe_name = name if my_font else "Stock"
        if my_font:
            plt.title(f"{code} {safe_name} {tag}", fontproperties=my_font)
        else:
            plt.title(f"{code} {tag}")
            
        plt.tight_layout()
        plt.savefig(f"{SAVE_DIR}/{code}_{tag}.png")
        plt.close()
    except:
        pass

# --- 5. 主程序 ---
def main():
    print("程序启动...")
    result = []
    
    try:
        print("正在获取股票列表...")
        stock_list = ak.stock_info_a_code_name()
        targets = stock_list[stock_list["code"].str.startswith(("60", "00"))]
        
        # --- 测试开关 ---
        # 建议先跑前30个测试网络，成功后再把下面这行删掉
        targets = targets.head(30) 
        # ---------------
        
        # 动态设置开始时间：只取最近180天的数据，减少流量，提高成功率
        start_dt = (datetime.now() - timedelta(days=200)).strftime("%Y%m%d")
        
        print(f"待扫描: {len(targets)} 只, 开始日期: {start_dt}")

        for _, s in targets.iterrows():
            code = s["code"]
            name = s["name"]
            
            # 使用带重试机制的获取函数
            df = get_data_with_retry(code, start_dt)
            
            if df is None or df.empty or len(df) < MIN_DAYS:
                continue
            
            # 数据清洗
            try:
                df.rename(columns={"日期":"date","开盘":"open","收盘":"close","最高":"high","最低":"low","成交量":"volume"}, inplace=True)
                df["date"] = pd.to_datetime(df["date"])
                df.set_index("date", inplace=True)
                
                df = add_indicators(df)
                
                if check_conditions(df):
                    print(f"选中: {code}")
                    result.append([code, name])
                    plot_kline(df.tail(100), code, name, "Daily")
            except:
                continue
                
            # 【重要】每跑完一只，暂停 0.5 秒，防止被封 IP
            time.sleep(0.5)

    except Exception as e:
        print(f"主程序报错: {e}")

    # 保存结果
    dt_str = datetime.now().strftime("%Y%m%d")
    
    # 无论有无结果，都生成 Excel，避免 Actions 报错
    if len(result) > 0:
        pd.DataFrame(result, columns=["代码", "名称"]).to_excel(f"Result_{dt_str}.xlsx", index=False)
    else:
        pd.DataFrame([["无", "无符合条件或网络错误"]], columns=["代码", "状态"]).to_excel(f"Empty_Result_{dt_str}.xlsx", index=False)

if __name__ == "__main__":
    main()
