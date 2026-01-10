import akshare as ak
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from ta.trend import MACD
from ta.momentum import StochasticOscillator
from datetime import datetime
import os
import urllib.request
import traceback  # 用于捕获错误详细信息

# --- 1. 基础配置 ---
plt.switch_backend("Agg")
SAVE_DIR = "KLine_Charts"
# 强制创建目录，防止上传时找不到目录报错
os.makedirs(SAVE_DIR, exist_ok=True)

# 无论如何先创建一个占位文件，防止上传报错
with open(os.path.join(SAVE_DIR, "init.txt"), "w") as f:
    f.write("Folder initialized.")

MIN_DAYS = 120
VOL_RATIO = 1.5

# --- 2. 字体安全加载 (防崩溃版) ---
def config_font():
    font_path = "SimHei.ttf"
    try:
        if not os.path.exists(font_path):
            print("正在尝试下载字体...")
            # 备用下载链接，如果失败则捕获异常
            url = "https://github.com/StellarCN/scp_zh/raw/master/fonts/SimHei.ttf"
            urllib.request.urlretrieve(url, font_path)
        
        if os.path.exists(font_path):
            return fm.FontProperties(fname=font_path)
    except Exception as e:
        print(f"字体配置失败，将使用默认字体: {e}")
    return None

my_font = config_font()

# --- 3. 核心指标函数 ---
def add_indicators(df):
    df["MA5"] = df["close"].rolling(5).mean()
    df["MA10"] = df["close"].rolling(10).mean()
    df["MA20"] = df["close"].rolling(20).mean()
    df["MA60"] = df["close"].rolling(60).mean()
    
    # 解决 MACD 计算警告
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
    
    # 获取最后两行数据
    curr = df.iloc[-1]
    prev = df.iloc[-2]
    
    # 确保没有空值
    if pd.isna(curr['MA60']) or pd.isna(curr['VOL_MA5']): return False

    # 1. MACD 金叉
    macd_gold = prev["DIF"] < prev["DEA"] and curr["DIF"] > curr["DEA"]
    # 2. KDJ 金叉
    kdj_gold = prev["K"] < prev["D"] and curr["K"] > curr["D"]
    # 3. 均线多头
    ma_bull = curr["MA5"] > curr["MA10"] > curr["MA20"] > curr["MA60"]
    # 4. 放量
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
        
        # 标题处理
        safe_name = name if my_font else "Stock"
        title_str = f"{code} {safe_name} {tag}"
        
        if my_font:
            plt.title(title_str, fontproperties=my_font)
        else:
            plt.title(f"{code} {tag}")
            
        plt.tight_layout()
        plt.savefig(f"{SAVE_DIR}/{code}_{tag}.png")
        plt.close()
    except Exception as e:
        print(f"绘图失败 {code}: {e}")

# --- 4. 主程序 (带错误捕获) ---
def main():
    print("程序启动...")
    result = []
    error_log = []
    
    try:
        print("正在获取股票列表...")
        stock_list = ak.stock_info_a_code_name()
        # 筛选 60 和 00 开头
        targets = stock_list[stock_list["code"].str.startswith(("60", "00"))]
        
        # 【测试模式】为了确保能跑通，先只跑前 30 只
        # 如果你确定没问题了，再把下面这行删掉
        print("注意：当前为测试模式，仅扫描前30只股票")
        targets = targets.head(30) 
        
        print(f"待扫描数量: {len(targets)}")

        for _, s in targets.iterrows():
            code = s["code"]
            name = s["name"]
            
            try:
                # 获取数据
                df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date="20230601", adjust="qfq")
                
                if df is None or df.empty or len(df) < MIN_DAYS:
                    continue
                
                # 统一列名
                df.rename(columns={"日期":"date","开盘":"open","收盘":"close","最高":"high","最低":"low","成交量":"volume"}, inplace=True)
                df["date"] = pd.to_datetime(df["date"])
                df.set_index("date", inplace=True)
                
                df = add_indicators(df)
                
                if check_conditions(df):
                    print(f"选中: {code} {name}")
                    result.append([code, name])
                    plot_kline(df.tail(100), code, name, "Daily")
                    
            except Exception as inner_e:
                # 单只股票出错不影响整体
                continue

    except Exception as e:
        # 全局崩溃捕获
        err_msg = traceback.format_exc()
        print(f"严重错误: {err_msg}")
        error_log.append(["System Error", err_msg])

    # --- 5. 保存结果 (关键：无论是否有结果都保存) ---
    dt = datetime.now().strftime("%Y%m%d")
    filename = f"Result_{dt}.xlsx"
    
    if len(result) > 0:
        df_res = pd.DataFrame(result, columns=["代码", "名称"])
        df_res.to_excel(filename, index=False)
        print(f"成功保存 {len(result)} 条结果")
    elif len(error_log) > 0:
        df_err = pd.DataFrame(error_log, columns=["错误类型", "详情"])
        df_err.to_excel("ERROR_REPORT.xlsx", index=False)
        print("已生成错误报告")
    else:
        # 如果既没选中也没报错（比如条件太严），生成一个空表
        df_empty = pd.DataFrame([["无", "没有符合条件的股票"]], columns=["代码", "状态"])
        df_empty.to_excel(filename, index=False)
        print("未选中股票，已生成空结果表")

if __name__ == "__main__":
    main()
