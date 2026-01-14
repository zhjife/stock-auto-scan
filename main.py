import akshare as ak
import pandas as pd
import numpy as np
from ta.trend import MACD
from ta.momentum import StochasticOscillator, RSIIndicator
from ta.volatility import BollingerBands
from ta.volume import OnBalanceVolumeIndicator, ChaikinMoneyFlowIndicator
from datetime import datetime, timedelta
import os
import time
import sys
import traceback

# --- 1. 环境初始化 ---
current_dir = os.getcwd()
sys.path.append(current_dir)
HISTORY_FILE = "stock_selection_history.csv" # 历史记录文件

# --- 2. 历史记录管理 (核心新功能) ---
def load_history():
    """加载历史选股记录，用于比对是否连续出现"""
    if os.path.exists(HISTORY_FILE):
        try:
            df = pd.read_csv(HISTORY_FILE, dtype={"code": str})
            return df
        except:
            return pd.DataFrame(columns=["date", "code"])
    else:
        return pd.DataFrame(columns=["date", "code"])

def append_history(new_results, date_str):
    """将今日结果追加到历史文件"""
    if not new_results: return
    
    # 构造今日的记录 DataFrame
    new_df = pd.DataFrame(new_results)[["代码"]]
    new_df.columns = ["code"]
    new_df["date"] = date_str
    
    # 加载旧记录
    if os.path.exists(HISTORY_FILE):
        old_df = pd.read_csv(HISTORY_FILE, dtype={"code": str})
        # 删除今日已有的旧记录(防止同一天运行多次导致重复)
        old_df = old_df[old_df["date"] != date_str]
        final_df = pd.concat([old_df, new_df], ignore_index=True)
    else:
        final_df = new_df
        
    final_df.to_csv(HISTORY_FILE, index=False)
    print(f"✅ 选股记录已更新至: {HISTORY_FILE}")

# --- 3. 获取股票列表 ---
def get_targets_robust():
    print(">>> 开始获取股票列表...")
    try:
        df = ak.stock_zh_a_spot_em()
        df = df[["代码", "名称"]]
        df.columns = ["code", "name"]
        targets = df[df["code"].str.startswith(("60", "00"))]
        return targets, "方案A-东财(全量)"
    except:
        try:
            df = ak.stock_zh_a_spot()
            df = df[["symbol", "name"]]
            df.columns = ["code", "name"]
            targets = df[df["code"].str.startswith(("sh60", "sz00"))]
            targets["code"] = targets["code"].str.replace("sh", "").str.replace("sz", "")
            return targets, "方案B-新浪(全量)"
        except:
            manual_list = [
                ["600519", "贵州茅台"], ["300750", "宁德时代"], ["002594", "比亚迪"], 
                ["601318", "中国平安"], ["600036", "招商银行"]
            ]
            return pd.DataFrame(manual_list, columns=["code", "name"]), "方案C-离线(保底)"

# --- 4. 获取热点板块 ---
def get_hot_stock_pool():
    print(">>> 正在扫描市场热点...")
    hot_codes = set()
    try:
        df_ind = ak.stock_board_industry_name_em().sort_values(by="涨跌幅", ascending=False).head(5)
        for board in df_ind['板块名称']:
            try:
                df = ak.stock_board_industry_cons_em(symbol=board)
                hot_codes.update(df['代码'].tolist())
            except: pass
            time.sleep(0.2)
        return hot_codes
    except:
        return None

# --- 5. 数据获取 ---
def get_data_with_retry(code, start_date):
    for i in range(3):
        try:
            df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start_date, adjust="qfq")
            if df is None or df.empty: raise ValueError("Empty")
            return df
        except: time.sleep(1)
    return None

# --- 6. 核心计算 (包含新逻辑) ---
def process_stock(df):
    if len(df) < 60: return None
    
    # 指标计算
    df["MA5"] = df["close"].rolling(5).mean()
    df["MA10"] = df["close"].rolling(10).mean()
    df["MA20"] = df["close"].rolling(20).mean()
    df["MA60"] = df["close"].rolling(60).mean()
    
    # 量比
    vol_ma5 = df["volume"].rolling(5).mean()
    vol_ratio = 0 if vol_ma5.iloc[-1] == 0 else round(df["volume"].iloc[-1] / vol_ma5.iloc[-1], 2)

    # MACD
    macd = MACD(df["close"])
    df["DIF"] = macd.macd()
    df["DEA"] = macd.macd_signal()
    df["MACD_Hist"] = macd.macd_diff()
    
    # KDJ & RSI & OBV
    kdj = StochasticOscillator(df["high"], df["low"], df["close"])
    df["K"] = kdj.stoch()
    df["D"] = kdj.stoch_signal()
    df["RSI"] = RSIIndicator(close=df["close"], window=14).rsi()
    obv_ind = OnBalanceVolumeIndicator(close=df["close"], volume=df["volume"])
    df["OBV"] = obv_ind.on_balance_volume()
    df["OBV_MA10"] = df["OBV"].rolling(10).mean()

    # CMF
    cmf_ind = ChaikinMoneyFlowIndicator(high=df["high"], low=df["low"], close=df["close"], volume=df["volume"], window=20)
    df["CMF"] = cmf_ind.chaikin_money_flow()

    # === 信号判定 ===
    curr = df.iloc[-1]
    prev = df.iloc[-2]
    
    # 1. 基础信号
    s_macd = (prev["DIF"] < prev["DEA"] and curr["DIF"] > curr["DEA"] and curr["MACD_Hist"] > prev["MACD_Hist"])
    is_near_gold = (curr["DIF"] < curr["DEA"]) and (curr["DEA"] - curr["DIF"] < 0.05) and (curr["DIF"] > prev["DIF"])
    s_kdj = (prev["K"] < prev["D"] and curr["K"] > curr["D"])
    s_ma_bull = (curr["MA5"] > curr["MA10"] > curr["MA20"] > curr["MA60"])

    # 2. 底背离
    is_divergence = False
    last_60_low_idx = df["low"].tail(60).idxmin()
    if last_60_low_idx != curr.name:
        if curr["close"] < df.loc[last_60_low_idx, "low"] * 1.05:
            if curr["DIF"] > df.loc[last_60_low_idx, "DIF"] + 0.1:
                is_divergence = True

    # 3. 筛选逻辑 (MACD组 或 趋势组)
    if not ((s_macd or is_divergence or is_near_gold) or (s_kdj and s_ma_bull)):
        return None

    # 4. 避坑过滤
    if curr["close"] < curr["BOLL_Mid"]: return None
    if curr["OBV"] < curr["OBV_MA10"]: return None
    if curr["RSI"] > 80: return None

    # ==================================================
    # 🆕 新增功能计算区域
    # ==================================================

    # A. CMF 趋势
    cmf_curr = curr["CMF"]
    cmf_prev = prev["CMF"]
    cmf_status = "平稳"
    if cmf_prev < 0 and cmf_curr > 0: cmf_status = "★资金转正"
    elif cmf_curr > cmf_prev and cmf_curr > 0.1: cmf_status = "流入加速"
    elif cmf_curr > cmf_prev and cmf_curr < 0: cmf_status = "流出减弱"

    # B. 3日涨跌幅 (计算今日收盘相对于3个交易日前收盘的幅度)
    pct_3d = 0.0
    try:
        close_3d_ago = df["close"].iloc[-4] # -1是今天, -2昨, -3前, -4大前
        pct_3d = round((curr["close"] - close_3d_ago) / close_3d_ago * 100, 2)
    except: pass

    # C. 生成操作说明 (Strategy Advice)
    advice = "观察"
    if cmf_status == "★资金转正" and s_macd:
        advice = "【积极买入】资金共振"
    elif is_divergence:
        advice = "【低吸潜伏】左侧抄底"
    elif s_macd:
        advice = "【右侧买点】金叉确认"
    elif s_kdj and s_ma_bull:
        advice = "【趋势跟随】持股/做T"
    elif is_near_gold:
        advice = "【预警观察】等待金叉"

    return {
        "close": curr["close"],
        "pct_3d": pct_3d,             # 新增：3日涨幅
        "advice": advice,             # 新增：操作建议
        "vol_ratio": vol_ratio,
        "cmf_curr": round(cmf_curr, 3),
        "cmf_prev": round(cmf_prev, 3),
        "cmf_trend": cmf_status,
        "macd_gold": "真金叉" if s_macd else "",
        "near_gold": "预警" if is_near_gold else "",
        "divergence": "底背离" if is_divergence else "",
        "obv_desc": "强力" if curr["OBV"] > curr["OBV_MA10"] * 1.01 else "温和",
        "ma_bull": "是" if s_ma_bull else "",
        "kdj_gold": "是" if s_kdj else ""
    }

# --- 7. 主程序 ---
def main():
    print("=== 精英选股 (历史回溯 + 操作建议版) ===")
    
    # 1. 加载历史记录
    history_df = load_history()
    today_str = datetime.now().strftime("%Y%m%d")
    
    try:
        # 2. 获取目标池
        base_targets, source_name = get_targets_robust()
        hot_pool = get_hot_stock_pool()
        if hot_pool and len(base_targets) > 100:
            targets = base_targets[base_targets["code"].isin(hot_pool)]
            source_tag = f"{source_name}+热点"
        else:
            targets = base_targets
            source_tag = source_name

        start_dt = (datetime.now() - timedelta(days=200)).strftime("%Y%m%d")
        result_data = []
        total = len(targets)
        print(f"扫描 {total} 只股票 (含历史连选检测)...")

        for i, s in targets.iterrows():
            code = s["code"]
            name = s["name"]
            if i % 20 == 0: print(f"进度: {i}/{total} ...")

            try:
                df = get_data_with_retry(code, start_dt)
                if df is None: continue
                
                # 数据预处理
                df.rename(columns={"日期":"date","开盘":"open","收盘":"close","最高":"high","最低":"low","成交量":"volume"}, inplace=True)
                df["date"] = pd.to_datetime(df["date"])
                df.set_index("date", inplace=True)

                res = process_stock(df)
                
                if res:
                    # === 重点标记逻辑 (不是同一天，但以前出现过) ===
                    # 检查该代码是否在历史记录中，且日期不是今天
                    past_records = history_df[
                        (history_df["code"] == code) & 
                        (history_df["date"] != today_str)
                    ]
                    
                    is_repeated = not past_records.empty
                    mark_status = "★连选牛股" if is_repeated else "首选"
                    
                    if is_repeated:
                        print(f"  >>> 捕捉到连选牛股: {code} {name} (历史曾出现)")

                    result_data.append({
                        "标记": mark_status,       # 新增
                        "代码": code,
                        "名称": name,
                        "操作建议": res["advice"],  # 新增
                        "3日涨跌%": res["pct_3d"],  # 新增
                        "现价": res["close"],
                        "CMF趋势": res["cmf_trend"],
                        "CMF今日": res["cmf_curr"],
                        "MACD金叉": res["macd_gold"],
                        "底背离": res["divergence"],
                        "即将金叉": res["near_gold"],
                        "量比": res["vol_ratio"],
                        "资金流": res["obv_desc"],
                        "KDJ金叉": res["kdj_gold"],
                        "数据源": source_tag
                    })
            except: continue
            time.sleep(0.05)

        # 3. 输出结果
        if result_data:
            # 更新历史文件
            append_history(result_data, today_str)
            
            # 排序：优先看连选的，然后看操作建议是积极买入的，最后看CMF
            cols = ["标记", "代码", "名称", "操作建议", "3日涨跌%", 
                    "CMF趋势", "CMF今日", "MACD金叉", "底背离", 
                    "即将金叉", "量比", "资金流", "KDJ金叉", "现价", "数据源"]
            
            df_res = pd.DataFrame(result_data, columns=cols)
            
            # 辅助排序列
            df_res["_rank"] = 0
            df_res.loc[df_res["标记"] == "★连选牛股", "_rank"] += 100
            df_res.loc[df_res["操作建议"].str.contains("积极"), "_rank"] += 50
            df_res.loc[df_res["CMF趋势"] == "★资金转正", "_rank"] += 30
            
            df_res = df_res.sort_values(by=["_rank", "CMF今日"], ascending=[False, False]).drop(columns=["_rank"])
            
            filename = f"最终选股_{today_str}.xlsx"
            df_res.to_excel(filename, index=False)
            print(f"\n✅ 选股完成！\n👉 结果已保存: {filename}\n👉 历史记录已更新: {HISTORY_FILE}")
            print("💡 提示：Excel第一列显示 '★连选牛股' 的是重点关注对象")
        else:
            print("无符合条件股票")

    except Exception:
        with open("ERROR_LOG.txt", "w") as f: f.write(traceback.format_exc())

if __name__ == "__main__":
    main()
