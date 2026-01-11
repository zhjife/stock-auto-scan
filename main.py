import akshare as ak
import pandas as pd
import numpy as np
from ta.trend import MACD
from ta.momentum import StochasticOscillator
from datetime import datetime, timedelta
import os
import time
import sys
import traceback

# --- 1. 环境初始化 ---
current_dir = os.getcwd()
sys.path.append(current_dir)

# --- 2. 核心功能：获取热点板块股票池 ---
def get_hot_stock_pool():
    """
    获取涨幅前8的行业板块 + 涨幅前8的概念板块
    返回这些板块下的所有股票代码集合 (去重)
    """
    print(">>> 正在扫描市场热点 (行业 & 概念 Top 8)...")
    hot_codes = set()
    hot_names = []

    try:
        # 1. 获取行业板块
        df_ind = ak.stock_board_industry_name_em()
        # 按涨跌幅排序，取前8
        top_ind = df_ind.sort_values(by="涨跌幅", ascending=False).head(8)
        print(f"🔥 热门行业: {top_ind['板块名称'].tolist()}")
        
        for _, row in top_ind.iterrows():
            board_name = row['板块名称']
            hot_names.append(board_name)
            # 获取板块内的股票
            try:
                df_members = ak.stock_board_industry_cons_em(symbol=board_name)
                hot_codes.update(df_members['代码'].tolist())
            except: continue
            time.sleep(0.5)

        # 2. 获取概念板块
        df_con = ak.stock_board_concept_name_em()
        top_con = df_con.sort_values(by="涨跌幅", ascending=False).head(8)
        print(f"🔥 热门概念: {top_con['板块名称'].tolist()}")

        for _, row in top_con.iterrows():
            board_name = row['板块名称']
            hot_names.append(board_name)
            try:
                df_members = ak.stock_board_concept_cons_em(symbol=board_name)
                hot_codes.update(df_members['代码'].tolist())
            except: continue
            time.sleep(0.5)
            
        print(f">>> 热点股票池构建完成，共包含 {len(hot_codes)} 只股票")
        return hot_codes

    except Exception as e:
        print(f"获取热点板块失败: {e}")
        print("降级策略：使用全量主板股票")
        return None

# --- 3. 获取个股列表 (带热点过滤) ---
def get_targets_with_filter():
    # 先获取所有主板股票
    try:
        df = ak.stock_zh_a_spot_em()
        df = df[["代码", "名称"]]
        df.columns = ["code", "name"]
        # 只选沪深主板
        all_main = df[df["code"].str.startswith(("60", "00"))]
    except:
        # 备用方案
        df = ak.stock_info_a_code_name()
        all_main = df[df["code"].str.startswith(("60", "00"))]

    # 获取热点池
    hot_pool = get_hot_stock_pool()
    
    if hot_pool:
        # 取交集：既在主板，又在热点板块里
        targets = all_main[all_main["code"].isin(hot_pool)]
        print(f"经过热点过滤，待扫描股票从 {len(all_main)} 减少到 {len(targets)}")
        return targets
    else:
        return all_main

# --- 4. 获取K线数据 ---
def get_data_with_retry(code, start_date):
    for i in range(3):
        try:
            df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start_date, adjust="qfq")
            if df is None or df.empty: raise ValueError("Empty")
            return df
        except:
            time.sleep(1)
    return None

# --- 5. 核心计算逻辑 (包含高级形态) ---
def process_stock(df):
    if len(df) < 60: return None
    
    # 基础指标
    df["MA5"] = df["close"].rolling(5).mean()
    df["MA10"] = df["close"].rolling(10).mean()
    df["MA20"] = df["close"].rolling(20).mean()
    df["MA60"] = df["close"].rolling(60).mean()
    
    # 量比 (5日均量)
    vol_ma5 = df["volume"].rolling(5).mean()
    vol_ratio = 0 if vol_ma5.iloc[-1] == 0 else round(df["volume"].iloc[-1] / vol_ma5.iloc[-1], 2)

    # MACD
    macd = MACD(df["close"], window_slow=26, window_fast=12, window_sign=9)
    df["DIF"] = macd.macd()
    df["DEA"] = macd.macd_signal()
    df["MACD_Hist"] = macd.macd_diff() # 红绿柱
    
    # KDJ
    kdj = StochasticOscillator(df["high"], df["low"], df["close"], window=9, smooth_window=3)
    df["K"] = kdj.stoch()
    df["D"] = kdj.stoch_signal()

    # --- 数据切片 ---
    curr = df.iloc[-1]
    prev = df.iloc[-2]
    if pd.isna(curr['MA60']): return None

    # ==========================
    # 高级判断逻辑
    # ==========================

    # 1. MACD 金叉 (含假金叉过滤)
    # 原始金叉: 昨天DIF<DEA, 今天DIF>DEA
    raw_macd_gold = (prev["DIF"] < prev["DEA"] and curr["DIF"] > curr["DEA"])
    is_real_gold = False
    
    if raw_macd_gold:
        # 过滤器：DIF 必须是向上的 (今天DIF > 昨天DIF) 且 红柱子出现
        if curr["DIF"] > prev["DIF"] and curr["MACD_Hist"] > 0:
            is_real_gold = True
    
    # 2. 接近金叉预警 (即将金叉)
    # 条件: DIF在DEA下方，但两者距离非常近 (比如相差 < 0.05)，且DIF在拐头向上
    is_near_gold = False
    diff_val = curr["DEA"] - curr["DIF"]
    if 0 < diff_val < 0.05 and curr["DIF"] > prev["DIF"]:
        is_near_gold = True

    # 3. 底背离 (Price Lower Low, MACD Higher Low)
    # 简化逻辑：比较最近60天最低价时刻的MACD 与 当前MACD
    is_divergence = False
    # 找到过去60天最低价的位置
    last_60 = df.tail(60)
    min_price_idx = last_60["low"].idxmin()
    
    # 如果最低价不是今天(给一点容错)，且现在的收盘价接近最低价，但MACD比最低价时要高
    if min_price_idx != curr.name: 
        min_price_macd = df.loc[min_price_idx, "DIF"]
        # 价格接近新低 (在最低价 5% 范围内)
        price_near_low = (curr["close"] - last_60["low"].min()) / last_60["low"].min() < 0.05
        # 现在的DIF 明显高于 最低价时的DIF
        macd_higher = curr["DIF"] > min_price_macd + 0.05
        
        if price_near_low and macd_higher:
            is_divergence = True

    # 4. 双底 (W底) 简单的形态判断
    # 逻辑：过去60天有两个明显的低点，且两个低点价格相近
    is_double_bottom = False
    # 将数据分成两段：最近20天，和20-60天前
    recent_period = df.iloc[-20:]
    past_period = df.iloc[-60:-20]
    
    recent_low = recent_period["low"].min()
    past_low = past_period["low"].min()
    
    # 两个低点差距不超过 3%
    if abs(recent_low - past_low) / past_low < 0.03:
        # 且中间有过反弹 (中间最高价必须高于低点 10%以上)
        mid_high = df.iloc[-60:]["high"].max()
        if mid_high > past_low * 1.1:
            # 且当前价格处于右底支撑位附近
            if curr["close"] < recent_low * 1.05:
                is_double_bottom = True

    # 其他基础指标
    s_kdj = (prev["K"] < prev["D"] and curr["K"] > curr["D"])
    s_ma = (curr["MA5"] > curr["MA10"] > curr["MA20"] > curr["MA60"])
    
    # 金山谷 (MA10上穿MA20, 且MA5在上方)
    s_valley = (prev["MA10"] < prev["MA20"] and curr["MA10"] > curr["MA20"] and curr["MA5"] > curr["MA10"])

    # 只要满足任意一个条件，就返回
    signals = [is_real_gold, s_kdj, s_ma, s_valley, is_near_gold, is_divergence, is_double_bottom]
    if any(signals):
        return {
            "close": curr["close"],
            "vol_ratio": vol_ratio,
            "macd_gold": "是" if is_real_gold else "",
            "kdj_gold": "是" if s_kdj else "",
            "ma_bull": "是" if s_ma else "",
            "gold_valley": "是" if s_valley else "",
            "near_gold": "预警" if is_near_gold else "",  # 新增
            "divergence": "底背离" if is_divergence else "", # 新增
            "double_bottom": "疑似双底" if is_double_bottom else "" # 新增
        }
    
    return None

# --- 6. 主程序 ---
def main():
    print("=== 高级选股启动 (热点+形态版) ===")
    
    pd.DataFrame([["Init", "OK"]]).to_excel("Init_Check.xlsx", index=False)
    
    try:
        # 获取带热点过滤的股票列表
        targets = get_targets_with_filter()
        
        # --- 测试开关 ---
        # targets = targets.head(50) 
        # ----------------
        
        start_dt = (datetime.now() - timedelta(days=200)).strftime("%Y%m%d")
        result_data = []
        
        total = len(targets)
        print(f"开始深度扫描 {total} 只股票...")

        for i, s in targets.iterrows():
            code = s["code"]
            name = s["name"]
            
            if i % 20 == 0:
                print(f"进度: {i}/{total} ...")

            try:
                df = get_data_with_retry(code, start_dt)
                if df is None: continue

                # 清洗
                df.rename(columns={"日期":"date","开盘":"open","收盘":"close","最高":"high","最低":"low","成交量":"volume"}, inplace=True)
                df["date"] = pd.to_datetime(df["date"])
                df.set_index("date", inplace=True)

                res = process_stock(df)
                
                if res:
                    # 只有满足某些强条件才打印，防止刷屏
                    if res['macd_gold'] or res['divergence'] or res['near_gold']:
                        print(f"  >>> 发现: {code} {name} (量比:{res['vol_ratio']})")
                    
                    result_data.append({
                        "代码": code,
                        "名称": name,
                        "现价": res["close"],
                        "量比": res["vol_ratio"],
                        "MACD金叉(真)": res["macd_gold"],
                        "KDJ金叉": res["kdj_gold"],
                        "即将金叉(预警)": res["near_gold"], # 新列
                        "底背离": res["divergence"],       # 新列
                        "双底形态": res["double_bottom"],  # 新列
                        "均线多头": res["ma_bull"],
                        "金山谷": res["gold_valley"]
                    })
                    
            except: continue
            time.sleep(0.05)

        # 保存
        dt_str = datetime.now().strftime("%Y%m%d")
        if result_data:
            cols = ["代码", "名称", "现价", "量比", "MACD金叉(真)", "即将金叉(预警)", "底背离", "双底形态", "KDJ金叉", "均线多头", "金山谷"]
            df_res = pd.DataFrame(result_data, columns=cols)
            # 优先显示底背离和真金叉的
            df_res = df_res.sort_values(by=["底背离", "MACD金叉(真)", "量比"], ascending=False)
            
            filename = f"热点选股增强版_{dt_str}.xlsx"
            df_res.to_excel(filename, index=False)
            print(f"完成！结果已保存为: {filename}")
        else:
            pd.DataFrame([["无"]], columns=["Info"]).to_excel(f"无结果_{dt_str}.xlsx")

    except Exception:
        err = traceback.format_exc()
        print(f"FATAL ERROR: {err}")
        with open("FATAL_ERROR.txt", "w") as f:
            f.write(err)

if __name__ == "__main__":
    main()
