import akshare as ak
import pandas as pd
import numpy as np
from ta.trend import MACD
from ta.momentum import StochasticOscillator, RSIIndicator
from ta.volatility import BollingerBands
from ta.volume import OnBalanceVolumeIndicator
from datetime import datetime, timedelta
import os
import time
import sys
import traceback

# --- 1. 环境初始化 ---
current_dir = os.getcwd()
sys.path.append(current_dir)

# --- 2. 获取股票列表 (方案A/B/C 铜墙铁壁版) ---
def get_targets_robust():
    print(">>> 开始获取股票列表...")
    
    # 方案 A: 东方财富接口
    try:
        print("尝试方案 A (东方财富)...")
        df = ak.stock_zh_a_spot_em()
        df = df[["代码", "名称"]]
        df.columns = ["code", "name"]
        targets = df[df["code"].str.startswith(("60", "00"))]
        print(f"✅ 方案 A 成功！获取到 {len(targets)} 只股票")
        return targets, "方案A-东财(全量)"
    except Exception as e:
        print(f"❌ 方案 A 失败: {e}")

    # 方案 B: 新浪财经接口
    try:
        print("尝试方案 B (新浪财经)...")
        df = ak.stock_zh_a_spot()
        df = df[["symbol", "name"]]
        df.columns = ["code", "name"]
        targets = df[df["code"].str.startswith(("sh60", "sz00"))]
        targets["code"] = targets["code"].str.replace("sh", "").str.replace("sz", "")
        print(f"✅ 方案 B 成功！获取到 {len(targets)} 只股票")
        return targets, "方案B-新浪(全量)"
    except Exception as e:
        print(f"❌ 方案 B 失败: {e}")

    # 方案 C: 离线保底模式
    print(">>> ⚠️ 警告：在线接口全部失败，切换到【离线精选模式】")
    manual_list = [
        ["600519", "贵州茅台"], ["000858", "五粮液"], ["600887", "伊利股份"], ["601888", "中国中免"],
        ["002594", "比亚迪"], ["300750", "宁德时代"], ["601012", "隆基绿能"], ["002475", "立讯精密"],
        ["002415", "海康威视"], ["000725", "京东方A"], ["600438", "通威股份"],
        ["601318", "中国平安"], ["600036", "招商银行"], ["600030", "中信证券"], ["000001", "平安银行"],
        ["600276", "恒瑞医药"], ["300760", "迈瑞医疗"], ["603259", "药明康德"],
        ["601668", "中国建筑"], ["600900", "长江电力"], ["600009", "上海机场"], ["000333", "美的集团"],
        ["000651", "格力电器"], ["601857", "中国石油"], ["600028", "中国石化"], ["601088", "中国神华"]
    ]
    return pd.DataFrame(manual_list, columns=["code", "name"]), "方案C-离线(保底)"

# --- 3. 获取热点板块 ---
def get_hot_stock_pool():
    print(">>> 正在扫描市场热点 (行业 & 概念 Top 8)...")
    hot_codes = set()
    try:
        # 行业
        df_ind = ak.stock_board_industry_name_em()
        top_ind = df_ind.sort_values(by="涨跌幅", ascending=False).head(8)
        print(f"🔥 热门行业: {top_ind['板块名称'].tolist()}")
        for board in top_ind['板块名称']:
            try:
                df = ak.stock_board_industry_cons_em(symbol=board)
                hot_codes.update(df['代码'].tolist())
            except: pass
            time.sleep(0.3)

        # 概念
        df_con = ak.stock_board_concept_name_em()
        top_con = df_con.sort_values(by="涨跌幅", ascending=False).head(8)
        print(f"🔥 热门概念: {top_con['板块名称'].tolist()}")
        for board in top_con['板块名称']:
            try:
                df = ak.stock_board_concept_cons_em(symbol=board)
                hot_codes.update(df['代码'].tolist())
            except: pass
            time.sleep(0.3)
            
        print(f">>> 热点池共 {len(hot_codes)} 只")
        return hot_codes
    except:
        print("热点获取失败，将使用基础列表")
        return None

# --- 4. 数据获取 ---
def get_data_with_retry(code, start_date):
    for i in range(3):
        try:
            df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start_date, adjust="qfq")
            if df is None or df.empty: raise ValueError("Empty")
            return df
        except:
            time.sleep(1)
    return None

# --- 5. 核心计算 (包含避坑过滤器) ---
def process_stock(df):
    if len(df) < 60: return None
    
    # === 基础指标 ===
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
    
    # KDJ
    kdj = StochasticOscillator(df["high"], df["low"], df["close"])
    df["K"] = kdj.stoch()
    df["D"] = kdj.stoch_signal()

    # 布林带 (BOLL)
    boll = BollingerBands(close=df["close"], window=20, window_dev=2)
    df["BOLL_High"] = boll.bollinger_hband()
    df["BOLL_Mid"] = boll.bollinger_mavg()
    
    # RSI
    rsi_ind = RSIIndicator(close=df["close"], window=14)
    df["RSI"] = rsi_ind.rsi()
    
    # OBV
    obv_ind = OnBalanceVolumeIndicator(close=df["close"], volume=df["volume"])
    df["OBV"] = obv_ind.on_balance_volume()
    df["OBV_MA10"] = df["OBV"].rolling(10).mean()

    # === 信号判定 ===
    curr = df.iloc[-1]
    prev = df.iloc[-2]
    if pd.isna(curr['MA60']): return None

    # 1. 信号搜集
    s_macd = (prev["DIF"] < prev["DEA"] and curr["DIF"] > curr["DEA"] and curr["MACD_Hist"] > prev["MACD_Hist"])
    s_kdj = (prev["K"] < prev["D"] and curr["K"] > curr["D"])
    s_ma_bull = (curr["MA5"] > curr["MA10"] > curr["MA20"] > curr["MA60"])
    is_near_gold = (curr["DIF"] < curr["DEA"]) and (curr["DEA"] - curr["DIF"] < 0.05) and (curr["DIF"] > prev["DIF"])
    
    # 底背离
    is_divergence = False
    last_60_low_idx = df["low"].tail(60).idxmin()
    if last_60_low_idx != curr.name:
        if curr["close"] < df.loc[last_60_low_idx, "low"] * 1.05:
            if curr["DIF"] > df.loc[last_60_low_idx, "DIF"] + 0.1:
                is_divergence = True

    # 综合买点信号
    has_buy_signal = s_macd or s_kdj or s_ma_bull or is_near_gold or is_divergence

    if not has_buy_signal:
        return None

    # ==========================================
    # 🛡️ 避坑过滤器 (Pitfall Filters)
    # ==========================================
    
    # 1. 弱势过滤: 股价还在布林带中轨之下 -> 剔除
    if curr["close"] < curr["BOLL_Mid"]:
        return None 

    # 2. 资金背离过滤: 资金流出 (OBV < 10日均线) -> 剔除
    if curr["OBV"] < curr["OBV_MA10"]:
        return None

    # 3. 超买过滤: RSI > 80 -> 剔除
    if curr["RSI"] > 80:
        return None

    # ==========================================
    # 通过所有体检
    # ==========================================

    return {
        "close": curr["close"],
        "vol_ratio": vol_ratio,
        "rsi": round(curr["RSI"], 1),
        "macd_gold": "真金叉" if s_macd else "",
        "near_gold": "预警" if is_near_gold else "",
        "divergence": "底背离" if is_divergence else "",
        "kdj_gold": "是" if s_kdj else "",
        "ma_bull": "是" if s_ma_bull else "",
        # 显示辅助状态
        "boll_status": "突破上轨" if curr["close"] > curr["BOLL_High"] else "安全区",
        "obv_status": "资金流入"
    }

# --- 6. 主程序 ---
def main():
    print("=== 精英选股启动 (避坑版 + 来源显示) ===")
    pd.DataFrame([["Init", "OK"]]).to_excel("Init_Check.xlsx", index=False)
    
    try:
        # 1. 获取基础列表和来源名称
        base_targets, source_name = get_targets_robust()
        print(f"当前基础数据源: {source_name}")
        
        # 2. 尝试热点过滤
        hot_pool = get_hot_stock_pool()
        
        final_source_tag = source_name
        
        # 只有在网络正常(非离线模式)且热点获取成功时，才进行热点过滤
        if hot_pool and len(base_targets) > 100:
            print("正在进行热点过滤...")
            targets = base_targets[base_targets["code"].isin(hot_pool)]
            final_source_tag = f"{source_name} + 热点过滤"
            print(f"热点过滤后剩余: {len(targets)} 只")
        else:
            print("跳过热点过滤 (使用基础列表)")
            targets = base_targets

        start_dt = (datetime.now() - timedelta(days=200)).strftime("%Y%m%d")
        result_data = []
        
        total = len(targets)
        print(f"开始深度扫描 {total} 只股票 (已开启强力过滤)...")

        for i, s in targets.iterrows():
            code = s["code"]
            name = s["name"]
            
            if i % 20 == 0: print(f"进度: {i}/{total} ...")

            try:
                df = get_data_with_retry(code, start_dt)
                if df is None: continue

                df.rename(columns={"日期":"date","开盘":"open","收盘":"close","最高":"high","最低":"low","成交量":"volume"}, inplace=True)
                df["date"] = pd.to_datetime(df["date"])
                df.set_index("date", inplace=True)

                res = process_stock(df)
                
                if res:
                    if res['macd_gold'] and res['vol_ratio'] > 1.5:
                        print(f"  ★ 极品: {code} {name} (量比:{res['vol_ratio']})")
                    
                    result_data.append({
                        "代码": code,
                        "名称": name,
                        "现价": res["close"],
                        "量比": res["vol_ratio"],
                        "RSI数值": res["rsi"],
                        "MACD真金叉": res["macd_gold"],
                        "即将金叉": res["near_gold"],
                        "底背离": res["divergence"],
                        "KDJ金叉": res["kdj_gold"],
                        "均线多头": res["ma_bull"],
                        "资金状态": res["obv_status"],
                        "通道状态": res["boll_status"],
                        "数据来源": final_source_tag  # <--- 新增列
                    })
            except: continue
            time.sleep(0.05)

        dt_str = datetime.now().strftime("%Y%m%d")
        if result_data:
            # 包含所有信息的列
            cols = ["代码", "名称", "现价", "量比", "RSI数值", 
                    "MACD真金叉", "即将金叉", "底背离", 
                    "资金状态", "通道状态",
                    "KDJ金叉", "均线多头", "数据来源"]
            
            df_res = pd.DataFrame(result_data, columns=cols)
            # 排序
            df_res = df_res.sort_values(by=["MACD真金叉", "量比"], ascending=False)
            
            filename = f"精品选股结果_{dt_str}.xlsx"
            df_res.to_excel(filename, index=False)
            print(f"完成！已保存: {filename}")
        else:
            pd.DataFrame([["无"]]).to_excel(f"无结果_{dt_str}.xlsx")

    except Exception:
        with open("FATAL_ERROR.txt", "w") as f: f.write(traceback.format_exc())

if __name__ == "__main__":
    main()
