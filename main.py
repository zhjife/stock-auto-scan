import akshare as ak
import pandas as pd
import numpy as np
from ta.trend import MACD
from ta.momentum import StochasticOscillator, RSIIndicator
from ta.volatility import BollingerBands
# 修改点1：引入 ChaikinMoneyFlowIndicator
from ta.volume import OnBalanceVolumeIndicator, ChaikinMoneyFlowIndicator
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

# --- 5. 核心计算 (包含新逻辑) ---
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
    
    # OBV (能量潮)
    obv_ind = OnBalanceVolumeIndicator(close=df["close"], volume=df["volume"])
    df["OBV"] = obv_ind.on_balance_volume()
    df["OBV_MA10"] = df["OBV"].rolling(10).mean()

    # 修改点2：增加 CMF (Chaikin Money Flow) 计算
    # 20日周期，衡量机构资金流向，正数代表流入，负数代表流出
    cmf_ind = ChaikinMoneyFlowIndicator(high=df["high"], low=df["low"], close=df["close"], volume=df["volume"], window=20)
    df["CMF"] = cmf_ind.chaikin_money_flow()

    # === 信号判定 ===
    curr = df.iloc[-1]
    prev = df.iloc[-2]
    if pd.isna(curr['MA60']): return None

    # 1. MACD 相关信号
    # 真金叉：DIF 上穿 DEA，且红柱变长
    s_macd = (prev["DIF"] < prev["DEA"] and curr["DIF"] > curr["DEA"] and curr["MACD_Hist"] > prev["MACD_Hist"])
    # 即将金叉预警：目前死叉，但开口非常小，且DIF在回升
    is_near_gold = (curr["DIF"] < curr["DEA"]) and (curr["DEA"] - curr["DIF"] < 0.05) and (curr["DIF"] > prev["DIF"])
    
    # 底背离 (60天内最低价创新低，但DIF没创新低)
    is_divergence = False
    last_60_low_idx = df["low"].tail(60).idxmin()
    if last_60_low_idx != curr.name:
        if curr["close"] < df.loc[last_60_low_idx, "low"] * 1.05:
            if curr["DIF"] > df.loc[last_60_low_idx, "DIF"] + 0.1:
                is_divergence = True

    # 2. 趋势相关信号
    s_kdj = (prev["K"] < prev["D"] and curr["K"] > curr["D"]) # KDJ金叉
    s_ma_bull = (curr["MA5"] > curr["MA10"] > curr["MA20"] > curr["MA60"]) # 均线多头

    # ==========================================
    # 修改点3：严格选股逻辑判定
    # ==========================================
    
    # 条件组 A: MACD 强势或反转信号
    group_macd = s_macd or is_divergence or is_near_gold
    
    # 条件组 B: 趋势共振 (必须 KDJ金叉 且 均线多头)
    group_trend = s_kdj and s_ma_bull

    # 如果既不满足MACD组，也不满足趋势组，直接剔除
    if not (group_macd or group_trend):
        return None

    # ==========================================
    # 🛡️ 避坑过滤器 (在满足逻辑的前提下，过滤垃圾股)
    # ==========================================
    
    # 1. 弱势过滤: 股价还在布林带中轨之下 -> 剔除
    if curr["close"] < curr["BOLL_Mid"]:
        return None 

    # 2. 资金背离过滤: 资金流出 (OBV < 10日均线) -> 剔除
    # 这里我们保留这个过滤，确保选出的票OBV状态是好的
    if curr["OBV"] < curr["OBV_MA10"]:
        return None

    # 3. 超买过滤: RSI > 80 -> 剔除
    if curr["RSI"] > 80:
        return None

    # ==========================================
    # 组装结果
    # ==========================================
    
    # 计算OBV描述
    obv_val = "强力流入" if curr["OBV"] > curr["OBV_MA10"] * 1.01 else "温和流入"
    # CMF 描述
    cmf_val = round(curr["CMF"], 3)

    return {
        "close": curr["close"],
        "vol_ratio": vol_ratio,
        "rsi": round(curr["RSI"], 1),
        "cmf": cmf_val, # 新增
        "obv_desc": f"{obv_val}", # 修改描述
        "macd_gold": "真金叉" if s_macd else "",
        "near_gold": "预警" if is_near_gold else "",
        "divergence": "底背离" if is_divergence else "",
        "kdj_gold": "KDJ金叉" if s_kdj else "", # 修改文本方便阅读
        "ma_bull": "多头" if s_ma_bull else "", # 修改文本方便阅读
        "boll_status": "突破上轨" if curr["close"] > curr["BOLL_High"] else "安全区"
    }

# --- 6. 主程序 ---
def main():
    print("=== 精英选股启动 (MACD/CMF增强版) ===")
    pd.DataFrame([["Init", "OK"]]).to_excel("Init_Check.xlsx", index=False)
    
    try:
        base_targets, source_name = get_targets_robust()
        print(f"当前基础数据源: {source_name}")
        
        hot_pool = get_hot_stock_pool()
        final_source_tag = source_name
        
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
        print(f"开始深度扫描 {total} 只股票 (严格逻辑：MACD组 或 KDJ+多头组)...")

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
                    # 打印一些特别优质的信号
                    if res['cmf'] > 0.1 and res['vol_ratio'] > 1.5:
                         print(f"  ★ 资金抢筹: {code} {name} (CMF:{res['cmf']} 量比:{res['vol_ratio']})")
                    
                    result_data.append({
                        "代码": code,
                        "名称": name,
                        "现价": res["close"],
                        "量比": res["vol_ratio"],
                        "CMF数值": res["cmf"],          # 新增列
                        "OBV资金流向": res["obv_desc"], # 新增列
                        "MACD真金叉": res["macd_gold"],
                        "即将金叉": res["near_gold"],
                        "底背离": res["divergence"],
                        "KDJ金叉": res["kdj_gold"],
                        "均线多头": res["ma_bull"],
                        "RSI数值": res["rsi"],
                        "通道状态": res["boll_status"],
                        "数据来源": final_source_tag
                    })
            except: continue
            time.sleep(0.05)

        dt_str = datetime.now().strftime("%Y%m%d")
        if result_data:
            # 修改点4：调整Excel输出顺序，突出资金和核心信号
            cols = ["代码", "名称", "现价", "量比", 
                    "CMF数值", "OBV资金流向", # 资金面优先
                    "MACD真金叉", "即将金叉", "底背离", 
                    "KDJ金叉", "均线多头", 
                    "RSI数值", "通道状态", "数据来源"]
            
            df_res = pd.DataFrame(result_data, columns=cols)
            # 排序：优先看有真金叉的，其次看CMF资金流入大的
            df_res = df_res.sort_values(by=["MACD真金叉", "CMF数值"], ascending=[False, False])
            
            filename = f"MACD_CMF_选股结果_{dt_str}.xlsx"
            df_res.to_excel(filename, index=False)
            print(f"完成！已保存: {filename}")
            print(f"共筛选出 {len(df_res)} 只股票")
        else:
            print("没有符合严格筛选条件的股票。")
            pd.DataFrame([["无"]]).to_excel(f"无结果_{dt_str}.xlsx")

    except Exception:
        with open("FATAL_ERROR.txt", "w") as f: f.write(traceback.format_exc())

if __name__ == "__main__":
    main()
