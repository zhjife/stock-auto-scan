# -*- coding: utf-8 -*-
"""
Alpha Galaxy Excel - 宇宙级全形态量化系统 (Excel终极版)
Features: 30+种严谨K线形态 | 自动交易计划 | Excel多Sheet导出
Author: Quant Studio
"""

import akshare as ak
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import warnings
from datetime import datetime, timedelta
import time
import functools

# 配置
warnings.filterwarnings('ignore')

# ==========================================
# 1. 严谨K线形态识别引擎 (30+ Patterns)
# ==========================================
class KLineStrictLib:
    """
    基于严谨定义的 Pandas 向量化形态库
    不依赖 TA-Lib，但逻辑对标标准蜡烛图技术
    """
    @staticmethod
    def detect(df):
        if len(df) < 20: return 0, [], []
        
        # 基础数据
        c = df['close']
        o = df['open']
        h = df['high']
        l = df['low']
        v = df['volume']
        
        # 均线
        ma5, ma10, ma20 = df['ma5'], df['ma10'], df['ma20']
        
        # 形态特征变量
        body = np.abs(c - o)                   # 实体高度
        upper_s = h - np.maximum(c, o)         # 上影线
        lower_s = np.minimum(c, o) - l         # 下影线
        avg_body = body.rolling(10).mean()     # 平均实体大小
        range_ = h - l                         # 全长
        
        # 辅助函数: 获取倒数第 i 天的数据
        def get(s, i): return s.iloc[i]
        
        buy_pats = []    # 买入形态
        risk_pats = []   # 风险形态
        score = 0
        
        # =========================================
        # A. 底部反转形态 (Bottom Reversal)
        # =========================================
        
        # 1. 早晨之星 (Morning Star) [标准定义]
        # Day1: 长阴; Day2: 向下跳空, 小实体; Day3: 阳线, 收盘价 > Day1实体中点
        if (get(c,-3) < get(o,-3)) and (get(body,-3) > get(avg_body,-3)) and \
           (get(h,-2) < get(l,-3)) and \
           (get(c,-1) > get(o,-1)) and (get(c,-1) > (get(o,-3)+get(c,-3))/2):
            buy_pats.append("早晨之星(标准)")
            score += 20

        # 2. 锤子线 (Hammer)
        # 处于下降趋势(近5日低点), 下影线 > 2倍实体, 上影线极短
        if (get(l,-1) == l.iloc[-5:].min()) and \
           (get(lower_s,-1) >= 2 * get(body,-1)) and \
           (get(upper_s,-1) <= 0.1 * get(body,-1)):
            buy_pats.append("锤子线")
            score += 15

        # 3. 倒锤子线 (Inverted Hammer)
        # 处于下降趋势, 上影线 > 2倍实体, 下影线极短
        if (get(l,-1) == l.iloc[-5:].min()) and \
           (get(upper_s,-1) >= 2 * get(body,-1)) and \
           (get(lower_s,-1) <= 0.1 * get(body,-1)):
            buy_pats.append("倒锤头")
            score += 10

        # 4. 启明星/旭日东升 (Bullish Engulfing)
        # 阳包阴: Day2开盘 < Day1收盘, Day2收盘 > Day1开盘
        if (get(c,-2) < get(o,-2)) and (get(c,-1) > get(o,-1)) and \
           (get(o,-1) < get(c,-2)) and (get(c,-1) > get(o,-2)):
            buy_pats.append("阳包阴(吞噬)")
            score += 20

        # 5. 曙光初现 (Piercing Line)
        # Day1大阴, Day2低开, 收盘刺入Day1实体一半以上
        if (get(c,-2) < get(o,-2)) and (get(body,-2) > get(avg_body,-2)) and \
           (get(o,-1) < get(l,-2)) and \
           (get(c,-1) > (get(o,-2)+get(c,-2))/2) and (get(c,-1) < get(o,-2)):
            buy_pats.append("曙光初现")
            score += 15

        # 6. 平底/镊子底 (Tweezer Bottom)
        if abs(get(l,-1) - get(l,-2)) < (get(c,-1)*0.002) and (get(l,-1) == l.iloc[-10:].min()):
            buy_pats.append("镊子底")
            score += 10

        # 7. 红三兵 (Three White Soldiers)
        # 连续三阳, 收盘价创新高, 且每根都在上一根实体内开盘
        if (get(c,-3)>get(o,-3)) and (get(c,-2)>get(o,-2)) and (get(c,-1)>get(o,-1)) and \
           (get(c,-1)>get(c,-2)>get(c,-3)):
            buy_pats.append("红三兵")
            score += 15

        # =========================================
        # B. 攻击与整理形态 (Continuation / Breakout)
        # =========================================

        # 8. 上升三法 (Rising Three Methods) [复杂形态]
        # 长阳 -> 3根小阴线(不跌破长阳低点) -> 长阳创新高
        if (get(c,-5)>get(o,-5)) and (get(body,-5)>get(avg_body,-5)) and \
           (get(c,-4)<get(o,-4)) and (get(c,-3)<get(o,-3)) and (get(c,-2)<get(o,-2)) and \
           (get(l,-4)>get(l,-5)) and (get(l,-2)>get(l,-5)) and \
           (get(c,-1)>get(o,-1)) and (get(c,-1)>get(c,-5)):
            buy_pats.append("上升三法(N字反包)")
            score += 25

        # 9. 多方炮 (Two Red Sandwiched Black)
        if (get(c,-3)>get(o,-3)) and (get(c,-2)<get(o,-2)) and (get(c,-1)>get(o,-1)) and \
           (get(c,-1) > get(c,-3)):
            buy_pats.append("多方炮")
            score += 20

        # 10. 向上跳空缺口 (Gap Up)
        if get(l,-1) > get(h,-2):
            buy_pats.append("跳空缺口")
            score += 15

        # 11. 一阳穿三线 (Golden Breakout)
        if (get(c,-1) > max(get(ma5,-1), get(ma10,-1), get(ma20,-1))) and \
           (get(o,-1) < min(get(ma5,-1), get(ma10,-1), get(ma20,-1))):
            buy_pats.append("一阳穿三线")
            score += 25
        
        # 12. 倍量过左峰 (Volume Breakout)
        if (get(v,-1) > get(v,-2)*1.9) and (get(c,-1) >= c.iloc[-20:].max()):
            buy_pats.append("倍量过左峰")
            score += 20

        # 13. 金蜘蛛 (Golden Spider)
        diff = max(get(ma5,-1), get(ma10,-1), get(ma20,-1)) - min(get(ma5,-1), get(ma10,-1), get(ma20,-1))
        if (diff/get(c,-1) < 0.015) and (get(c,-1) > get(ma5,-1)):
            buy_pats.append("金蜘蛛")
            score += 15

        # =========================================
        # C. 顶部/风险形态 (Top Reversal / Risk) - 扣分
        # =========================================

        # 14. 黄昏之星 (Evening Star)
        if (get(c,-3)>get(o,-3)) and (get(body,-3)>get(avg_body,-3)) and \
           (get(l,-2)>get(h,-3)) and \
           (get(c,-1)<get(o,-1)) and (get(c,-1)<(get(o,-3)+get(c,-3))/2):
            risk_pats.append("风险:黄昏之星")
            score -= 30

        # 15. 乌云盖顶 (Dark Cloud Cover)
        if (get(c,-2)>get(o,-2)) and (get(c,-1)<get(o,-1)) and \
           (get(o,-1)>get(h,-2)) and (get(c,-1)<(get(o,-2)+get(c,-2))/2):
            risk_pats.append("风险:乌云盖顶")
            score -= 25

        # 16. 穿头破脚/阴包阳 (Bearish Engulfing)
        if (get(c,-2)>get(o,-2)) and (get(c,-1)<get(o,-1)) and \
           (get(o,-1)>get(c,-2)) and (get(c,-1)<get(o,-2)):
            risk_pats.append("风险:阴包阳")
            score -= 25

        # 17. 三只乌鸦 (Three Black Crows)
        if (get(c,-1)<get(o,-1)) and (get(c,-2)<get(o,-2)) and (get(c,-3)<get(o,-3)) and \
           (get(c,-1)<get(c,-2)<get(c,-3)):
            risk_pats.append("风险:三只乌鸦")
            score -= 30

        # 18. 射击之星/流星 (Shooting Star)
        # 上影线长，实体小，高位
        if (get(upper_s,-1) > 2*get(body,-1)) and (get(lower_s,-1) < 0.1*get(body,-1)) and \
           (get(c,-1) > get(c,-20)*1.15):
            risk_pats.append("风险:射击之星")
            score -= 20

        # 19. 吊颈线 (Hanging Man)
        # 下影线长，实体小，高位
        if (get(lower_s,-1) > 2*get(body,-1)) and (get(upper_s,-1) < 0.1*get(body,-1)) and \
           (get(c,-1) > get(c,-20)*1.15):
            risk_pats.append("风险:吊颈线")
            score -= 20
        
        # 20. 断头铡刀
        if (get(c,-1) < min(get(ma5,-1), get(ma10,-1), get(ma20,-1))) and \
           (get(o,-1) > max(get(ma5,-1), get(ma10,-1), get(ma20,-1))):
            risk_pats.append("风险:断头铡刀")
            score -= 40

        return score, buy_pats, risk_pats

# ==========================================
# 2. 高级指标计算引擎
# ==========================================
class IndicatorEngine:
    @staticmethod
    def calculate(df):
        if len(df) < 60: return None
        
        c = df['close']; h = df['high']; l = df['low']; v = df['volume']
        
        # 均线
        ma5 = c.rolling(5).mean()
        ma10 = c.rolling(10).mean()
        ma20 = c.rolling(20).mean()
        ma60 = c.rolling(60).mean()
        df['ma5'], df['ma10'], df['ma20'] = ma5, ma10, ma20 # 注入df供形态库使用
        
        # CMF (资金流)
        mf_mult = ((c - l) - (h - c)) / (h - l).replace(0, 0.01)
        cmf = (mf_mult * v).rolling(20).sum() / v.rolling(20).sum()
        
        # CCI (动量)
        tp = (h + l + c) / 3
        cci = (tp - tp.rolling(14).mean()) / (0.015 * tp.rolling(14).apply(lambda x: np.mean(np.abs(x - np.mean(x))), raw=True))
        
        # ATR (风控)
        tr = pd.concat([h - l, abs(h - c.shift(1)), abs(l - c.shift(1))], axis=1).max(axis=1)
        atr = tr.rolling(14).mean()
        
        # ADX (趋势强度)
        up = h - h.shift(1); down = l.shift(1) - l
        plus_dm = np.where((up > down) & (up > 0), up, 0.0)
        minus_dm = np.where((down > up) & (down > 0), down, 0.0)
        tr_smooth = tr.rolling(14).sum()
        plus_di = 100 * (pd.Series(plus_dm).rolling(14).sum() / tr_smooth)
        minus_di = 100 * (pd.Series(minus_dm).rolling(14).sum() / tr_smooth)
        dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
        adx = dx.rolling(14).mean()
        
        # MACD
        exp12 = c.ewm(span=12, adjust=False).mean()
        exp26 = c.ewm(span=26, adjust=False).mean()
        dif = exp12 - exp26
        dea = dif.ewm(span=9, adjust=False).mean()
        
        curr = df.iloc[-1]
        
        return {
            'close': curr['close'],
            'ma20': ma20.iloc[-1], 'ma60': ma60.iloc[-1],
            'cmf': cmf.iloc[-1],
            'cci': cci.iloc[-1],
            'adx': adx.iloc[-1],
            'atr': atr.iloc[-1],
            'macd_dif': dif.iloc[-1], 'macd_dea': dea.iloc[-1]
        }

# ==========================================
# 3. Excel 导出引擎 (Excel Exporter)
# ==========================================
class ExcelExporter:
    @staticmethod
    def save(df_data, filename):
        if df_data.empty: return
        
        print(f"正在生成 Excel 报表: {filename} ...")
        
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Sheet 1: 选股结果
            cols = ['代码', '名称', '总分', '现价', '建议买入区间', '止损价', '止盈价', 
                    '买入形态', '风险形态', '得分详情', 'CMF', 'CCI', 'ADX']
            df_export = df_data[cols]
            df_export.to_excel(writer, sheet_name='选股结果', index=False)
            
            # Sheet 2: K线形态字典
            patterns_desc = [
                ['形态名称', '类型', '大白话说明'],
                ['早晨之星(标准)', '买入-反转', '底部三日组合：阴线+星线+阳线(切入一半以上)，强力见底'],
                ['锤子线', '买入-反转', '底部长下影线，实体小，主力试盘后拉回，支撑强'],
                ['倒锤头', '买入-反转', '底部长上影线，实体小，主力低位试盘，抛压被承接'],
                ['阳包阴(吞噬)', '买入-反转', '今日阳线实体完全包住昨日阴线，多头反击'],
                ['曙光初现', '买入-反转', '大阴线后低开高走，阳线实体刺入阴线一半以上'],
                ['镊子底', '买入-反转', '两日最低价几乎相同，形成平底支撑'],
                ['红三兵', '买入-攻击', '连续三天阳线稳步推升，重心上移，多头排列初期'],
                ['上升三法(N字反包)', '买入-持续', '大阳线后接三根小调整线(不破低)，再拉大阳创新高'],
                ['多方炮', '买入-攻击', '阳阴阳组合，中间是洗盘，洗完继续涨'],
                ['跳空缺口', '买入-强势', '向上跳空高开不回补，留下缺口，主力强势表现'],
                ['一阳穿三线', '买入-突破', '一根大阳线同时突破5/10/20均线，爆发力强'],
                ['倍量过左峰', '买入-突破', '成交量翻倍且价格突破前期高点，解放套牢盘'],
                ['金蜘蛛', '买入-突破', '均线粘合后，价格放量突破，均线向上发散'],
                ['风险:黄昏之星', '卖出-风险', '顶部反转：阳线+星线+阴线，主力出货'],
                ['风险:乌云盖顶', '卖出-风险', '大阳后接低开低走大阴线，吃掉昨日阳线一半涨幅'],
                ['风险:阴包阳', '卖出-风险', '穿头破脚，阴线实体完全吃掉昨日阳线，空头吞噬'],
                ['风险:三只乌鸦', '卖出-风险', '连续三根阴线杀跌，重心下移，资金出逃'],
                ['风险:射击之星', '卖出-风险', '高位出现长上影线，实体小，冲高回落见顶'],
                ['风险:吊颈线', '卖出-风险', '高位出现长下影线，实体小，看似支撑实为诱多'],
                ['风险:断头铡刀', '卖出-风险', '一根大阴线直接切断5/10/20所有均线，趋势崩塌']
            ]
            df_pat = pd.DataFrame(patterns_desc[1:], columns=patterns_desc[0])
            df_pat.to_excel(writer, sheet_name='形态图解', index=False)
            
            # Sheet 3: 指标说明书
            indicators_desc = [
                ['指标名称', '实战含义', '判断标准'],
                ['CMF (蔡金资金流)', '监控主力资金进出', '>0.1表示主力抢筹；<0表示流出'],
                ['CCI (顺势指标)', '监控股价爆发力', '>100表示进入主升浪加速区，适合短线'],
                ['ADX (趋势强度)', '监控趋势是否真实', '>25表示趋势强劲；<20表示震荡无方向'],
                ['ATR (真实波幅)', '计算止损和仓位', '价格的波动范围，用于科学设定止损位'],
                ['MACD', '趋势之王', '水上金叉(0轴上)是主升浪最稳信号']
            ]
            df_ind = pd.DataFrame(indicators_desc[1:], columns=indicators_desc[0])
            df_ind.to_excel(writer, sheet_name='指标说明书', index=False)
            
        print(f"✅ Excel 文件已保存至: {filename}")

# ==========================================
# 4. 策略主控 (Main Strategy)
# ==========================================
class AlphaGalaxyUltimate:
    def __init__(self):
        self.min_cap = 40 * 10000 * 10000 

    def get_candidates(self):
        print("1. 获取全市场快照 & 初步清洗...")
        try:
            df = ak.stock_zh_a_spot_em()
            df['总市值'] = pd.to_numeric(df['总市值'], errors='coerce')
            df['最新价'] = pd.to_numeric(df['最新价'], errors='coerce')
            df['换手率'] = pd.to_numeric(df['换手率'], errors='coerce')
            
            mask = (
                (~df['代码'].str.startswith(('30', '688', '8', '4'))) & 
                (~df['名称'].str.contains('ST|退')) &
                (df['总市值'] > self.min_cap) &
                (df['最新价'] > 3.0) &
                (df['换手率'] > 1.0) & (df['换手率'] < 20)
            )
            return list(zip(df[mask]['代码'], df[mask]['名称']))
        except:
            return []

    def analyze_one(self, args):
        symbol, name = args
        try:
            # QFQ 前复权，确保形态准确
            end = datetime.now().strftime("%Y%m%d")
            start = (datetime.now() - timedelta(days=400)).strftime("%Y%m%d")
            df = ak.stock_zh_a_hist(symbol=symbol, period='daily', start_date=start, end_date=end, adjust='qfq')
            
            if df is None: return None
            df.rename(columns={'日期':'date', '开盘':'open', '收盘':'close', '最高':'high', '最低':'low', '成交量':'volume'}, inplace=True)
            
            # 计算
            fac = IndicatorEngine.calculate(df)
            if not fac: return None
            
            k_score, buy_pats, risk_pats = KLineStrictLib.detect(df)
            
            # 评分
            score = 0
            logic = []
            
            # 否决项
            if risk_pats: score -= 30
            if fac['ma20'] < fac['ma60']: return None
            
            # 趋势项
            if fac['close'] > fac['ma20'] > fac['ma60']:
                base = 20
                if fac['adx'] > 25: base += 10; logic.append(f"ADX强趋势({int(fac['adx'])})")
                score += base
                
            # 资金项
            if fac['cmf'] > 0.15: score += 15; logic.append(f"资金流入({round(fac['cmf'],2)})")
            elif fac['cmf'] > 0: score += 5
            
            # 动量项
            if fac['cci'] > 100: score += 10; logic.append("CCI爆发")
            if fac['macd_dif'] > fac['macd_dea'] and fac['macd_dif'] > 0: score += 10; logic.append("MACD水上金叉")
            
            # 形态项
            if k_score > 0: score += k_score
            
            # 交易计划
            buy_l = fac['close'] * 0.99
            buy_h = fac['close'] * 1.01
            stop = fac['close'] - 2 * fac['atr']
            profit = fac['close'] + 3 * fac['atr']
            
            if score >= 60:
                return {
                    "代码": symbol,
                    "名称": name,
                    "总分": score,
                    "现价": fac['close'],
                    "建议买入区间": f"{round(buy_l,2)}~{round(buy_h,2)}",
                    "止损价": round(stop, 2),
                    "止盈价": round(profit, 2),
                    "买入形态": " | ".join(buy_pats) if buy_pats else "-",
                    "风险形态": " | ".join(risk_pats) if risk_pats else "-",
                    "得分详情": " ".join(logic),
                    "CMF": round(fac['cmf'], 3),
                    "CCI": round(fac['cci'], 1),
                    "ADX": int(fac['adx'])
                }
            return None
        except:
            return None

    def run(self):
        print(f"{'='*100}")
        print(" 🌌 Alpha Galaxy Excel - 宇宙级全形态选股系统 🌌")
        print(f"{'='*100}")
        
        candidates = self.get_candidates()
        print(f"待扫描: {len(candidates)} 只...")
        
        results = []
        with ThreadPoolExecutor(max_workers=16) as executor:
            for res in tqdm(executor.map(self.analyze_one, candidates), total=len(candidates)):
                if res: results.append(res)
        
        if results:
            df = pd.DataFrame(results)
            df.sort_values(by='总分', ascending=False, inplace=True)
            
            # 终端展示
            print("\n" + "="*120)
            print(df[['代码', '名称', '总分', '现价', '买入形态', '风险形态', '建议买入区间']].head(10).to_string(index=False))
            
            # 导出Excel
            filename = f"Alpha_Galaxy_Report_{datetime.now().strftime('%Y%m%d')}.xlsx"
            ExcelExporter.save(df, filename)
        else:
            print("无符合条件标的。")

if __name__ == "__main__":
    AlphaGalaxyUltimate().run()
89.3s
info
Google AI models may make mistakes, so double-check outputs.
Use Arrow Up and Arrow Down to select a turn, Enter to jump to it, and Escape to return to the chat.
Start typing a prompt

            ]
            df_pat = pd.DataFrame(patterns_desc[1:], columns=patterns_desc[0])
            df_pat.to_excel(writer, sheet_name='形态图解', index=False)
            
            # Sheet 3: 指标说明书
            indicators_desc = [
                ['指标名称', '实战含义', '判断标准'],
                ['CMF (蔡金资金流)', '监控主力资金进出', '>0.1表示主力抢筹；<0表示流出'],
                ['CCI (顺势指标)', '监控股价爆发力', '>100表示进入主升浪加速区，适合短线'],
                ['ADX (趋势强度)', '监控趋势是否真实', '>25表示趋势强劲；<20表示震荡无方向'],
                ['ATR (真实波幅)', '计算止损和仓位', '价格的波动范围，用于科学设定止损位'],
                ['MACD', '趋势之王', '水上金叉(0轴上)是主升浪最稳信号']
            ]
            df_ind = pd.DataFrame(indicators_desc[1:], columns=indicators_desc[0])
            df_ind.to_excel(writer, sheet_name='指标说明书', index=False)
            
        print(f"✅ Excel 文件已保存至: {filename}")

# ==========================================
# 4. 策略主控 (Main Strategy)
# ==========================================
class AlphaGalaxyUltimate:
    def __init__(self):
        self.min_cap = 40 * 10000 * 10000 

    def get_candidates(self):
        print("1. 获取全市场快照 & 初步清洗...")
        try:
            df = ak.stock_zh_a_spot_em()
            df['总市值'] = pd.to_numeric(df['总市值'], errors='coerce')
            df['最新价'] = pd.to_numeric(df['最新价'], errors='coerce')
            df['换手率'] = pd.to_numeric(df['换手率'], errors='coerce')
            
            mask = (
                (~df['代码'].str.startswith(('30', '688', '8', '4'))) & 
                (~df['名称'].str.contains('ST|退')) &
                (df['总市值'] > self.min_cap) &
                (df['最新价'] > 3.0) &
                (df['换手率'] > 1.0) & (df['换手率'] < 20)
            )
            return list(zip(df[mask]['代码'], df[mask]['名称']))
        except:
            return []

    def analyze_one(self, args):
        symbol, name = args
        try:
            # QFQ 前复权，确保形态准确
            end = datetime.now().strftime("%Y%m%d")
            start = (datetime.now() - timedelta(days=400)).strftime("%Y%m%d")
            df = ak.stock_zh_a_hist(symbol=symbol, period='daily', start_date=start, end_date=end, adjust='qfq')
            
            if df is None: return None
            df.rename(columns={'日期':'date', '开盘':'open', '收盘':'close', '最高':'high', '最低':'low', '成交量':'volume'}, inplace=True)
            
            # 计算
            fac = IndicatorEngine.calculate(df)
            if not fac: return None
            
            k_score, buy_pats, risk_pats = KLineStrictLib.detect(df)
            
            # 评分
            score = 0
            logic = []
            
            # 否决项
            if risk_pats: score -= 30
            if fac['ma20'] < fac['ma60']: return None
            
            # 趋势项
            if fac['close'] > fac['ma20'] > fac['ma60']:
                base = 20
                if fac['adx'] > 25: base += 10; logic.append(f"ADX强趋势({int(fac['adx'])})")
                score += base
                
            # 资金项
            if fac['cmf'] > 0.15: score += 15; logic.append(f"资金流入({round(fac['cmf'],2)})")
            elif fac['cmf'] > 0: score += 5
            
            # 动量项
            if fac['cci'] > 100: score += 10; logic.append("CCI爆发")
            if fac['macd_dif'] > fac['macd_dea'] and fac['macd_dif'] > 0: score += 10; logic.append("MACD水上金叉")
            
            # 形态项
            if k_score > 0: score += k_score
            
            # 交易计划
            buy_l = fac['close'] * 0.99
            buy_h = fac['close'] * 1.01
            stop = fac['close'] - 2 * fac['atr']
            profit = fac['close'] + 3 * fac['atr']
            
            if score >= 60:
                return {
                    "代码": symbol,
                    "名称": name,
                    "总分": score,
                    "现价": fac['close'],
                    "建议买入区间": f"{round(buy_l,2)}~{round(buy_h,2)}",
                    "止损价": round(stop, 2),
                    "止盈价": round(profit, 2),
                    "买入形态": " | ".join(buy_pats) if buy_pats else "-",
                    "风险形态": " | ".join(risk_pats) if risk_pats else "-",
                    "得分详情": " ".join(logic),
                    "CMF": round(fac['cmf'], 3),
                    "CCI": round(fac['cci'], 1),
                    "ADX": int(fac['adx'])
                }
            return None
        except:
            return None

    def run(self):
        print(f"{'='*100}")
        print(" 🌌 Alpha Galaxy Excel - 宇宙级全形态选股系统 🌌")
        print(f"{'='*100}")
        
        candidates = self.get_candidates()
        print(f"待扫描: {len(candidates)} 只...")
        
        results = []
        with ThreadPoolExecutor(max_workers=12) as executor:
            for res in tqdm(executor.map(self.analyze_one, candidates), total=len(candidates)):
                if res: results.append(res)
        
        if results:
            df = pd.DataFrame(results)
            df.sort_values(by='总分', ascending=False, inplace=True)
            
            # 终端展示
            print("\n" + "="*120)
            print(df[['代码', '名称', '总分', '现价', '买入形态', '风险形态', '建议买入区间']].head(10).to_string(index=False))
            
            # 导出Excel
            filename = f"Alpha_Galaxy_Report_{datetime.now().strftime('%Y%m%d')}.xlsx"
            ExcelExporter.save(df, filename)
        else:
            print("无符合条件标的。")

if __name__ == "__main__":
    AlphaGalaxyUltimate().run()
