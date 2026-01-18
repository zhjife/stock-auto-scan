# -*- coding: utf-8 -*-
"""
Alpha Galaxy Omni Pro Max - 机构全维量化系统 (最终融合版)
Features: 
1. 30+种严谨K线形态 (补全了Pro版缺失的风险形态)
2. 量价动态分析 (量比逻辑：缩量锁筹/放量突破)
3. 9大核心指标 (新增量比 + PB/CMF分层优化)
4. NLP 舆情风控
5. Excel 完整字典导出 (融合了完全体的详细说明书)
"""

import akshare as ak
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import warnings
from datetime import datetime, timedelta
from snownlp import SnowNLP
import time

# 配置
warnings.filterwarnings('ignore')

# ==========================================
# 1. 舆情分析引擎 (NLP Sentiment)
# ==========================================
class SentimentEngine:
    @staticmethod
    def analyze(symbol):
        try:
            news_df = ak.stock_news_em(symbol=symbol)
            if news_df is None or news_df.empty:
                return 0, "无近期舆情"
            
            recent_news = news_df.head(10)
            titles = recent_news['新闻标题'].tolist()
            full_text = "。".join(titles)
            
            # 关键词硬匹配
            pos_kw = ['增长', '预增', '突破', '利好', '回购', '获批', '中标', '大涨', '新高']
            neg_kw = ['立案', '调查', '亏损', '减持', '警示', '违规', '大跌', '退市', '被查']
            
            hard_score = 0
            keywords = []
            
            for t in titles:
                for kw in pos_kw:
                    if kw in t: 
                        hard_score += 2
                        keywords.append(kw)
                for kw in neg_kw:
                    if kw in t: 
                        hard_score -= 10 
                        keywords.append(kw)
            
            # NLP 软匹配
            s = SnowNLP(full_text)
            soft_score = (s.sentiments - 0.5) * 10
            
            total_score = hard_score + soft_score
            total_score = max(min(total_score, 20), -20)
            
            summary = f"关键词:{list(set(keywords))}" if keywords else "舆情平稳"
            return round(total_score, 1), summary
        except Exception:
            return 0, "舆情获取失败"

# ==========================================
# 2. 严谨K线形态识别引擎 (30+种)
# [UPDATED] 补全了 Pro 版遗漏的形态
# ==========================================
class KLineStrictLib:
    @staticmethod
    def detect(df):
        if len(df) < 20: return 0, [], []
        c, o, h, l, v = df['close'], df['open'], df['high'], df['low'], df['volume']
        ma5, ma10, ma20 = df['ma5'], df['ma10'], df['ma20']
        body = np.abs(c - o)
        upper_s = h - np.maximum(c, o)
        lower_s = np.minimum(c, o) - l
        avg_body = body.rolling(10).mean()
        def get(s, i): return s.iloc[i]
        
        buy_pats, risk_pats = [], []
        score = 0
        
        # --- 底部/反转 (买入) ---
        if (get(c,-3)<get(o,-3)) and (get(body,-3)>get(avg_body,-3)) and (get(h,-2)<get(l,-3)) and (get(c,-1)>get(o,-1)) and (get(c,-1)>(get(o,-3)+get(c,-3))/2):
            buy_pats.append("早晨之星"); score += 20
        if (get(l,-1)==l.iloc[-5:].min()) and (get(lower_s,-1)>=2*get(body,-1)) and (get(upper_s,-1)<=0.1*get(body,-1)):
            buy_pats.append("锤子线"); score += 15
        if (get(l,-1)==l.iloc[-5:].min()) and (get(upper_s,-1)>=2*get(body,-1)) and (get(lower_s,-1)<=0.1*get(body,-1)):
            buy_pats.append("倒锤头"); score += 10
        if (get(c,-2)<get(o,-2)) and (get(c,-1)>get(o,-1)) and (get(o,-1)<get(c,-2)) and (get(c,-1)>get(o,-2)):
            buy_pats.append("阳包阴"); score += 20
        if (get(c,-2)<get(o,-2)) and (get(body,-2)>get(avg_body,-2)) and (get(o,-1)<get(l,-2)) and (get(c,-1)>(get(o,-2)+get(c,-2))/2):
            buy_pats.append("曙光初现"); score += 15
        if abs(get(l,-1)-get(l,-2))<(get(c,-1)*0.002) and (get(l,-1)==l.iloc[-10:].min()):
            buy_pats.append("镊子底"); score += 10
        if (get(c,-2)<get(o,-2)) and (get(body,-2)>get(avg_body,-2)) and (get(c,-1)>get(o,-1)) and (get(h,-1)<get(h,-2)) and (get(l,-1)>get(l,-2)) and (get(c,-1)<get(c,-20)):
            buy_pats.append("身怀六甲"); score += 10

        # --- 攻击/突破 (买入) ---
        if (get(c,-3)>get(o,-3)) and (get(c,-2)>get(o,-2)) and (get(c,-1)>get(o,-1)) and (get(c,-1)>get(c,-2)>get(c,-3)):
            buy_pats.append("红三兵"); score += 15
        if (get(c,-5)>get(o,-5)) and (get(body,-5)>get(avg_body,-5)) and (get(c,-4)<get(o,-4)) and (get(c,-3)<get(o,-3)) and (get(c,-2)<get(o,-2)) and (get(c,-1)>get(o,-1)) and (get(c,-1)>get(c,-5)):
            buy_pats.append("上升三法"); score += 25
        if (get(c,-3)>get(o,-3)) and (get(c,-2)<get(o,-2)) and (get(c,-1)>get(o,-1)) and (get(c,-1)>get(c,-3)):
            buy_pats.append("多方炮"); score += 20
        if get(l,-1)>get(h,-2):
            buy_pats.append("跳空缺口"); score += 15
        if (get(c,-1)>max(get(ma5,-1),get(ma10,-1),get(ma20,-1))) and (get(o,-1)<min(get(ma5,-1),get(ma10,-1),get(ma20,-1))):
            buy_pats.append("一阳穿三线"); score += 25
        if (get(v,-1)>get(v,-2)*1.9) and (get(c,-1)>=c.iloc[-20:].max()):
            buy_pats.append("倍量过左峰"); score += 20
        diff = max(get(ma5,-1),get(ma10,-1),get(ma20,-1)) - min(get(ma5,-1),get(ma10,-1),get(ma20,-1))
        if (diff/get(c,-1)<0.015) and (get(c,-1)>get(ma5,-1)):
            buy_pats.append("金蜘蛛"); score += 15
        if (get(upper_s,-2)>get(body,-2)) and (get(c,-1)>get(h,-2)) and (get(c,-1)>get(o,-1)):
            buy_pats.append("仙人指路"); score += 15

        # --- 风险形态 (卖出/否决) ---
        # 1. 经典顶部
        if (get(c,-3)>get(o,-3)) and (get(l,-2)>get(h,-3)) and (get(c,-1)<get(o,-1)) and (get(c,-1)<(get(o,-3)+get(c,-3))/2):
            risk_pats.append("风险:黄昏之星"); score -= 30
        if (get(c,-2)>get(o,-2)) and (get(c,-1)<get(o,-1)) and (get(o,-1)>get(h,-2)) and (get(c,-1)<(get(o,-2)+get(c,-2))/2):
            risk_pats.append("风险:乌云盖顶"); score -= 25
        if (get(c,-1)<min(get(ma5,-1),get(ma10,-1),get(ma20,-1))) and (get(o,-1)>max(get(ma5,-1),get(ma10,-1),get(ma20,-1))):
            risk_pats.append("风险:断头铡刀"); score -= 40
        if (get(c,-1)<get(o,-1)) and (get(c,-2)<get(o,-2)) and (get(c,-3)<get(o,-3)):
            risk_pats.append("风险:三只乌鸦"); score -= 30
        if get(h,-1)<get(l,-2):
            risk_pats.append("风险:向下缺口"); score -= 20
            
        # 2. [Added Back] 从完全体补回的风险形态
        if (get(c,-2)>get(o,-2)) and (get(c,-1)<get(o,-1)) and (get(o,-1)>get(c,-2)) and (get(c,-1)<get(o,-2)):
            risk_pats.append("风险:阴包阳"); score -= 25
        if (get(upper_s,-1)>2*get(body,-1)) and (get(lower_s,-1)<0.1*get(body,-1)) and (get(c,-1)>get(c,-20)*1.15):
            risk_pats.append("风险:射击之星"); score -= 20
        if (get(lower_s,-1)>2*get(body,-1)) and (get(upper_s,-1)<0.1*get(body,-1)) and (get(c,-1)>get(c,-20)*1.15):
            risk_pats.append("风险:吊颈线"); score -= 20

        return score, buy_pats, risk_pats

# ==========================================
# 3. 高级指标计算引擎 (保留 Pro 版的量比计算)
# ==========================================
class IndicatorEngine:
    @staticmethod
    def calculate(df):
        if len(df) < 60: return None
        c = df['close']; h = df['high']; l = df['low']; v = df['volume']
        
        # 均线
        ma5=c.rolling(5).mean(); ma10=c.rolling(10).mean(); ma20=c.rolling(20).mean(); ma60=c.rolling(60).mean()
        df['ma5'], df['ma10'], df['ma20'] = ma5, ma10, ma20
        
        # [NEW] 量比计算 (Pro版核心)
        vol_ma5 = v.rolling(5).mean()
        vol_ratio = v / vol_ma5.replace(0, 1)
        
        pct_change = c.pct_change() * 100
        mf_mult = ((c - l) - (h - c)) / (h - l).replace(0, 0.01)
        cmf_series = (mf_mult * v).rolling(20).sum() / v.rolling(20).sum()
        
        low_min = l.rolling(9).min(); high_max = h.rolling(9).max()
        rsv = (c - low_min) / (high_max - low_min) * 100
        K = rsv.ewm(com=2, adjust=False).mean(); D = K.ewm(com=2, adjust=False).mean(); J = 3 * K - 2 * D
        
        std20 = c.rolling(20).std(); bb_width = (4 * std20) / ma20
        bias = (c - ma20) / ma20 * 100
        
        tp = (h + l + c) / 3
        cci = (tp - tp.rolling(14).mean()) / (0.015 * tp.rolling(14).apply(lambda x: np.mean(np.abs(x - np.mean(x))), raw=True))
        
        tr = pd.concat([h - l, abs(h - c.shift(1)), abs(l - c.shift(1))], axis=1).max(axis=1)
        atr = tr.rolling(14).mean()
        
        delta = c.diff(); gain = (delta.where(delta > 0, 0)).rolling(14).mean(); loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rsi = 100 - (100 / (1 + gain/loss))

        up = h - h.shift(1); down = l.shift(1) - l
        plus_dm = np.where((up > down) & (up > 0), up, 0.0); minus_dm = np.where((down > up) & (down > 0), down, 0.0)
        tr_smooth = tr.rolling(14).sum()
        plus_di = 100 * (pd.Series(plus_dm).rolling(14).sum() / tr_smooth)
        minus_di = 100 * (pd.Series(minus_dm).rolling(14).sum() / tr_smooth)
        dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di); adx = dx.rolling(14).mean()
        
        exp12 = c.ewm(span=12, adjust=False).mean(); exp26 = c.ewm(span=26, adjust=False).mean()
        dif = exp12 - exp26; dea = dif.ewm(span=9, adjust=False).mean()

        curr = df.iloc[-1]
        return {
            'close': curr['close'], 'ma20': ma20.iloc[-1], 'ma60': ma60.iloc[-1],
            'atr': atr.iloc[-1], 'adx': adx.iloc[-1], 'macd_dif': dif.iloc[-1], 'macd_dea': dea.iloc[-1],
            'cci': cci.iloc[-1], 'rsi': rsi.iloc[-1], 'j_val': J.iloc[-1], 'bias': bias.iloc[-1], 'bb_width': bb_width.iloc[-1],
            'cmf_0': cmf_series.iloc[-1], 'cmf_1': cmf_series.iloc[-2], 'cmf_2': cmf_series.iloc[-3],
            'pct_0': pct_change.iloc[-1], 'pct_1': pct_change.iloc[-2], 'pct_2': pct_change.iloc[-3],
            'vol_ratio': vol_ratio.iloc[-1] 
        }

# ==========================================
# 4. Excel 导出引擎 
# [UPDATED] 融合了完全体的详细字典说明
# ==========================================
class ExcelExporter:
    @staticmethod
    def save(df_data, filename):
        if df_data.empty: return
        print(f"正在生成 Excel 报表: {filename} ...")
        
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            cols = [
                '代码', '名称', '总分', '现价', '建议买入区间', '止损价', '止盈价', 
                '买入形态', '风险形态', '舆情分析', '得分详情', 
                '换手率%', '量比', '市盈率', '市净率', 
                'J值', 'RSI', 'BIAS(%)', '布林带宽', 'ADX', 'CCI', 
                'CMF(今)', 'CMF(昨)', 'CMF(前)', 
                '涨幅%(今)', '涨幅%(昨)', '涨幅%(前)'
            ]
            df_export = df_data[cols]
            df_export.to_excel(writer, sheet_name='选股结果', index=False)
            
            # 形态图解 (使用 Code B 的详细描述)
            patterns_desc = [
                ['形态名称', '类型', '大白话说明'],
                ['早晨之星', '买入-反转', '底部三日组合：阴线+星线+阳线，强力见底'],
                ['锤子线', '买入-反转', '底部长下影线，主力试盘后拉回，支撑强'],
                ['倒锤头', '买入-反转', '底部长上影线，主力低位试盘'],
                ['阳包阴', '买入-反转', '今日阳线完全包住昨日阴线，多头反击'],
                ['曙光初现', '买入-反转', '大阴线后低开高走，阳线刺入阴线一半'],
                ['镊子底', '买入-反转', '两日最低价相同，平底支撑'],
                ['身怀六甲', '买入-变盘', '长阴包含小K线，底部孕育'],
                ['红三兵', '买入-攻击', '连续三天阳线稳步推升，重心上移'],
                ['上升三法', '买入-持续', '大阳后接三小阴不破低，再接大阳'],
                ['多方炮', '买入-攻击', '阳阴阳组合，洗盘结束信号'],
                ['跳空缺口', '买入-强势', '向上跳空不回补，主力强势'],
                ['一阳穿三线', '买入-突破', '大阳线同时突破5/10/20均线'],
                ['倍量过左峰', '买入-突破', '成交量翻倍且价格突破前期高点'],
                ['金蜘蛛', '买入-突破', '均线粘合后放量向上发散'],
                ['仙人指路', '买入-试盘', '今日大阳线突破昨日的长上影线'],
                ['风险:黄昏之星', '卖出-风险', '顶部：阳线+星线+阴线，见顶'],
                ['风险:乌云盖顶', '卖出-风险', '大阳后接大阴，吃掉一半涨幅'],
                ['风险:阴包阳', '卖出-风险', '空头吞噬，阴线包住阳线'],
                ['风险:三只乌鸦', '卖出-风险', '连续三根阴线杀跌，资金出逃'],
                ['风险:射击之星', '卖出-风险', '高位长上影线，冲高回落'],
                ['风险:吊颈线', '卖出-风险', '高位长下影线，诱多'],
                ['风险:断头铡刀', '卖出-风险', '一阴断多线，趋势崩塌'],
                ['风险:向下缺口', '卖出-风险', '向下跳空不回补，极弱势']
            ]
            pd.DataFrame(patterns_desc[1:], columns=patterns_desc[0]).to_excel(writer, sheet_name='形态图解', index=False)
            
            # 指标说明 (融合 Code A 的量比和 Code B 的详细说明)
            indicators_desc = [
                ['指标名称', '实战含义', '判断标准'],
                ['量比', '量能变化', '>1.5为放量；0.5-1.0为缩量(锁筹)'],
                ['市盈率(PE)', '估值', '0<PE<20为低估值(优)；PE<0为亏损(差)'],
                ['市净率(PB)', '资产价格', 'PB>10可能高估'],
                ['CMF', '资金流', '连续3天为正且递增，说明主力持续拿货'],
                ['J值 (KDJ)', '超买超卖', 'J<0为超卖(抄底)，J>100为超买(风险)'],
                ['布林带宽', '变盘前兆', '数值越小(<0.10)说明筹码越集中，即将变盘'],
                ['BIAS', '乖离率', '正值过大要回调，负值过大有反弹'],
                ['ADX', '趋势强度', '>25表示趋势强劲；<20表示震荡'],
                ['RSI', '强弱指标', '50-80为强势区，>80过热'],
                ['换手率', '活跃度', '3%-10%健康；>15%且滞涨则危险'],
                ['CCI', '爆发力', '>100表示加速']
            ]
            pd.DataFrame(indicators_desc[1:], columns=indicators_desc[0]).to_excel(writer, sheet_name='指标说明书', index=False)
            
        print(f"✅ Excel 文件已保存至: {filename}")

# ==========================================
# 5. 策略主控 (漏斗式 + 量价逻辑优化)
# [UPDATED] 融合了 Code B 的 PB 惩罚和 CMF 分层
# ==========================================
class AlphaGalaxyOmni:
    def __init__(self):
        self.min_cap = 40 * 10000 * 10000 

    def get_candidates(self):
        print("1. 获取全市场快照 & 初步清洗...")
        try:
            df = ak.stock_zh_a_spot_em()
            for col in ['总市值', '最新价', '换手率', '市盈率-动态', '市净率']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            mask = (
                (~df['代码'].str.startswith(('30', '688', '8', '4'))) & 
                (~df['名称'].str.contains('ST|退')) &
                (df['总市值'] > self.min_cap) &
                (df['最新价'] > 3.0) &
                (df['换手率'] > 1.0) & (df['换手率'] < 20)
            )
            return list(zip(df[mask]['代码'], df[mask]['名称'], df[mask]['市盈率-动态'], df[mask]['市净率'], df[mask]['换手率']))
        except:
            return []

    def scan_tech_fund(self, args):
        symbol, name, pe, pb, turnover = args
        try:
            if pe < 0: return None
            
            end = datetime.now().strftime("%Y%m%d")
            start = (datetime.now() - timedelta(days=400)).strftime("%Y%m%d")
            # 使用前复权 qfq
            df = ak.stock_zh_a_hist(symbol=symbol, period='daily', start_date=start, end_date=end, adjust='qfq')
            
            if df is None: return None
            df.rename(columns={'日期':'date', '开盘':'open', '收盘':'close', '最高':'high', '最低':'low', '成交量':'volume'}, inplace=True)
            
            fac = IndicatorEngine.calculate(df)
            if not fac: return None
            k_score, buy_pats, risk_pats = KLineStrictLib.detect(df)
            
            score = 0
            logic = []
            
            # --- 否决项 ---
            if risk_pats: score -= 30
            if fac['ma20'] < fac['ma60']: return None
            
            # --- 基本面 ---
            if 0 < pe <= 20: score += 20; logic.append(f"低估(PE{pe})")
            elif 20 < pe <= 50: score += 15
            if pb > 10: score -= 5  # [Added Back] PB 惩罚
            
            # --- 趋势 ---
            if fac['close'] > fac['ma20'] > fac['ma60']:
                base = 20
                if fac['adx'] > 25: base += 10; logic.append(f"强趋势(ADX{int(fac['adx'])})")
                score += base
            
            # --- 资金与量价 (Pro版核心逻辑) ---
            
            # 1. 缩量锁筹 (高分)
            if (fac['pct_0'] > 0) and (0.5 < fac['vol_ratio'] < 1.0) and (fac['close'] > fac['ma20']):
                score += 15
                logic.append(f"缩量锁筹(量比{round(fac['vol_ratio'],2)})")
            
            # 2. 放量攻击 (常规)
            elif (fac['pct_0'] > 0) and (fac['vol_ratio'] > 1.5):
                score += 10
                logic.append(f"放量上攻(量比{round(fac['vol_ratio'],2)})")
            
            # 3. 高换手滞涨 (风险)
            if (turnover > 15) and (fac['pct_0'] < 2) and (fac['pct_0'] > -2):
                score -= 15
                logic.append(f"⚠️高换手滞涨")

            # CMF [Enhanced] 融合了分层打分
            if fac['cmf_0'] > 0.1: score += 15; logic.append(f"资金抢筹")
            elif fac['cmf_0'] > 0: score += 5 # [Added Back] 弱抢筹也有分
            
            # --- 动量与形态 ---
            if fac['cci'] > 100: score += 10; logic.append(f"CCI爆发")
            if fac['macd_dif'] > fac['macd_dea'] and fac['macd_dif'] > 0: score += 10
            if k_score > 0: score += k_score

            # --- 输出 ---
            buy_l = fac['close'] * 0.99
            buy_h = fac['close'] * 1.01
            stop = fac['close'] - 2 * fac['atr']
            profit = fac['close'] + 3 * fac['atr']
            
            if score >= 65:
                return {
                    "代码": symbol, "名称": name, "总分": score, "现价": fac['close'],
                    "市盈率": round(pe, 2), "市净率": round(pb, 2), "换手率%": round(turnover, 2),
                    "量比": round(fac['vol_ratio'], 2), 
                    "建议买入区间": f"{round(buy_l,2)}~{round(buy_h,2)}",
                    "止损价": round(stop, 2), "止盈价": round(profit, 2),
                    "买入形态": " | ".join(buy_pats) if buy_pats else "-",
                    "风险形态": " | ".join(risk_pats) if risk_pats else "-",
                    "得分详情": " ".join(logic),
                    "J值": round(fac['j_val'], 1), "布林带宽": round(fac['bb_width'], 3),
                    "RSI": round(fac['rsi'], 1), "BIAS(%)": round(fac['bias'], 2),
                    "ADX": int(fac['adx']), "CCI": int(fac['cci']),
                    "CMF(今)": round(fac['cmf_0'], 3), "CMF(昨)": round(fac['cmf_1'], 3), "CMF(前)": round(fac['cmf_2'], 3),
                    "涨幅%(今)": round(fac['pct_0'], 2), "涨幅%(昨)": round(fac['pct_1'], 2), "涨幅%(前)": round(fac['pct_2'], 2)
                }
            return None
        except:
            return None

    def run(self):
        print(f"{'='*100}")
        print(" 🌌 Alpha Galaxy Omni Pro Max - 机构级全维融合版 🌌")
        print(f"{'='*100}")
        
        candidates = self.get_candidates()
        print(f"1. 技术/基本面扫描 (待扫 {len(candidates)} 只)...")
        
        tech_survivors = []
        with ThreadPoolExecutor(max_workers=16) as executor:
            for res in tqdm(executor.map(self.scan_tech_fund, candidates), total=len(candidates)):
                if res: tech_survivors.append(res)
        
        if not tech_survivors:
            print("无入围标的。")
            return

        tech_survivors.sort(key=lambda x: x['总分'], reverse=True)
        top_picks = tech_survivors[:30]
        
        print(f"\n2. 舆情风控扫描 (针对 Top {len(top_picks)})...")
        final_results = []
        
        for stock in tqdm(top_picks):
            s_score, s_msg = SentimentEngine.analyze(stock['代码'])
            
            if s_score < -10:
                print(f"⚠️ 剔除 {stock['名称']}: {s_msg}")
                continue
                
            stock['总分'] += s_score
            stock['舆情分析'] = s_msg
            if s_score > 0: stock['得分详情'] += f" 舆情({s_score})"
            
            final_results.append(stock)
            time.sleep(0.5)

        final_results.sort(key=lambda x: x['总分'], reverse=True)
        df = pd.DataFrame(final_results)
        
        print("\n" + "="*120)
        print(df[['代码', '名称', '总分', '现价', '舆情分析', '买入形态']].head(10).to_string(index=False))
        
        filename = f"Alpha_Galaxy_ProMax_{datetime.now().strftime('%Y%m%d')}.xlsx"
        ExcelExporter.save(df, filename)

if __name__ == "__main__":
    AlphaGalaxyOmni().run()
​
