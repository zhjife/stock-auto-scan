# -*- coding: utf-8 -*-
"""
Alpha Galaxy Omni Pro Max - 机构全维量化系统 (最终修复版 - 宽进严出策略)
Features: 
1. [Core Fix] 雪球备用源：移除换手率和价格上限过滤，确保能获取到数据，避免因"涨幅榜高换手"导致全军覆没。
2. [Optimization] 东方财富：强制 while 循环翻页，直至取完数据。
3. 完整保留所有策略与导出功能。
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
import json
import random
import re

# === 引入 Playwright ===
try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("❌ 缺少 playwright 库，请先运行: pip install playwright && playwright install chromium")
    exit(1)

warnings.filterwarnings('ignore')

# ==========================================
# 1. 舆情分析 (保持不变)
# ==========================================
class SentimentEngine:
    @staticmethod
    def analyze(symbol):
        try:
            news_df = ak.stock_news_em(symbol=symbol)
            if news_df is None or news_df.empty: return 0, "无近期舆情"
            recent_news = news_df.head(10)
            titles = recent_news['新闻标题'].tolist()
            full_text = "。".join(titles)
            pos_kw = ['增长', '预增', '突破', '利好', '回购', '获批', '中标', '大涨', '新高']
            neg_kw = ['立案', '调查', '亏损', '减持', '警示', '违规', '大跌', '退市', '被查']
            hard_score = 0
            keywords = []
            for t in titles:
                for kw in pos_kw:
                    if kw in t: hard_score += 2; keywords.append(kw)
                for kw in neg_kw:
                    if kw in t: hard_score -= 10; keywords.append(kw)
            s = SnowNLP(full_text)
            soft_score = (s.sentiments - 0.5) * 10
            total_score = max(min(hard_score + soft_score, 20), -20)
            return round(total_score, 1), f"关键词:{list(set(keywords))}" if keywords else "舆情平稳"
        except: return 0, "舆情获取失败"

# ==========================================
# 2. K线形态 (保持不变)
# ==========================================
class KLineStrictLib:
    @staticmethod
    def detect(df):
        if len(df) < 30: return 0, [], []
        c = df['close']; o = df['open']; h = df['high']; l = df['low']; v = df['volume']
        ma5, ma10, ma20 = df['ma5'], df['ma10'], df['ma20']
        body = np.abs(c - o); upper_s = h - np.maximum(c, o); lower_s = np.minimum(c, o) - l
        avg_body = body.rolling(10).mean()
        def get(s, i): return s.iloc[i]
        buy_pats, risk_pats = [], []
        score = 0
        
        # 买入
        if (get(c,-3)<get(o,-3)) and (get(body,-3)>get(avg_body,-3)) and (get(h,-2)<get(l,-3)) and (get(c,-1)>get(o,-1)) and (get(c,-1)>(get(o,-3)+get(c,-3))/2): buy_pats.append("早晨之星"); score += 20
        if (get(l,-1)==l.iloc[-5:].min()) and (get(lower_s,-1)>=2*get(body,-1)) and (get(upper_s,-1)<=0.1*get(body,-1)): buy_pats.append("锤子线"); score += 15
        if (get(l,-1)==l.iloc[-5:].min()) and (get(upper_s,-1)>=2*get(body,-1)) and (get(lower_s,-1)<=0.1*get(body,-1)): buy_pats.append("倒锤头"); score += 10
        if (get(c,-2)<get(o,-2)) and (get(c,-1)>get(o,-1)) and (get(o,-1)<get(c,-2)) and (get(c,-1)>get(o,-2)): buy_pats.append("阳包阴"); score += 20
        if (get(c,-2)<get(o,-2)) and (get(body,-2)>get(avg_body,-2)) and (get(o,-1)<get(l,-2)) and (get(c,-1)>(get(o,-2)+get(c,-2))/2): buy_pats.append("曙光初现"); score += 15
        if abs(get(l,-1)-get(l,-2)) < (get(c,-1)*0.003) and (get(l,-1) <= l.iloc[-10:].min()): buy_pats.append("平底"); score += 15
        if (get(c,-2)<get(o,-2)) and (get(body,-2)>get(avg_body,-2)) and (get(c,-1)>get(o,-1)) and (get(h,-1)<get(h,-2)) and (get(l,-1)>get(l,-2)): buy_pats.append("多头孕线"); score += 15
        if (get(c,-2)<get(o,-2)) and (get(body,-2)>get(avg_body,-2)*1.2) and (get(o,-1)>get(c,-2)) and (get(c,-1)>get(o,-2)): buy_pats.append("旭日东升"); score += 25
        if (get(h,-2) < get(l,-3)) and (get(l,-1) > get(h,-2)): buy_pats.append("岛形反转(底)"); score += 35
        if (get(upper_s,-1) == 0) and (get(lower_s,-1) > 0) and (get(c,-1)>get(o,-1)) and (get(o,-1) > get(h,-2)): buy_pats.append("踢脚线"); score += 20
        if (get(l,-1) <= get(ma20,-1)) and (min(get(o,-1), get(c,-1)) > get(ma20,-1)) and (get(c,-1)>get(o,-1)): buy_pats.append("蜻蜓点水"); score += 15
        if (get(c,-3)>get(o,-3)) and (get(c,-2)>get(o,-2)) and (get(c,-1)>get(o,-1)) and (get(c,-1)>get(c,-2)>get(c,-3)): buy_pats.append("红三兵"); score += 15
        if (get(c,-5)>get(o,-5)) and (get(body,-5)>get(avg_body,-5)) and (get(c,-4)<get(o,-4)) and (get(c,-3)<get(o,-3)) and (get(c,-2)<get(o,-2)) and (get(c,-1)>get(o,-1)) and (get(c,-1)>get(c,-5)): buy_pats.append("上升三法"); score += 25
        if (get(c,-3)>get(o,-3)) and (get(c,-2)<get(o,-2)) and (get(c,-1)>get(o,-1)) and (get(c,-1)>get(c,-3)): buy_pats.append("多方炮"); score += 20
        if get(l,-1)>get(h,-2): buy_pats.append("向上缺口"); score += 15
        if (get(c,-1)>max(get(ma5,-1),get(ma10,-1),get(ma20,-1))) and (get(o,-1)<min(get(ma5,-1),get(ma10,-1),get(ma20,-1))): buy_pats.append("一阳穿三线"); score += 25
        if (get(v,-1)>get(v,-2)*1.9) and (get(c,-1)>=c.iloc[-20:].max()): buy_pats.append("倍量过左峰"); score += 20
        diff = max(get(ma5,-1),get(ma10,-1),get(ma20,-1)) - min(get(ma5,-1),get(ma10,-1),get(ma20,-1))
        if (diff/get(c,-1)<0.015) and (get(c,-1)>get(ma5,-1)) and (get(c,-1)>get(o,-1)): buy_pats.append("金蜘蛛"); score += 15
        if (get(upper_s,-2)>get(body,-2)) and (get(c,-1)>get(h,-2)) and (get(c,-1)>get(o,-1)): buy_pats.append("仙人指路"); score += 15

        # 卖出
        if (get(c,-3)>get(o,-3)) and (get(l,-2)>get(h,-3)) and (get(c,-1)<get(o,-1)) and (get(c,-1)<(get(o,-3)+get(c,-3))/2): risk_pats.append("风险:黄昏之星"); score -= 30
        if (get(c,-2)>get(o,-2)) and (get(c,-1)<get(o,-1)) and (get(o,-1)>get(h,-2)) and (get(c,-1)<(get(o,-2)+get(c,-2))/2): risk_pats.append("风险:乌云盖顶"); score -= 25
        if (get(c,-2)>get(o,-2)) and (get(c,-1)<get(o,-1)) and (get(o,-1)>get(c,-2)) and (get(c,-1)<get(o,-2)): risk_pats.append("风险:阴包阳"); score -= 25
        if (get(c,-1)<get(o,-1)) and (get(c,-2)<get(o,-2)) and (get(c,-3)<get(o,-3)): risk_pats.append("风险:三只乌鸦"); score -= 30
        if (get(upper_s,-1)>2*get(body,-1)) and (get(lower_s,-1)<0.1*get(body,-1)) and (get(c,-1)>get(c,-20)*1.15): risk_pats.append("风险:射击之星"); score -= 20
        if (get(lower_s,-1)>2*get(body,-1)) and (get(upper_s,-1)<0.1*get(body,-1)) and (get(c,-1)>get(c,-20)*1.15): risk_pats.append("风险:吊颈线"); score -= 20
        if (get(c,-1)<min(get(ma5,-1),get(ma10,-1),get(ma20,-1))) and (get(o,-1)>max(get(ma5,-1),get(ma10,-1),get(ma20,-1))): risk_pats.append("风险:断头铡刀"); score -= 40
        if get(h,-1)<get(l,-2): risk_pats.append("风险:向下缺口"); score -= 20
        if (get(c,-2)>get(o,-2)) and (get(o,-1)<get(c,-2)) and (get(c,-1)<get(o,-2)) and (get(c,-1)<get(o,-1)): risk_pats.append("风险:倾盆大雨"); score -= 25
        if (get(c,-2)>get(o,-2)) and (get(body,-2)>get(avg_body,-2)) and (get(c,-1)<get(o,-1)) and (get(h,-1)<get(h,-2)) and (get(l,-1)>get(l,-2)) and (get(c,-1)>get(c,-20)*1.1): risk_pats.append("风险:空头孕线"); score -= 20
        if (get(l,-2) > get(h,-3)) and (get(h,-1) < get(l,-2)): risk_pats.append("风险:岛形反转(顶)"); score -= 50
        if (get(body,-1) < 0.005*get(c,-1)) and (get(upper_s,-1) > 3*get(body,-1)) and (get(lower_s,-1) < get(body,-1)) and (get(c,-1) > get(c,-20)*1.2): risk_pats.append("风险:墓碑线"); score -= 30
        return score, buy_pats, risk_pats

# ==========================================
# 3. 指标计算 (保持不变)
# ==========================================
class IndicatorEngine:
    @staticmethod
    def calculate(df):
        if len(df) < 60: return None
        c = df['close']; h = df['high']; l = df['low']; v = df['volume']
        ma5=c.rolling(5).mean(); ma10=c.rolling(10).mean(); ma20=c.rolling(20).mean(); ma60=c.rolling(60).mean()
        df['ma5'], df['ma10'], df['ma20'] = ma5, ma10, ma20
        vol_ma5 = v.rolling(5).mean(); vol_ratio = v / vol_ma5.replace(0, 1)
        mf_mult = ((c - l) - (h - c)) / (h - l).replace(0, 0.01)
        cmf_series = (mf_mult * v).rolling(20).sum() / v.rolling(20).sum()
        low_min = l.rolling(9).min(); high_max = h.rolling(9).max()
        rsv = (c - low_min) / (high_max - low_min) * 100
        K = rsv.ewm(com=2, adjust=False).mean(); D = K.ewm(com=2, adjust=False).mean(); J = 3 * K - 2 * D
        std20 = c.rolling(20).std(); boll_up = ma20 + 2 * std20; boll_low = ma20 - 2 * std20
        bb_width = (boll_up - boll_low) / ma20; bias = (c - ma20) / ma20 * 100
        tp = (h + l + c) / 3; cci = (tp - tp.rolling(14).mean()) / (0.015 * tp.rolling(14).apply(lambda x: np.mean(np.abs(x - np.mean(x))), raw=True))
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
        dif = exp12 - exp26; dea = dif.ewm(span=9, adjust=False).mean(); macd_bar = 2 * (dif - dea)
        curr = df.iloc[-1]; pct_change = c.pct_change() * 100
        return {
            'close': curr['close'], 'ma20': ma20.iloc[-1], 'ma60': ma60.iloc[-1],
            'atr': atr.iloc[-1], 'adx': adx.iloc[-1], 
            'macd_dif': dif.iloc[-1], 'macd_dea': dea.iloc[-1],
            'dif_0': dif.iloc[-1], 'dif_1': dif.iloc[-2], 'dea_0': dea.iloc[-1], 'dea_1': dea.iloc[-2], 'macd_bar_0': macd_bar.iloc[-1], 'macd_bar_1': macd_bar.iloc[-2],
            'cci': cci.iloc[-1], 'rsi': rsi.iloc[-1], 
            'j_val': J.iloc[-1], 'k_0': K.iloc[-1], 'k_1': K.iloc[-2], 'd_0': D.iloc[-1], 'd_1': D.iloc[-2],
            'bias': bias.iloc[-1], 'bb_width': bb_width.iloc[-1], 'bb_up': boll_up.iloc[-1], 'bb_low': boll_low.iloc[-1],
            'cmf_0': cmf_series.iloc[-1], 'cmf_1': cmf_series.iloc[-2], 'cmf_2': cmf_series.iloc[-3],
            'pct_0': pct_change.iloc[-1], 'pct_1': pct_change.iloc[-2], 'pct_2': pct_change.iloc[-3], 'vol_ratio': vol_ratio.iloc[-1] 
        }

# ==========================================
# 4. Excel 导出 (保持不变)
# ==========================================
class ExcelExporter:
    @staticmethod
    def save(df_data, filename):
        if df_data.empty: return
        print(f"正在生成 Excel 报表: {filename} ...")
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            cols = ['代码', '名称', '总分', '现价', '建议买入区间', '止损价', '止盈价', 
                '买入形态', '风险形态', '舆情分析', '得分详情', 'MACD状态', 'KDJ状态', 
                '换手率%', '量比', '市盈率', '市净率', 'J值', 'RSI', 'BIAS(%)', '布林带宽', 'ADX', 'CCI', 
                'CMF(今)', 'CMF(昨)', 'CMF(前)', '涨幅%(今)', '涨幅%(昨)', '涨幅%(前)']
            df_export = df_data[[c for c in cols if c in df_data.columns]]
            df_export.to_excel(writer, sheet_name='选股结果', index=False)
            
            patterns_desc = [
                ['形态名称', '类型', '大白话说明'],
                ['早晨之星', '买入-反转', '底部三日组合：阴线+星线+阳线，强力见底'],
                ['锤子线', '买入-反转', '底部长下影线，主力试盘后拉回，支撑强'],
                ['倒锤头', '买入-反转', '底部长上影线，主力低位试盘，抛压减轻'],
                ['阳包阴', '买入-反转', '今日阳线完全包住昨日阴线，多头反击'],
                ['曙光初现', '买入-反转', '大阴线后低开高走，阳线刺入阴线一半'],
                ['平底', '买入-反转', '两日最低价相同，筑底成功'],
                ['多头孕线', '买入-反转', '长阴包含小K线，底部孕育，变盘在即'],
                ['旭日东升', '买入-强反转', '大阴线后高开高走，收盘价高于前日开盘'],
                ['岛形反转(底)', '买入-强反转', '下跌缺口+盘整+上涨缺口，超强反转'],
                ['踢脚线', '买入-强反转', '大阴线后直接高开高走，无上影，主力暴力反转'],
                ['蜻蜓点水', '买入-技巧', '股价回踩均线(MA20/30)后立即弹起'],
                ['红三兵', '买入-攻击', '连续三天阳线稳步推升'],
                ['上升三法', '买入-持续', '大阳后接三小阴不破低，再接大阳'],
                ['多方炮', '买入-攻击', '阳阴阳组合，洗盘结束，再次上攻'],
                ['向上缺口', '买入-强势', '向上跳空不回补，主力强势特征'],
                ['一阳穿三线', '买入-突破', '大阳线同时突破5/10/20均线'],
                ['倍量过左峰', '买入-突破', '成交量翻倍且价格突破前期高点'],
                ['金蜘蛛', '买入-突破', '均线粘合后放量向上发散'],
                ['仙人指路', '买入-试盘', '今日大阳线突破昨日的长上影线'],
                ['黄昏之星', '卖出-风险', '顶部三日组合：阳线+星线+阴线'],
                ['乌云盖顶', '卖出-风险', '大阳后接大阴，吃掉一半涨幅'],
                ['阴包阳', '卖出-风险', '空头吞噬，阴线包住阳线'],
                ['三只乌鸦', '卖出-风险', '连续三根阴线杀跌'],
                ['射击之星', '卖出-风险', '高位长上影线，冲高回落'],
                ['吊颈线', '卖出-风险', '高位长下影线，主力诱多'],
                ['断头铡刀', '卖出-风险', '一阴断多线，趋势崩塌'],
                ['向下缺口', '卖出-风险', '向下跳空不回补，极弱势'],
                ['倾盆大雨', '卖出-风险', '低开低走大阴线，吞没前日涨幅'],
                ['空头孕线', '卖出-风险', '高位长阳包含小K线，滞涨信号'],
                ['岛形反转(顶)', '卖出-风险', '上涨缺口+盘整+下跌缺口，见顶信号'],
                ['墓碑线', '卖出-风险', '高位T字线，多头力竭'],
                ['黄金坑', '买入-机会', '跌破布林下轨且主力资金逆势进场']
            ]
            pd.DataFrame(patterns_desc[1:], columns=patterns_desc[0]).to_excel(writer, sheet_name='形态图解', index=False)
            
            indicators_desc = [
                ['指标名称', '实战含义', '判断标准'],
                ['量比', '量能变化', '>1.5为放量；0.5-1.0为缩量(锁筹)'],
                ['市盈率(PE)', '估值', '0<PE<20为低估值(优)；PE<0为亏损(差)'],
                ['市净率(PB)', '资产价格', 'PB>10可能高估'],
                ['CMF', '资金流', '正值越大说明主力吸筹越明显'],
                ['J值 (KDJ)', '超买超卖', 'J<0为超卖(抄底)，J>100为超买(风险)'],
                ['布林带宽', '变盘前兆', '数值越小(<0.10)说明筹码越集中，即将变盘'],
                ['BIAS', '乖离率', '正值过大要回调，负值过大有反弹'],
                ['ADX', '趋势强度', '>25表示趋势强劲；<20表示震荡'],
                ['RSI', '强弱指标', '50-80为强势区，>80过热'],
                ['换手率', '活跃度', '3%-10%健康；>15%且滞涨则危险'],
                ['CCI', '爆发力', '>100表示加速'],
                ['MACD状态', '趋势判断', '红柱伸长表加速上涨，绿柱缩短表止跌反弹'],
                ['KDJ状态', '短线买卖', '低位金叉为买点，高位死叉为卖点']
            ]
            pd.DataFrame(indicators_desc[1:], columns=indicators_desc[0]).to_excel(writer, sheet_name='指标说明书', index=False)
        print(f"✅ Excel 文件已保存至: {filename}")

# ==========================================
# 5. 策略主控 (重点修改：宽进严出)
# ==========================================
class AlphaGalaxyOmni:
    def __init__(self):
        self.min_cap = 40 * 10000 * 10000 

    # [Source 1] 东方财富 (修复：while 循环翻页)
    def fetch_from_eastmoney(self, page):
        api_nodes = [
            "push2.eastmoney.com", "4.push2.eastmoney.com", "19.push2.eastmoney.com",
            "26.push2.eastmoney.com", "6.push2.eastmoney.com", "82.push2.eastmoney.com"
        ]
        # 使用 pz=100 (每页100条)
        base_url = "https://{DOMAIN}/api/qt/clist/get?pn={PAGE}&pz=100&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048&fields=f12,f14,f2,f3,f8,f9,f20,f23"
        
        for node in api_nodes:
            print(f"   ↳ [EastMoney] 尝试连接节点: {node} ...")
            current_node_data = []
            page_num = 1
            node_failed = False
            
            while True: # 循环翻页
                target_url = base_url.format(DOMAIN=node, PAGE=page_num)
                try:
                    response = page.goto(target_url, timeout=8000, wait_until='domcontentloaded')
                    if response.status == 200:
                        json_text = response.text()
                        if not json_text: node_failed=True; break
                        
                        data_json = json.loads(json_text)
                        if 'data' in data_json and 'diff' in data_json['data']:
                            raw_data = data_json['data']['diff']
                            if not raw_data: break # 无更多数据
                            
                            for item in raw_data:
                                try:
                                    code = str(item.get('f12', ''))
                                    name = str(item.get('f14', ''))
                                    price = float(item.get('f2', 0))
                                    pe = float(item.get('f9', -1))
                                    pb = float(item.get('f23', -1))
                                    turnover = float(item.get('f8', 0))
                                    cap = float(item.get('f20', 0))
                                    
                                    # 东方财富的逻辑保持略严，因为数据量大
                                    if (not code.startswith(('30', '688', '8', '4'))) and \
                                       ('ST' not in name) and ('退' not in name) and \
                                       (cap > self.min_cap) and (price > 3.0) and \
                                       (turnover > 1.0) and (turnover < 20):
                                            current_node_data.append((code, name, pe, pb, turnover))
                                except: continue
                            
                            page_num += 1
                            # 简单的进度提示
                            if page_num % 10 == 0: print(f"     ...已读取 {page_num} 页 (累计 {len(current_node_data)} 条)...")
                        else: node_failed=True; break
                    else: node_failed=True; break
                except Exception: node_failed=True; break
            
            if not node_failed and len(current_node_data) > 100:
                print(f"     ✅ 东方财富获取成功: 共 {len(current_node_data)} 条有效数据")
                return current_node_data
            elif len(current_node_data) > 100:
                print(f"     ⚠️ 节点中断但已获取 {len(current_node_data)} 条，保留使用")
                return current_node_data
                
        return []

    # [Source 2] 雪球 (修复：移除所有严格过滤，只做基础清洗)
    def fetch_from_xueqiu(self, page):
        print("   ↳ [Xueqiu] 启动备份数据源 (自动翻页 + 宽进严出)...")
        data_list = []
        try:
            page.goto("https://xueqiu.com", timeout=20000, wait_until='domcontentloaded')
            time.sleep(2) 
            current_page = 1
            max_page = 60
            page_size = 90 
            
            while current_page <= max_page:
                xq_url = f"https://xueqiu.com/service/v5/stock/screener/quote/list?page={current_page}&size={page_size}&order=desc&order_by=percent&exchange=CN&market=CN&type=sha,shb,sza,szb"
                try:
                    response = page.goto(xq_url, timeout=10000, wait_until='domcontentloaded')
                    if response.status != 200: break
                    json_data = response.json()
                    if 'data' not in json_data or 'list' not in json_data['data']: break
                    raw_list = json_data['data']['list']
                    if not raw_list: break
                    
                    for item in raw_list:
                        try:
                            raw_code = str(item.get('symbol', ''))
                            code = re.sub(r'^[A-Za-z]+', '', raw_code)
                            name = str(item.get('name', ''))
                            price = float(item.get('current', 0))
                            pe = float(item.get('pe_ttm') or -1)
                            pb = float(item.get('pb') or -1)
                            turnover = float(item.get('turnover_rate') or 0)
                            cap = float(item.get('market_capital') or 0)
                            
                            # [关键修改]：移除换手率上限(turnover < 20)和价格过滤
                            # 只保留最基础的过滤：非科创板/北交所，非ST，非微盘股
                            # 这样可以先把数据拿下来，后续 scan_tech_fund 再去仔细筛选
                            if (not code.startswith(('30', '688', '8', '4'))) and \
                               ('ST' not in name) and ('退' not in name) and \
                               (cap > self.min_cap):
                                    data_list.append((code, name, pe, pb, turnover))
                        except: continue
                        
                    current_page += 1
                    time.sleep(0.5) 
                except: break
                    
            print(f"     ✅ 雪球数据获取结束: 共 {len(data_list)} 条")
            return data_list
        except: return []

    # [Core] 主获取逻辑
    def get_candidates(self):
        print("1. 启动 Playwright 浏览器获取市场快照 (双源保障模式)...")
        data_list = []
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True, 
                    args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-blink-features=AutomationControlled']
                )
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                    ignore_https_errors=True
                )
                page = context.new_page()

                # 1. 尝试东方财富
                data_list = self.fetch_from_eastmoney(page)
                
                # 2. 如果失败，尝试雪球
                if not data_list:
                    print("⚠️ 东方财富节点全灭，切换至雪球接口...")
                    data_list = self.fetch_from_xueqiu(page)

                browser.close()
        except Exception as e:
            print(f"❌ Playwright 致命错误: {e}")
            return []
            
        print(f"   ✅ 最终清洗后剩余候选股: {len(data_list)} 只")
        return data_list

    def scan_tech_fund(self, args):
        symbol, name, pe, pb, turnover = args
        try:
            if pe < 0: return None
            end = datetime.now().strftime("%Y%m%d")
            start = (datetime.now() - timedelta(days=400)).strftime("%Y%m%d")
            
            df = None
            for _ in range(2):
                try:
                    df = ak.stock_zh_a_hist(symbol=symbol, period='daily', start_date=start, end_date=end, adjust='qfq')
                    if df is not None and not df.empty: break
                except: time.sleep(0.5)
            
            if df is None or df.empty: return None
            df.rename(columns={'日期':'date', '开盘':'open', '收盘':'close', '最高':'high', '最低':'low', '成交量':'volume'}, inplace=True)
            
            fac = IndicatorEngine.calculate(df)
            if not fac: return None
            k_score, buy_pats, risk_pats = KLineStrictLib.detect(df)
            
            score = 0
            logic = []
            
            if risk_pats: score -= 30
            
            dif0, dea0, dif1, dea1 = fac['dif_0'], fac['dea_0'], fac['dif_1'], fac['dea_1']
            bar0, bar1 = fac['macd_bar_0'], fac['macd_bar_1']
            macd_cross = "金叉(新)" if (dif0>dea0 and dif1<=dea1) else ("死叉(新)" if (dif0<dea0 and dif1>=dea1) else ("金叉持仓" if dif0>dea0 else "死叉持币"))
            bar_status = ("红柱伸长" if bar0>bar1 else "红柱缩短") if bar0>0 else ("绿柱缩短" if bar0>bar1 else "绿柱伸长")
            macd_full_status = f"{macd_cross} | {bar_status}"

            k0, d0, k1, d1 = fac['k_0'], fac['d_0'], fac['k_1'], fac['d_1']
            kdj_status = "金叉(新)" if (k0>d0 and k1<=d1) else ("死叉(新)" if (k0<d0 and k1>=d1) else ("多头" if k0>d0 else "空头"))

            is_trend_up = fac['close'] > fac['ma20']
            
            # [策略判断] 
            # A: 主力意图
            if is_trend_up and (1 < turnover < 5) and (0.5 < fac['vol_ratio'] < 1.2):
                score += 20; logic.append("A:主力锁筹")
            elif is_trend_up and (fac['vol_ratio'] > 1.5) and (fac['pct_0'] > 0):
                score += 15; logic.append("A:放量启动")
            if (turnover > 15) and (-2 < fac['pct_0'] < 2):
                score -= 30; logic.append("A:⚠️高换手滞涨")

            # B: MACD+RSI
            if (fac['macd_dif'] > fac['macd_dea']) and (fac['macd_dif'] > 0):
                if fac['rsi'] < 80: score += 10; logic.append("B:共振")
                else: score -= 5; logic.append("B:RSI过热")
            
            # C: 黄金坑
            if (fac['close'] < fac['bb_low']) and (fac['cmf_0'] > 0.1):
                score += 40; logic.append("C:黄金坑")
            if (fac['close'] > fac['bb_up']) and (fac['cmf_0'] < -0.05):
                score -= 40; logic.append("C:⚠️顶背离")

            if 0 < pe <= 25: score += 10
            if pb > 10: score -= 5
            if fac['adx'] > 25 and is_trend_up: score += 5
            if k_score > 0: score += k_score

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
                    "MACD状态": macd_full_status, "KDJ状态": kdj_status,
                    "J值": round(fac['j_val'], 1), "布林带宽": round(fac['bb_width'], 3),
                    "RSI": round(fac['rsi'], 1), "BIAS(%)": round(fac['bias'], 2),
                    "ADX": int(fac['adx']), "CCI": int(fac['cci']),
                    "CMF(今)": round(fac['cmf_0'], 3), "CMF(昨)": round(fac['cmf_1'], 3), "CMF(前)": round(fac['cmf_2'], 3),
                    "涨幅%(今)": round(fac['pct_0'], 2), "涨幅%(昨)": round(fac['pct_1'], 2), "涨幅%(前)": round(fac['pct_2'], 2)
                }
            return None
        except: return None

    def run(self):
        print(f"{'='*100}")
        print(" 🌌 Alpha Galaxy Omni - Dual Source Edition (EastMoney + Xueqiu) 🌌")
        print(f"{'='*100}")
        
        candidates = self.get_candidates()
        
        if not candidates:
            print("❌ 所有数据源均不可用，程序退出。")
            return

        print(f"2. 技术/基本面扫描 (待扫 {len(candidates)} 只)...")
        
        tech_survivors = []
        with ThreadPoolExecutor(max_workers=16) as executor:
            for res in tqdm(executor.map(self.scan_tech_fund, candidates), total=len(candidates)):
                if res: tech_survivors.append(res)
        
        if not tech_survivors:
            print("无入围标的。")
            return

        tech_survivors.sort(key=lambda x: x['总分'], reverse=True)
        top_picks = tech_survivors[:30]
        
        print(f"\n3. 舆情风控扫描 (针对 Top {len(top_picks)})...")
        final_results = []
        
        for stock in tqdm(top_picks):
            s_score, s_msg = SentimentEngine.analyze(stock['代码'])
            if s_score < -10: continue
            stock['总分'] += s_score
            stock['舆情分析'] = s_msg
            if s_score > 0: stock['得分详情'] += f" 舆情({s_score})"
            final_results.append(stock)

        final_results.sort(key=lambda x: x['总分'], reverse=True)
        df = pd.DataFrame(final_results)
        
        print("\n" + "="*120)
        if not df.empty:
            print(df[['代码', '名称', '总分', '现价', 'MACD状态', 'KDJ状态']].head(10).to_string(index=False))
            filename = f"Alpha_Galaxy_DualSource_{datetime.now().strftime('%Y%m%d')}.xlsx"
            ExcelExporter.save(df, filename)
        else:
            print("没有生成结果。")

if __name__ == "__main__":
    AlphaGalaxyOmni().run()
