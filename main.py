# -*- coding: utf-8 -*-
"""
Alpha Galaxy Omni Pro Max - GitHub Action 稳定版 (多节点容灾修复)
Features: 
1. [Core Fix] 引入东方财富多节点轮询机制 (解决 ERR_EMPTY_RESPONSE 问题)
2. [Strategy] 完整保留 A+B+C 策略、K线形态、NLP
3. [Export] 完整 Excel 导出
"""

import akshare as ak
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import warnings
from datetime import datetime, timedelta
from snownlp import SnowNLP
import time
import json
import random

# === 引入 Playwright ===
try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("❌ 缺少 playwright 库，请确保 workflow 中执行了 pip install playwright 和 playwright install chromium")
    exit(1)

# 配置
warnings.filterwarnings('ignore')

# ==========================================
# 1. 舆情分析引擎
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
        except:
            return 0, "舆情分析跳过"

# ==========================================
# 2. 严谨K线形态识别引擎
# ==========================================
class KLineStrictLib:
    @staticmethod
    def detect(df):
        if len(df) < 30: return 0, [], []
        c = df['close']; o = df['open']; h = df['high']; l = df['low']; v = df['volume']
        ma5, ma10, ma20 = df['ma5'], df['ma10'], df['ma20']
        body = np.abs(c - o)
        upper_s = h - np.maximum(c, o); lower_s = np.minimum(c, o) - l
        avg_body = body.rolling(10).mean()
        def get(s, i): return s.iloc[i]
        
        buy_pats, risk_pats = [], []
        score = 0
        
        # 买入形态
        if (get(c,-3)<get(o,-3)) and (get(body,-3)>get(avg_body,-3)) and (get(h,-2)<get(l,-3)) and (get(c,-1)>get(o,-1)) and (get(c,-1)>(get(o,-3)+get(c,-3))/2):
            buy_pats.append("早晨之星"); score += 20
        if (get(l,-1)==l.iloc[-5:].min()) and (get(lower_s,-1)>=2*get(body,-1)) and (get(upper_s,-1)<=0.1*get(body,-1)):
            buy_pats.append("锤子线"); score += 15
        if (get(c,-2)<get(o,-2)) and (get(c,-1)>get(o,-1)) and (get(o,-1)<get(c,-2)) and (get(c,-1)>get(o,-2)):
            buy_pats.append("阳包阴"); score += 20
        if (get(c,-2)<get(o,-2)) and (get(body,-2)>get(avg_body,-2)*1.2) and (get(o,-1)>get(c,-2)) and (get(c,-1)>get(o,-2)):
            buy_pats.append("旭日东升"); score += 25
        if (get(h,-2) < get(l,-3)) and (get(l,-1) > get(h,-2)): 
            buy_pats.append("岛形反转(底)"); score += 35
        if (get(v,-1)>get(v,-2)*1.9) and (get(c,-1)>=c.iloc[-20:].max()):
            buy_pats.append("倍量过左峰"); score += 20
        if (get(c,-1)>max(get(ma5,-1),get(ma10,-1),get(ma20,-1))) and (get(o,-1)<min(get(ma5,-1),get(ma10,-1),get(ma20,-1))):
            buy_pats.append("一阳穿三线"); score += 25

        # 卖出形态
        if (get(c,-3)>get(o,-3)) and (get(l,-2)>get(h,-3)) and (get(c,-1)<get(o,-1)) and (get(c,-1)<(get(o,-3)+get(c,-3))/2):
            risk_pats.append("风险:黄昏之星"); score -= 30
        if (get(c,-2)>get(o,-2)) and (get(c,-1)<get(o,-1)) and (get(o,-1)>get(h,-2)) and (get(c,-1)<(get(o,-2)+get(c,-2))/2):
            risk_pats.append("风险:乌云盖顶"); score -= 25
        if (get(c,-1)<min(get(ma5,-1),get(ma10,-1),get(ma20,-1))) and (get(o,-1)>max(get(ma5,-1),get(ma10,-1),get(ma20,-1))):
            risk_pats.append("风险:断头铡刀"); score -= 40
        
        return score, buy_pats, risk_pats

# ==========================================
# 3. 高级指标计算引擎
# ==========================================
class IndicatorEngine:
    @staticmethod
    def calculate(df):
        if len(df) < 60: return None
        c = df['close']; h = df['high']; l = df['low']; v = df['volume']
        
        ma20=c.rolling(20).mean(); df['ma5']=c.rolling(5).mean(); df['ma10']=c.rolling(10).mean(); df['ma20']=ma20
        
        vol_ratio = v / v.rolling(5).mean().replace(0, 1)
        mf_mult = ((c - l) - (h - c)) / (h - l).replace(0, 0.01)
        cmf_series = (mf_mult * v).rolling(20).sum() / v.rolling(20).sum()
        
        low_min = l.rolling(9).min(); high_max = h.rolling(9).max()
        rsv = (c - low_min) / (high_max - low_min) * 100
        K = rsv.ewm(com=2, adjust=False).mean(); D = K.ewm(com=2, adjust=False).mean(); J = 3 * K - 2 * D
        
        std20 = c.rolling(20).std(); boll_up = ma20 + 2 * std20; boll_low = ma20 - 2 * std20
        bb_width = (boll_up - boll_low) / ma20
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
        macd_bar = 2 * (dif - dea)

        curr = df.iloc[-1]
        pct_change = c.pct_change() * 100
        
        return {
            'close': curr['close'], 'ma20': ma20.iloc[-1],
            'atr': atr.iloc[-1], 'adx': adx.iloc[-1], 
            'macd_dif': dif.iloc[-1], 'macd_dea': dea.iloc[-1],
            'dif_0': dif.iloc[-1], 'dif_1': dif.iloc[-2], 'dea_0': dea.iloc[-1], 'dea_1': dea.iloc[-2],
            'macd_bar_0': macd_bar.iloc[-1], 'macd_bar_1': macd_bar.iloc[-2],
            'cci': cci.iloc[-1], 'rsi': rsi.iloc[-1], 
            'j_val': J.iloc[-1], 'k_0': K.iloc[-1], 'k_1': K.iloc[-2], 'd_0': D.iloc[-1], 'd_1': D.iloc[-2],
            'bias': bias.iloc[-1], 'bb_width': bb_width.iloc[-1], 'bb_up': boll_up.iloc[-1], 'bb_low': boll_low.iloc[-1],
            'cmf_0': cmf_series.iloc[-1], 'cmf_1': cmf_series.iloc[-2], 'cmf_2': cmf_series.iloc[-3],
            'pct_0': pct_change.iloc[-1], 'vol_ratio': vol_ratio.iloc[-1] 
        }

# ==========================================
# 4. Excel 导出
# ==========================================
class ExcelExporter:
    @staticmethod
    def save(df_data, filename):
        if df_data.empty: return
        print(f"💾 生成报告: {filename}")
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            cols = ['代码', '名称', '总分', '现价', '建议买入区间', '止损价', '止盈价', 
                    '买入形态', '风险形态', '舆情分析', '得分详情', 'MACD状态', 'KDJ状态', 
                    '换手率%', '量比', '市盈率', '市净率', 'J值', 'RSI', '布林带宽', 'CMF(今)', '涨幅%(今)']
            df_export = df_data[[c for c in cols if c in df_data.columns]]
            df_export.to_excel(writer, sheet_name='选股结果', index=False)
        print("✅ 完成。")

# ==========================================
# 5. 核心逻辑 (多节点容灾版)
# ==========================================
class AlphaGalaxyOmni:
    def __init__(self):
        self.min_cap = 40 * 10000 * 10000 

    def get_candidates_via_playwright(self):
        print("1. 启动 Playwright 浏览器获取市场快照 (多节点轮询模式)...")
        
        # 定义多个节点，如果一个被封或 empty response，就试下一个
        # 82.push2 经常在 GitHub 环境报错，我们放到最后
        api_nodes = [
            "push2.eastmoney.com",      # 官方主域名
            "4.push2.eastmoney.com",
            "19.push2.eastmoney.com",
            "26.push2.eastmoney.com",
            "6.push2.eastmoney.com",
            "82.push2.eastmoney.com"    # 之前报错的那个
        ]
        
        # 基础 URL 结构 (替换 DOMAIN)
        base_url = "https://{DOMAIN}/api/qt/clist/get?pn=1&pz=5000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048&fields=f12,f14,f2,f3,f8,f9,f20,f23"
        
        data_list = []
        
        try:
            with sync_playwright() as p:
                # 启动参数优化：忽略 HTTPS 错误 (有时候证书问题会导致连接中断)
                browser = p.chromium.launch(
                    headless=True, 
                    args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-blink-features=AutomationControlled']
                )
                
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                    ignore_https_errors=True 
                )
                page = context.new_page()

                success = False
                
                # --- 节点轮询循环 ---
                for node in api_nodes:
                    target_url = base_url.format(DOMAIN=node)
                    print(f"   ↳ 尝试连接节点: {node} ...")
                    
                    try:
                        # domcontentloaded 比 load 更快，减少等待时间防止超时
                        response = page.goto(target_url, timeout=15000, wait_until='domcontentloaded')
                        
                        if response.status == 200:
                            json_text = response.text()
                            if not json_text: 
                                print(f"     ❌ 空响应，尝试下一节点...")
                                continue
                                
                            data_json = json.loads(json_text)
                            if 'data' in data_json and 'diff' in data_json['data']:
                                raw_data = data_json['data']['diff']
                                print(f"     ✅ 成功! 获取到 {len(raw_data)} 条数据")
                                success = True
                                
                                # 数据清洗
                                for item in raw_data:
                                    try:
                                        code = str(item.get('f12', ''))
                                        name = str(item.get('f14', ''))
                                        price = float(item.get('f2', 0))
                                        pe = float(item.get('f9', -1))
                                        pb = float(item.get('f23', -1))
                                        turnover = float(item.get('f8', 0))
                                        cap = float(item.get('f20', 0))
                                        
                                        if (not code.startswith(('30', '688', '8', '4'))) and \
                                           ('ST' not in name) and ('退' not in name) and \
                                           (cap > self.min_cap) and (price > 3.0) and \
                                           (turnover > 1.0) and (turnover < 20):
                                                data_list.append((code, name, pe, pb, turnover))
                                    except: continue
                                break # 成功获取数据，跳出循环
                            else:
                                print(f"     ❌ JSON 格式不符，尝试下一节点...")
                        else:
                            print(f"     ❌ HTTP {response.status}，尝试下一节点...")
                            
                    except Exception as e:
                        print(f"     ❌ 连接超时或被拒: {str(e)[:50]}...")
                        time.sleep(1) # 稍微冷却一下
                
                browser.close()
                
                if not success:
                    print("❌ 所有节点尝试均失败。")
                    return []
                
        except Exception as e:
            print(f"❌ Playwright 致命错误: {e}")
            return []
            
        print(f"   ✅ 清洗后剩余候选股: {len(data_list)} 只")
        return data_list

    def scan_tech_fund(self, args):
        symbol, name, pe, pb, turnover = args
        try:
            if pe < 0: return None
            end = datetime.now().strftime("%Y%m%d")
            start = (datetime.now() - timedelta(days=400)).strftime("%Y%m%d")
            
            # K线获取也有可能不稳定，增加简单重试
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
            is_trend_up = fac['close'] > fac['ma20']
            
            # A: 主力意图
            if is_trend_up and (1 < turnover < 5) and (0.5 < fac['vol_ratio'] < 1.2):
                score += 20; logic.append("A:主力锁筹")
            elif is_trend_up and (fac['vol_ratio'] > 1.5) and (fac['pct_0'] > 0):
                score += 15; logic.append("A:放量启动")
            if (turnover > 15) and (-2 < fac['pct_0'] < 2):
                score -= 30; logic.append("A:⚠️高换手滞涨")

            # B: MACD+RSI
            macd_gold = (fac['macd_dif'] > fac['macd_dea']) and (fac['macd_dif'] > 0)
            if macd_gold:
                if fac['rsi'] < 80: score += 10; logic.append("B:共振")
                else: score -= 5; logic.append("B:RSI过热")
            
            # C: 黄金坑
            if (fac['close'] < fac['bb_low']) and (fac['cmf_0'] > 0.1):
                score += 40; logic.append("C:黄金坑")
            
            dif0, dea0 = fac['dif_0'], fac['dea_0']
            macd_status = "金叉" if dif0 > dea0 else "死叉"
            k0, d0 = fac['k_0'], fac['d_0']
            kdj_status = "金叉" if k0 > d0 else "死叉"

            if 0 < pe <= 25: score += 10
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
                    "MACD状态": macd_status, "KDJ状态": kdj_status,
                    "J值": round(fac['j_val'], 1), "布林带宽": round(fac['bb_width'], 3),
                    "RSI": round(fac['rsi'], 1), 
                    "CMF(今)": round(fac['cmf_0'], 3), 
                    "涨幅%(今)": round(fac['pct_0'], 2)
                }
            return None
        except: return None

    def run(self):
        print(f"{'='*100}")
        print(" 🌌 Alpha Galaxy Omni - GitHub Action Stable (Multi-Node) 🌌")
        print(f"{'='*100}")
        
        candidates = self.get_candidates_via_playwright()
        
        if not candidates:
            print("❌ 获取候选股失败，程序退出。")
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
            print(df[['代码', '名称', '总分', '现价', '得分详情']].head(10).to_string(index=False))
            filename = f"Alpha_Galaxy_Github_{datetime.now().strftime('%Y%m%d')}.xlsx"
            ExcelExporter.save(df, filename)
        else:
            print("没有生成结果。")

if __name__ == "__main__":
    AlphaGalaxyOmni().run()
