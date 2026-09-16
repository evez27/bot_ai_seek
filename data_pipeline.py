# ==============================================================================
# 📛 FICHIER : data_pipeline.py
# ✅ VERSION : V6.0 - FOCUS CRYPTO MAJEURS + VOLUME > 50M
#    - 🆕 Sémaphore global anti-429 (GLOBAL_API_SEMAPHORE)
#    - 🆕 Cap TOTAL des appels HTTP (pipeline + bot réunis)
# ==============================================================================

import asyncio
import aiohttp
import json
import os
import time
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Tuple
from collections import defaultdict
import indicators_ab as ind

# --------------------------
# 📂 CONFIG
# --------------------------
API_BASE = "https://api.bybit.com"
CATEGORY = "linear"
BATCH_SIZE = 5
DATA_DIR = "data"
CANDLES_DIR = os.path.join(DATA_DIR, "candles")

# Volume minimum pour considérer un actif crypto (en USD / 24h)
VOLUME_MIN_CRYPTO = 200_000_000  # 50M$

# Autres catégories (conservées mais secondaires)
VOLUME_MIN_PAR_CATEGORIE = {
    "crypto": VOLUME_MIN_CRYPTO,
    "forex": 100_000,
    "metal": 100_000,
    "energie": 100_000,
    "indice": 200_000,
    "action": 100_000,
}

# Quotas et seuils par catégorie
CONFIG_GROUPES = {
    "crypto": {"vol_min": VOLUME_MIN_CRYPTO, "quota": 50, "w_amp": 0.4, "w_force": 0.6, "adx_min": 20, "atr_min": 0.5,
               "quota_top100": 30, "quota_top20": 6},
    "forex": {"vol_min": 100_000, "quota": 15, "w_amp": 0.2, "w_force": 0.8, "adx_min": 12, "atr_min": 0.02,
              "quota_top100": 5, "quota_top20": 1},
    "metal": {"vol_min": 100_000, "quota": 8, "w_amp": 0.3, "w_force": 0.7, "adx_min": 12, "atr_min": 0.05,
              "quota_top100": 3, "quota_top20": 1},
    "energie":{"vol_min": 100_000, "quota": 5, "w_amp": 0.3, "w_force": 0.7, "adx_min": 12, "atr_min": 0.05,
               "quota_top100": 2, "quota_top20": 1},
    "indice": {"vol_min": 200_000, "quota": 12, "w_amp": 0.2, "w_force": 0.8, "adx_min": 12, "atr_min": 0.03,
               "quota_top100": 3, "quota_top20": 1},
    "action": {"vol_min": 100_000, "quota": 10, "w_amp": 0.3, "w_force": 0.7, "adx_min": 12, "atr_min": 0.03,
               "quota_top100": 2, "quota_top20": 1},
}

# Actifs crypto majeurs à inclure systématiquement
MAJOR_CRYPTO_SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "LTCUSDT",
    "DOTUSDT", "MATICUSDT", "UNIUSDT", "ATOMUSDT", "ETCUSDT",
    "FILUSDT", "APTUSDT", "ARBUSDT", "OPUSDT", "INJUSDT",
    "TIAUSDT", "SEIUSDT", "SUIUSDT", "NEARUSDT",
]

# Actifs figés (pour compatibilité avec scanner_funding)
ACTIFS_FIGES = MAJOR_CRYPTO_SYMBOLS.copy()
ACTIFS_FIGES += [
    "EURUSD", "GBPUSD", "USDJPY", "XAUUSD", "XAGUSD",
    "SPX500", "NAS100", "US30", "GER40", "UK100",
    "AAPL", "TSLA", "NVDA",
]

TREND_ONLY = True

FREQ_TIER1 = 900
FREQ_TIER2 = 120
FREQ_TIER3 = 10
NB_KLINE_COMPLET = 200
NB_KLINE_LIVE = 100
TOP_LIQUIDITE = 200
TOP_VARIATION = 100
TOP_TENDANCE = 20
TOP_FINAL = 20

TOP100_FILE = os.path.join(DATA_DIR, "top100.json")
TOP20_FILE = os.path.join(DATA_DIR, "top20.json")
TOP20_INDICATORS_FILE = os.path.join(DATA_DIR, "top20_indicators.json")
TOP5_FILE = os.path.join(DATA_DIR, "top5.json")
TOP_FUNDING_FILE = os.path.join(DATA_DIR, "top_funding.json")
TOP_VWAP_FILE = os.path.join(DATA_DIR, "top_vwap.json")

os.makedirs(os.path.join(CANDLES_DIR, "1m"), exist_ok=True)
os.makedirs(os.path.join(CANDLES_DIR, "5m"), exist_ok=True)
os.makedirs(os.path.join(CANDLES_DIR, "15m"), exist_ok=True)

class PipelineState:
    t1_pret = asyncio.Event()
    t2_pret = asyncio.Event()
    stop_flag = False
state = PipelineState()

# ==============================================================================
# 🆕 V6.0 — SÉMAPHORE GLOBAL D'API (protection anti-429)
# ==============================================================================
# Ce sémaphore est PARTAGÉ entre :
#   - Les boucles internes du pipeline (TIER1/2/3)
#   - Les appels du bot (via get_data, update_recent_klines, get_open_interest)
#
# Il CAPE le nombre total d'appels concurrents vers Bybit, indépendamment
# du nombre de callers. C'est la dernière ligne de défense anti-429.
#
# Valeur recommandée : 20 (soit ~80 req/s en pointe avec latence 250ms)
# Ajustable via env var BYBIT_MAX_CONCURRENT pour tuning sans redéploiement.
# ==============================================================================
GLOBAL_API_SEMAPHORE = asyncio.Semaphore(
    int(os.getenv("BYBIT_MAX_CONCURRENT", "20"))
)

def detecter_categorie(symbole: str) -> str:
    s = symbole.upper()
    if any(prefix in s for prefix in ["XAU", "XAG", "XPT", "PALL", "GOLD", "SILVER"]):
        return "metal"
    if any(kw in s for kw in ["OIL", "BRENT", "WTI", "CRUDE", "GAS", "NAT"]):
        return "energie"
    if any(kw in s for kw in ["SPX", "NAS", "DJI", "FTSE", "DAX", "CAC", "HSI", "US500", "US30", "GER40", "UK100", "HK50", "SP500"]):
        return "indice"
    if s in ["EURUSD","GBPUSD","USDJPY","AUDUSD","USDCAD","USDCHF","NZDUSD","EURJPY","EURGBP","GBPJPY","EURCHF"] or (len(s)==6 and s[:3] in ["EUR","GBP","AUD","NZD","USD"] and s[3:] in ["USD","JPY","GBP","CHF","CAD","AUD","NZD","EUR"]):
        return "forex"
    if any(act in s for act in ["AAPL", "TSLA", "NVDA", "MSFT", "AMZN", "GOOGL", "META", "AMD", "NFLX", "BABA", "SPOT"]):
        return "action"
    return "crypto"

def save_json_atomic(path: str, data: Any):
    temp = f"{path}.tmp"
    with open(temp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    if os.path.exists(path):
        try: os.remove(path)
        except: pass
    os.replace(temp, path)

def load_json_safe(path: str, default=None):
    if not os.path.exists(path): return default or {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except: return default or {}

def save_kline_csv(symbol: str, interval: str, rows: List[List]):
    path = os.path.join(CANDLES_DIR, f"{interval}m", f"{symbol}.csv")
    temp = f"{path}.tmp"
    with open(temp, "w", newline="", encoding="utf-8") as f:
        import csv
        writer = csv.writer(f)
        writer.writerow(["timestamp", "open", "high", "low", "close", "volume", "turnover"])
        writer.writerows(rows)
    if os.path.exists(path):
        try: os.remove(path)
        except: pass
    os.replace(temp, path)

def calc_amplitude_recente(df: pd.DataFrame) -> float:
    if len(df) < 10: return 0.0
    try:
        highs = df["high"].values[-20:]
        lows = df["low"].values[-20:]
        prix_actuel = df["close"].values[-1]
        return round((np.max(highs) - np.min(lows)) / prix_actuel * 100, 2) if prix_actuel > 0 else 0.0
    except: return 0.0

def calc_tendance_strength(df: pd.DataFrame) -> float:
    try:
        c = df["close"].values.astype(np.float64)
        h = df["high"].values.astype(np.float64)
        l = df["low"].values.astype(np.float64)
        if len(c) < 15: return 0.0
        adx_arr, plus_di, minus_di = ind.adx(h, l, c, period=14)
        if len(adx_arr) < 1 or np.isnan(adx_arr[-1]): return 0.0
        adx_val = float(adx_arr[-1])
        plus_val = float(plus_di[-1])
        minus_val = float(minus_di[-1])
        total = plus_val + minus_val
        if total == 0: return 0.0
        ecart = (plus_val - minus_val) / total
        return round(adx_val * ecart, 2)
    except: return 0.0

def normalize_scores_by_category(scores: List[Dict]) -> List[Dict]:
    groups = defaultdict(list)
    for item in scores:
        groups[item['categorie']].append(item)

    for cat, items in groups.items():
        sorted_items = sorted(items, key=lambda x: x['score'], reverse=True)
        n = len(sorted_items)
        for i, item in enumerate(sorted_items):
            if n > 1:
                percentile = (1 - i / (n - 1)) * 100
            else:
                percentile = 100.0
            item['score_norm'] = percentile

    return scores

class BybitCollector:
    def __init__(self, api_key: str = "", api_secret: str = ""):
        self.api_key = api_key
        self.api_secret = api_secret
        self.session: Optional[aiohttp.ClientSession] = None
        self.volume_cache: Dict[str, float] = {}
        self.metadata = {}
        self.categorie_cache: Dict[str, str] = {}
        self.open_interest_history: Dict[str, List[Tuple[int, float]]] = {}
        self.current_open_interest: Dict[str, float] = {}

    async def _init_session(self):
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))

    # 🆕 V6.0 — Régulateur global anti-429 (GLOBAL_API_SEMAPHORE)
    async def safe_get(self, url: str, params: Dict, retries=3) -> Optional[Dict]:
        await self._init_session()
        async with GLOBAL_API_SEMAPHORE:
            for attempt in range(retries):
                if state.stop_flag: return None
                try:
                    async with self.session.get(url, params=params) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            if data.get("retCode") == 0: return data
                        elif resp.status == 429:
                            await asyncio.sleep(2 + attempt)
                            continue
                except Exception as e:
                    await asyncio.sleep(1.5 * (attempt + 1))
            return None

    async def get_tickers(self) -> Dict[str, float]:
        url = f"{API_BASE}/v5/market/tickers"
        data = await self.safe_get(url, {"category": CATEGORY})
        if not data: return {}
        return {item["symbol"]: float(item.get("volume24h", 0)) for item in data["result"]["list"]}

    async def get_tickers_with_oi(self) -> Dict[str, Dict[str, float]]:
        url = f"{API_BASE}/v5/market/tickers"
        data = await self.safe_get(url, {"category": CATEGORY})
        if not data: return {}
        result = {}
        for item in data["result"]["list"]:
            symbol = item["symbol"]
            vol = float(item.get("volume24h", 0))
            oi = float(item.get("openInterest", 0) or item.get("openInterestValue", 0) or 0)
            result[symbol] = {"volume": vol, "open_interest": oi}
        return result

    async def get_open_interest(self, symbol: str) -> float:
        url = f"{API_BASE}/v5/market/open-interest"
        data = await self.safe_get(url, {
            "category": CATEGORY,
            "symbol": symbol,
            "intervalTime": "5min"
        })
        if not data or "result" not in data or not data["result"]["list"]:
            return 0.0
        try:
            return float(data["result"]["list"][0]["openInterest"])
        except:
            return 0.0

    async def update_open_interest_history(self, max_age_seconds: int = 900):
        tickers = await self.get_tickers_with_oi()
        now = int(time.time())
        for sym, data in tickers.items():
            oi = data["open_interest"]
            if oi <= 0: continue
            if sym not in self.open_interest_history:
                self.open_interest_history[sym] = []
            self.open_interest_history[sym].append((now, oi))
            cutoff = now - max_age_seconds
            self.open_interest_history[sym] = [(ts, val) for ts, val in self.open_interest_history[sym] if ts >= cutoff]

    def get_anomaly_score(self, symbol: str, df_ltf: Optional[pd.DataFrame] = None, df_mtf: Optional[pd.DataFrame] = None) -> float:
        score = 0.0
        if df_ltf is not None and 'volume' in df_ltf.columns and len(df_ltf) >= 20:
            vol_1m = df_ltf['volume'].iloc[-1]
            vol_moyen_1m = df_ltf['volume'].iloc[-20:].mean()
            ratio_1m = vol_1m / vol_moyen_1m if vol_moyen_1m > 0 else 1.0
            if ratio_1m > 5: score += 40
            elif ratio_1m > 3: score += 25
            elif ratio_1m > 2: score += 10
        if df_mtf is not None and 'volume' in df_mtf.columns and len(df_mtf) >= 20:
            vol_5m = df_mtf['volume'].iloc[-1]
            vol_moyen_5m = df_mtf['volume'].iloc[-20:].mean()
            ratio_5m = vol_5m / vol_moyen_5m if vol_moyen_5m > 0 else 1.0
            if ratio_5m > 4: score += 30
            elif ratio_5m > 2.5: score += 20
            elif ratio_5m > 1.5: score += 10
        hist = self.open_interest_history.get(symbol, [])
        if len(hist) >= 2:
            oi_start = hist[0][1]
            oi_end = hist[-1][1]
            if oi_start > 0:
                variation_pct = (oi_end - oi_start) / oi_start * 100
                if abs(variation_pct) > 15: score += 40
                elif abs(variation_pct) > 10: score += 30
                elif abs(variation_pct) > 5: score += 15
        return min(score, 100.0)

    async def get_categorie(self, symbol: str) -> str:
        if symbol in self.categorie_cache:
            return self.categorie_cache[symbol]
        cat = detecter_categorie(symbol)
        self.categorie_cache[symbol] = cat
        return cat

    async def scanner_funding(self):
        url = f"{API_BASE}/v5/market/tickers"
        data = await self.safe_get(url, {"category": CATEGORY})
        if not data: return
        opportunites = []
        total_scannes = 0
        stats_categories = {}
        for item in data["result"]["list"]:
            try:
                symbol = item["symbol"]
                funding = float(item.get("fundingRate", 0)) * 100
                vol = float(item.get("volume24h", 0))
                categorie = await self.get_categorie(symbol)
                vol_min = VOLUME_MIN_PAR_CATEGORIE.get(categorie, VOLUME_MIN_CRYPTO)
                stats_categories[categorie] = stats_categories.get(categorie, 0) + 1
                is_figee = symbol in ACTIFS_FIGES
                if vol >= vol_min and abs(funding) >= 0.05:
                    total_scannes += 1
                    opportunites.append({
                        "symbol": symbol,
                        "categorie": categorie,
                        "funding": round(funding, 4),
                        "apy": round(funding * 3 * 365, 1),
                        "volume_24h": vol,
                        "est_figee": is_figee
                    })
                elif is_figee:
                    total_scannes += 1
                    opportunites.append({
                        "symbol": symbol,
                        "categorie": categorie,
                        "funding": round(funding, 4),
                        "apy": round(funding * 3 * 365, 1),
                        "volume_24h": vol,
                        "est_figee": True
                    })
            except:
                continue

        opportunites = sorted(opportunites, key=lambda x: abs(x["funding"]), reverse=True)
        categories_presentes = set()
        final_opps = []
        for opp in opportunites:
            if opp["categorie"] not in categories_presentes:
                final_opps.append(opp)
                categories_presentes.add(opp["categorie"])
            elif len(final_opps) < 20:
                final_opps.append(opp)
            if len(final_opps) >= 20:
                break
        save_json_atomic(TOP_FUNDING_FILE, {
            "timestamp": int(time.time()),
            "count": len(final_opps),
            "total_scannes": total_scannes,
            "stats_categories": stats_categories,
            "data": final_opps
        })
        if final_opps:
            top_pos = final_opps[0]
            cats = set(o["categorie"] for o in final_opps)
            print(f"💰 FUNDING: {len(final_opps)} | Top: {top_pos['symbol']} ({top_pos['categorie']}) {top_pos['funding']}% | Cats: {', '.join(cats)}")

    async def scanner_vwap(self, symboles: List[str]):
        vwap_signals = []
        for sym in symboles[:50]:
            csv_path = os.path.join(CANDLES_DIR, "15m", f"{sym}.csv")
            if not os.path.exists(csv_path): continue
            try:
                df = pd.read_csv(csv_path)
                if len(df) < 30: continue
                vwap_arr = ind.vwap(df["high"].values, df["low"].values, df["close"].values, df["volume"].values)
                vwap_val = float(vwap_arr[-1])
                prix = float(df["close"].iloc[-1])
                if vwap_val == 0: continue
                dist = (prix - vwap_val) / vwap_val * 100
                if abs(dist) < 1.0:
                    vwap_signals.append({"symbol": sym, "vwap": round(vwap_val, 4), "prix": prix, "dist_pct": round(dist, 2), "categorie": await self.get_categorie(sym)})
            except: continue
        save_json_atomic(TOP_VWAP_FILE, {"timestamp": int(time.time()), "count": len(vwap_signals), "data": vwap_signals})
        if vwap_signals:
            print(f"📈 VWAP: {len(vwap_signals)} en zone")

    async def get_klines_raw(self, symbol: str, interval: str, limit: int = 200) -> List[List]:
        url = f"{API_BASE}/v5/market/kline"
        data = await self.safe_get(url, {"category": CATEGORY, "symbol": symbol, "interval": interval, "limit": limit})
        if not data: return []
        return data["result"]["list"]

    async def get_data(self, symbol: str, interval: str, limit: int = 100) -> Optional[pd.DataFrame]:
        csv_path = os.path.join(CANDLES_DIR, f"{interval}m", f"{symbol}.csv")
        base_rows = []
        if os.path.exists(csv_path):
            try:
                df_csv = pd.read_csv(csv_path)
                base_rows = df_csv.values.tolist()
            except: base_rows = []
        live_rows = await self.get_klines_raw(symbol, interval, limit=NB_KLINE_LIVE)
        if not live_rows: live_rows = []
        all_rows = base_rows + live_rows
        if not all_rows: return None
        df = pd.DataFrame(all_rows, columns=["timestamp", "open", "high", "low", "close", "volume", "turnover"])
        for col in ["open", "high", "low", "close", "volume", "turnover"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")
        df = df.dropna().drop_duplicates(subset=["timestamp"]).sort_values("timestamp", ascending=True).reset_index(drop=True)
        save_kline_csv(symbol, interval, df.values.tolist())
        return df if limit > len(df) else df.tail(limit)

    async def update_recent_klines(self, symbol: str, interval: str, limit: int = 10):
        csv_path = os.path.join(CANDLES_DIR, f"{interval}m", f"{symbol}.csv")
        base_rows = []
        if os.path.exists(csv_path):
            try:
                df_csv = pd.read_csv(csv_path)
                base_rows = df_csv.values.tolist()
            except: base_rows = []
        live_rows = await self.get_klines_raw(symbol, interval, limit=limit)
        if not live_rows: return
        all_rows = base_rows + live_rows
        if not all_rows: return
        df = pd.DataFrame(all_rows, columns=["timestamp", "open", "high", "low", "close", "volume", "turnover"])
        for col in ["open", "high", "low", "close", "volume", "turnover"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")
        df = df.dropna().drop_duplicates(subset=["timestamp"]).sort_values("timestamp", ascending=True).reset_index(drop=True)
        save_kline_csv(symbol, interval, df.values.tolist())

    async def ensure_data_for_symbols(self, symbols: List[str], intervals: List[str], force: bool = False):
        for i in range(0, len(symbols), BATCH_SIZE):
            if state.stop_flag: break
            batch = symbols[i:i+BATCH_SIZE]
            for tf in intervals:
                for sym in batch:
                    try:
                        await self.get_data(sym, tf, NB_KLINE_COMPLET)
                        await asyncio.sleep(0.3)
                    except: await asyncio.sleep(0.5)
            await asyncio.sleep(1.5)

    async def update_top100(self, items: List[Any]):
        normalized = []
        for x in items:
            if isinstance(x, dict):
                cat = x.get("categorie") or await self.get_categorie(x.get("symbol", ""))
                normalized.append({"symbol": x.get("symbol", ""), "categorie": cat, "volume_24h": x.get("volume_24h", 0), "amplitude": x.get("amplitude", 0), "force": x.get("force", 0), "score": x.get("score", 0), "score_norm": x.get("score_norm", 0), "est_figee": x.get("est_figee", False), "adx": x.get("adx",0), "atr_pct": x.get("atr_pct",0)})
            else:
                sym = str(x)
                cat = await self.get_categorie(sym)
                normalized.append({"symbol": sym, "categorie": cat, "volume_24h": self.volume_cache.get(sym, 0), "amplitude": 0, "force": 0, "score": 0, "score_norm": 0, "est_figee": False, "adx":0, "atr_pct":0})
        self.metadata = {x["symbol"]: x for x in normalized}
        save_json_atomic(TOP100_FILE, {
            "timestamp": int(time.time()),
            "count": len(normalized),
            "symbols": [x["symbol"] for x in normalized],
            "volumes": {x["symbol"]: x["volume_24h"] for x in normalized},
            "amplitudes": {x["symbol"]: x["amplitude"] for x in normalized},
            "forces": {x["symbol"]: x["force"] for x in normalized},
            "scores": {x["symbol"]: x["score"] for x in normalized},
            "scores_norm": {x["symbol"]: x["score_norm"] for x in normalized},
            "figes": [x["symbol"] for x in normalized if x.get("est_figee", False)],
            "categories": {x["symbol"]: x["categorie"] for x in normalized},
            "adx": {x["symbol"]: x.get("adx",0) for x in normalized},
            "atr_pct": {x["symbol"]: x.get("atr_pct",0) for x in normalized}
        })

    async def update_top20(self, items: List[Dict]):
        save_json_atomic(TOP20_FILE, {"timestamp": int(time.time()), "count": len(items), "data": items})

    async def update_top5(self, items: List[str]):
        save_json_atomic(TOP5_FILE, {"timestamp": int(time.time()), "count": len(items), "symbols": items})

    async def start_background_collector(self):
        await self._init_session()
        t1 = asyncio.create_task(self._boucle_tier1())
        t2 = asyncio.create_task(self._boucle_tier2())
        t3 = asyncio.create_task(self._boucle_tier3())
        try: await asyncio.gather(t1, t2, t3)
        except asyncio.CancelledError: pass
        finally:
            if self.session and not self.session.closed: await self.session.close()

    def stop(self): state.stop_flag = True

    # ------------------------------------------------------------
    # TIER 1 : Scan initial + normalisation + quotas
    # ------------------------------------------------------------
    async def _boucle_tier1(self):
        while not state.stop_flag:
            debut = time.time()
            try:
                tickers = await self.get_tickers_with_oi()
                if not tickers:
                    await asyncio.sleep(30)
                    continue

                # Sélection des symboles à scanner
                selected_symbols = set(MAJOR_CRYPTO_SYMBOLS)  # inclure les majeurs
                for sym, d in tickers.items():
                    if d["volume"] >= VOLUME_MIN_CRYPTO:
                        selected_symbols.add(sym)

                # Regrouper par catégorie
                groupes = {k: [] for k in CONFIG_GROUPES}
                for sym in selected_symbols:
                    cat = await self.get_categorie(sym)
                    if cat in groupes:
                        volume = tickers.get(sym, {}).get("volume", 0)
                        groupes[cat].append({
                            "symbol": sym,
                            "volume_24h": volume,
                            "oi": tickers.get(sym, {}).get("open_interest", 0)
                        })

                print(f"📊 TIER1 FOCUS CRYPTO : " + " | ".join([f"{k}:{len(v)}" for k,v in groupes.items()]))

                all_scores = []

                for cat, items in groupes.items():
                    if not items: continue
                    items.sort(key=lambda x: x["volume_24h"], reverse=True)
                    # Limiter au quota*2 pour l'analyse
                    a_scanner = [x["symbol"] for x in items[:CONFIG_GROUPES[cat]["quota"]*2]]
                    await self.ensure_data_for_symbols(a_scanner, ["15"])

                    scores = []
                    for it in items[:CONFIG_GROUPES[cat]["quota"]*2]:
                        df = await self.get_data(it["symbol"], "15", 50)
                        if df is None or len(df) < 30: continue
                        try:
                            c = df["close"].values.astype(np.float64)
                            h = df["high"].values.astype(np.float64)
                            l = df["low"].values.astype(np.float64)
                            adx_arr, plus_di, minus_di = ind.adx(h,l,c,14)
                            if len(adx_arr) < 3: continue
                            adx = float(np.mean(adx_arr[-3:]))
                            plus_m = float(np.mean(plus_di[-3:]))
                            minus_m = float(np.mean(minus_di[-3:]))
                            atr = ind.atr(h,l,c,14)[-1]
                            atr_pct = atr / c[-1] * 100 if c[-1]>0 else 0
                        except: continue

                        # Filtre TREND_ONLY
                        if TREND_ONLY:
                            if adx < CONFIG_GROUPES[cat]["adx_min"]: continue
                            if abs(plus_m - minus_m) < 5: continue
                            if atr_pct < CONFIG_GROUPES[cat]["atr_min"]: continue

                        amplitude = calc_amplitude_recente(df)
                        force = calc_tendance_strength(df)
                        cfg = CONFIG_GROUPES[cat]
                        score_base = amplitude * cfg["w_amp"] + abs(force) * cfg["w_force"]
                        score_final = score_base + (adx * 0.05)
                        scores.append({
                            "symbol": it["symbol"], "volume_24h": it["volume_24h"],
                            "amplitude": amplitude, "force": force,
                            "score": round(score_final,2), "categorie": cat,
                            "adx": round(adx,1), "atr_pct": round(atr_pct,2),
                            "est_figee": it["symbol"] in ACTIFS_FIGES
                        })

                    # Ajouter à la liste globale
                    all_scores.extend(scores)

                if not all_scores:
                    await asyncio.sleep(10)
                    continue

                # Normalisation par catégorie
                all_scores = normalize_scores_by_category(all_scores)

                # Sélection Top100 avec quotas
                selected_top100 = []
                # D'abord, quota par catégorie basé sur score_norm décroissant
                for cat, cfg in CONFIG_GROUPES.items():
                    cat_items = [x for x in all_scores if x['categorie'] == cat]
                    cat_items.sort(key=lambda x: x['score_norm'], reverse=True)
                    quota = cfg.get('quota_top100', 0)
                    selected_top100.extend(cat_items[:quota])

                # Compléter jusqu'à 100 avec les meilleurs restants
                already = {x['symbol'] for x in selected_top100}
                restants = [x for x in all_scores if x['symbol'] not in already]
                restants.sort(key=lambda x: x['score_norm'], reverse=True)
                for item in restants:
                    if len(selected_top100) >= 100:
                        break
                    selected_top100.append(item)

                selected_top100 = selected_top100[:100]

                print(f"✅ TIER1 FOCUS CRYPTO: {len(selected_top100)} sélectionnés")
                await self.update_top100(selected_top100)
                await self.scanner_funding()
                await self.scanner_vwap([x["symbol"] for x in selected_top100])

                if not state.t1_pret.is_set():
                    state.t1_pret.set()
                    print(f"✅ TIER1 PREMIER SCAN OK")
            except Exception as e:
                print(f"❌ [TIER1] {e}")
                import traceback; traceback.print_exc()
            elapsed = time.time() - debut
            await asyncio.sleep(max(0, FREQ_TIER1 - elapsed))

    # ------------------------------------------------------------
    # TIER 2 : Mise à jour Top20 avec quotas par catégorie
    # ------------------------------------------------------------
    async def _boucle_tier2(self):
        print("⏳ TIER2 en attente Top100...")
        await state.t1_pret.wait()
        print("✅ TIER2 DÉMARRÉ")
        while not state.stop_flag:
            debut = time.time()
            try:
                top100_data = load_json_safe(TOP100_FILE)
                if not top100_data or "symbols" not in top100_data:
                    await asyncio.sleep(5); continue

                symboles = top100_data["symbols"]
                categories = top100_data.get("categories", {})
                scores_norm = top100_data.get("scores_norm", {})

                cat_symbols = defaultdict(list)
                for sym in symboles:
                    cat = categories.get(sym, "crypto")
                    cat_symbols[cat].append(sym)

                top20_simple = []
                for cat, syms in cat_symbols.items():
                    quota = CONFIG_GROUPES.get(cat, {}).get("quota_top20", 0)
                    syms_sorted = sorted(syms, key=lambda s: scores_norm.get(s, 0), reverse=True)
                    for sym in syms_sorted[:quota]:
                        top20_simple.append({"symbol": sym, "force": 0, "categorie": cat})

                if len(top20_simple) < 20:
                    pris = {x["symbol"] for x in top20_simple}
                    restants = [sym for sym in symboles if sym not in pris]
                    restants_sorted = sorted(restants, key=lambda s: scores_norm.get(s, 0), reverse=True)
                    for sym in restants_sorted:
                        if len(top20_simple) >= 20:
                            break
                        cat = categories.get(sym, "crypto")
                        top20_simple.append({"symbol": sym, "force": 0, "categorie": cat})

                await self.update_top20(top20_simple)
                if not state.t2_pret.is_set():
                    state.t2_pret.set()
                    print(f"✅ TIER2 PREMIER SCAN OK")
            except Exception as e:
                print(f"❌ [TIER2] {e}")
            await asyncio.sleep(max(0, FREQ_TIER2 - (time.time() - debut)))

    # ------------------------------------------------------------
    # TIER 3 : Mise à jour rapide des données pour les 20 actifs Top20
    # ------------------------------------------------------------
    async def _boucle_tier3(self):
        print("⏳ TIER3 en attente Top20...")
        await state.t2_pret.wait()
        print("✅ TIER3 DÉMARRÉ")
        while not state.stop_flag:
            try:
                top20_data = load_json_safe(TOP20_FILE)
                if top20_data and 'data' in top20_data:
                    symboles = [item['symbol'] for item in top20_data['data']]
                    await self.ensure_data_for_symbols(symboles, ["1"], force=True)
                await asyncio.sleep(FREQ_TIER3)
            except Exception as e:
                print(f"❌ [TIER3] {e}")
                await asyncio.sleep(30)

DataPipeline = BybitCollector