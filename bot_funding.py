# ==============================================================================
# 📛 FICHIER : bot_funding.py
# ✅ VERSION : V13.3 - Web Service Render + Flask Health + Discord + News Filter
#    - DRY_RUN=True  -> simulation interne
#    - DRY_RUN=False -> exécution réelle sur Bybit (testnet ou mainnet)
#    - 🆕 Serveur HTTP Flask pour éviter la mise en veille Render
#    - SL natif Bybit, TP géré par le bot, trailing manuel
#    - Sélection hiérarchique : OB > Swing > VP > S/S > VWAP
#    - Pending conservé sur le meilleur niveau hors fourchette
#    - Notifications Discord (ouvertures, fermetures, heartbeats)
#    - Blocage des trades ±15 min autour des annonces économiques High Impact
# ==============================================================================

import asyncio
import json
import logging
import os
import time
import signal
import hmac
import hashlib
import threading
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Any, Tuple
from collections import deque
import numpy as np
import pandas as pd
from tabulate import tabulate

# 🆕 Flask pour le serveur de santé (évite la veille Render)
try:
    from flask import Flask
    FLASK_AVAILABLE = True
except ImportError:
    FLASK_AVAILABLE = False

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from data_pipeline import DataPipeline, load_json_safe, save_json_atomic, state
import indicators_ab as ind

# Discord notifier
try:
    from discord_notifier import notifier
    DISCORD_ENABLED = True
except ImportError:
    notifier = None
    DISCORD_ENABLED = False
    print("⚠️ discord_notifier non trouvé, notifications Discord désactivées")

# News calendar
try:
    from news_calendar import news_calendar
    NEWS_ENABLED = True
except ImportError:
    news_calendar = None
    NEWS_ENABLED = False
    print("⚠️ news_calendar non trouvé, filtre news désactivé")

# ============================
# 📂 CONFIGURATION
# ============================
CAPITAL_INITIAL = 100

DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

SCAN_INTERVAL = 5
MONITOR_INTERVAL = 2

FEE_RATE = float(os.getenv("BYBIT_FEE_TAKER", "0.00055"))
SLIPPAGE_PCT = 0.05

BYBIT_API_KEY = os.getenv("BYBIT_API_KEY", "")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET", "")
BYBIT_TESTNET = os.getenv("BYBIT_TESTNET", "true").lower() == "true"
BYBIT_RECV_WINDOW = 5000

NEWS_BUFFER_MINUTES = 15
NEWS_UPDATE_INTERVAL = 1800

GLOBAL_RISK_PARAMS = {
    "risk_per_trade": 0.005,
    "sl_max_pct": 1.2,
    "rr_ratio": 3.0,
    "be_actif": False,
    "trail_actif": True,
    "trail_seuil_gain_pct": 1.2,
    "trailing_atr_mult": 1.5,
    "min_sl_pct": 0.5,
    "max_trade_duration": 40 * 60,
    "trailing_step_pct": 0.3,
    "momentum_bars_1m": 1,
    "momentum_bars_5m": 1,
    "structure_tolerance": 0.15,
    "fallback_si_aucun_niveau": False,
    "pending_timeout": 300,
    "sl_update_min_interval": 5,
}

CATEGORY_CONFIG = {
    "crypto": {
        "risk": {**GLOBAL_RISK_PARAMS},
        "indicators": {
            "vwap_period": 15, "adx_min": 20, "atr_min_pct": 0.5,
            "volume_confirm_min": 1.2, "funding_seuil_base": 0.08,
            "funding_percentile_min": 70, "volume_min_24h": 50_000_000,
            "adaptative": {"vwap_vol_base": 1.0, "vwap_fenetre_min": 5, "vwap_fenetre_max": 30,
                           "oi_fenetre_base": 10, "oi_volatilite_seuil": 2.0, "funding_alpha_base": 0.2}
        },
        "selection": {"momentum_score_min": 45, "quota": 50}
    },
    "forex": {
        "risk": {**GLOBAL_RISK_PARAMS, "sl_max_pct": 0.5, "rr_ratio": 2.5, "trail_seuil_gain_pct": 0.6,
                 "trailing_atr_mult": 1.2, "max_trade_duration": 120*60, "min_sl_pct": 0.2,
                 "trailing_step_pct": 0.1},
        "indicators": {"vwap_period": 20, "adx_min": 12, "atr_min_pct": 0.02, "volume_confirm_min": 1.5,
                       "funding_seuil_base": 0.02, "funding_percentile_min": 80, "volume_min_24h": 100_000,
                       "adaptative": {"vwap_vol_base": 0.2, "vwap_fenetre_min": 8, "vwap_fenetre_max": 40,
                                      "oi_fenetre_base": 10, "oi_volatilite_seuil": 0.5, "funding_alpha_base": 0.1}},
        "selection": {"momentum_score_min": 40, "quota": 15}
    },
    "metal": {
        "risk": {**GLOBAL_RISK_PARAMS, "sl_max_pct": 0.8, "rr_ratio": 2.0, "trail_seuil_gain_pct": 0.8,
                 "trailing_atr_mult": 1.3, "max_trade_duration": 60*60, "min_sl_pct": 0.3,
                 "trailing_step_pct": 0.2},
        "indicators": {"vwap_period": 15, "adx_min": 12, "atr_min_pct": 0.05, "volume_confirm_min": 1.2,
                       "funding_seuil_base": 0.05, "funding_percentile_min": 75, "volume_min_24h": 100_000,
                       "adaptative": {"vwap_vol_base": 0.5, "vwap_fenetre_min": 5, "vwap_fenetre_max": 30,
                                      "oi_fenetre_base": 8, "oi_volatilite_seuil": 1.0, "funding_alpha_base": 0.15}},
        "selection": {"momentum_score_min": 45, "quota": 8}
    },
    "energie": {
        "risk": {**GLOBAL_RISK_PARAMS, "sl_max_pct": 1.0, "rr_ratio": 2.5, "trail_seuil_gain_pct": 1.0,
                 "trailing_atr_mult": 1.4, "max_trade_duration": 60*60, "min_sl_pct": 0.4,
                 "trailing_step_pct": 0.2},
        "indicators": {"vwap_period": 15, "adx_min": 12, "atr_min_pct": 0.05, "volume_confirm_min": 1.2,
                       "funding_seuil_base": 0.05, "funding_percentile_min": 75, "volume_min_24h": 100_000,
                       "adaptative": {"vwap_vol_base": 0.6, "vwap_fenetre_min": 5, "vwap_fenetre_max": 30,
                                      "oi_fenetre_base": 8, "oi_volatilite_seuil": 1.5, "funding_alpha_base": 0.15}},
        "selection": {"momentum_score_min": 45, "quota": 5}
    },
    "indice": {
        "risk": {**GLOBAL_RISK_PARAMS, "sl_max_pct": 0.6, "rr_ratio": 2.5, "trail_seuil_gain_pct": 0.6,
                 "trailing_atr_mult": 1.2, "max_trade_duration": 120*60, "min_sl_pct": 0.2,
                 "trailing_step_pct": 0.1},
        "indicators": {"vwap_period": 20, "adx_min": 12, "atr_min_pct": 0.03, "volume_confirm_min": 1.5,
                       "funding_seuil_base": 0.03, "funding_percentile_min": 80, "volume_min_24h": 200_000,
                       "adaptative": {"vwap_vol_base": 0.3, "vwap_fenetre_min": 8, "vwap_fenetre_max": 40,
                                      "oi_fenetre_base": 10, "oi_volatilite_seuil": 0.8, "funding_alpha_base": 0.1}},
        "selection": {"momentum_score_min": 40, "quota": 12}
    },
    "action": {
        "risk": {**GLOBAL_RISK_PARAMS, "sl_max_pct": 1.0, "rr_ratio": 2.5, "trail_seuil_gain_pct": 0.8,
                 "trailing_atr_mult": 1.3, "max_trade_duration": 90*60, "min_sl_pct": 0.3,
                 "trailing_step_pct": 0.2},
        "indicators": {"vwap_period": 15, "adx_min": 12, "atr_min_pct": 0.03, "volume_confirm_min": 1.3,
                       "funding_seuil_base": 0.05, "funding_percentile_min": 75, "volume_min_24h": 100_000,
                       "adaptative": {"vwap_vol_base": 0.7, "vwap_fenetre_min": 5, "vwap_fenetre_max": 30,
                                      "oi_fenetre_base": 8, "oi_volatilite_seuil": 1.2, "funding_alpha_base": 0.15}},
        "selection": {"momentum_score_min": 45, "quota": 10}
    },
}

def get_category_config(category: str) -> Dict:
    return CATEGORY_CONFIG.get(category, CATEGORY_CONFIG["crypto"])

# ============================
# 📋 LOGGING
# ============================
class TierFormatter(logging.Formatter):
    def format(self, record):
        if not hasattr(record, 'tier'):
            record.tier = "GLOBAL"
        return super().format(record)

handler = logging.StreamHandler()
handler.setFormatter(TierFormatter("%(asctime)s [%(levelname)s] [%(tier)s] %(message)s"))
logger = logging.getLogger("funding_bot")
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)
logger.propagate = False

# ============================
# 🏥 SERVEUR HTTP DE SANTÉ (Flask)
# ============================
if FLASK_AVAILABLE:
    health_app = Flask(__name__)

    @health_app.route('/health')
    def health_check():
        return "OK", 200

    @health_app.route('/')
    def root():
        return "Bot Funding running", 200

    def run_health_server():
        port = int(os.environ.get("PORT", 10000))
        try:
            health_app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)
        except Exception as e:
            logger.error(f"❌ Erreur serveur HTTP: {e}", extra={'tier': 'GLOBAL'})

# ============================
# 🔐 BYBIT EXECUTOR
# ============================
class BybitExecutor:
    def __init__(self):
        self.api_key = BYBIT_API_KEY
        self.api_secret = BYBIT_API_SECRET
        self.base_url = "https://api-testnet.bybit.com" if BYBIT_TESTNET else "https://api.bybit.com"
        self.recv_window = BYBIT_RECV_WINDOW
        self.session = None
        self.sl_last_update = {}

    async def _init_session(self):
        import aiohttp
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))

    def _sign(self, params: dict, timestamp: int) -> str:
        param_str = str(timestamp) + self.api_key + str(self.recv_window) + json.dumps(params, separators=(',', ':'))
        return hmac.new(self.api_secret.encode(), param_str.encode(), hashlib.sha256).hexdigest()

    async def _get(self, endpoint: str, params: dict = None) -> Optional[Dict]:
        await self._init_session()
        params = params or {}
        timestamp = int(time.time() * 1000)
        param_str = str(timestamp) + self.api_key + str(self.recv_window) + "&".join(f"{k}={v}" for k, v in params.items())
        sign = hmac.new(self.api_secret.encode(), param_str.encode(), hashlib.sha256).hexdigest()
        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-TIMESTAMP": str(timestamp),
            "X-BAPI-SIGN": sign,
            "X-BAPI-RECV-WINDOW": str(self.recv_window),
        }
        url = f"{self.base_url}{endpoint}"
        for attempt in range(3):
            try:
                async with self.session.get(url, headers=headers, params=params) as resp:
                    data = await resp.json()
                    if data.get("retCode") == 0:
                        return data
                    logger.error(f"⚠️ Bybit GET {endpoint} erreur: {data.get('retMsg')}", extra={'tier': 'BYBIT'})
                    return None
            except Exception as e:
                logger.warning(f"⚠️ Bybit GET {endpoint} exception ({attempt+1}/3): {e}", extra={'tier': 'BYBIT'})
                await asyncio.sleep(1)
        return None

    async def _post(self, endpoint: str, params: dict) -> Optional[Dict]:
        await self._init_session()
        timestamp = int(time.time() * 1000)
        sign = self._sign(params, timestamp)
        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-TIMESTAMP": str(timestamp),
            "X-BAPI-SIGN": sign,
            "X-BAPI-RECV-WINDOW": str(self.recv_window),
            "Content-Type": "application/json",
        }
        url = f"{self.base_url}{endpoint}"
        for attempt in range(3):
            try:
                async with self.session.post(url, headers=headers, data=json.dumps(params)) as resp:
                    data = await resp.json()
                    if data.get("retCode") == 0:
                        return data
                    logger.error(f"⚠️ Bybit POST {endpoint} erreur: {data.get('retMsg')}", extra={'tier': 'BYBIT'})
                    return None
            except Exception as e:
                logger.warning(f"⚠️ Bybit POST {endpoint} exception ({attempt+1}/3): {e}", extra={'tier': 'BYBIT'})
                await asyncio.sleep(1)
        return None

    async def get_wallet_balance(self) -> float:
        data = await self._get("/v5/account/wallet-balance", {"accountType": "UNIFIED"})
        if not data: return 0.0
        try:
            return float(data["result"]["list"][0]["totalEquity"])
        except Exception as e:
            logger.error(f"⚠️ Lecture balance échouée: {e}", extra={'tier': 'BYBIT'})
            return 0.0

    async def get_open_positions(self) -> List[Dict]:
        data = await self._get("/v5/position/list", {"category": "linear", "settleCoin": "USDT"})
        if not data: return []
        try:
            return [p for p in data["result"]["list"] if float(p.get("size", 0)) > 0]
        except:
            return []

    async def place_order_market(self, symbol: str, side: str, qty: float, sl: Optional[float] = None) -> Optional[Dict]:
        params = {
            "category": "linear", "symbol": symbol, "side": side,
            "orderType": "Market", "qty": str(qty),
        }
        if sl is not None:
            params["stopLoss"] = str(sl)
            params["tpslMode"] = "Full"
        return await self._post("/v5/order/create", params)

    async def set_trading_stop(self, symbol: str, stop_loss: float) -> Optional[Dict]:
        now = time.time()
        last = self.sl_last_update.get(symbol, 0)
        if now - last < GLOBAL_RISK_PARAMS["sl_update_min_interval"]:
            return None
        params = {"category": "linear", "symbol": symbol, "stopLoss": str(stop_loss), "tpslMode": "Full"}
        result = await self._post("/v5/position/trading-stop", params)
        if result:
            self.sl_last_update[symbol] = now
        return result

    async def close_position(self, symbol: str, side: str, qty: float) -> Optional[Dict]:
        params = {
            "category": "linear", "symbol": symbol, "side": side,
            "orderType": "Market", "qty": str(qty), "reduceOnly": True,
        }
        return await self._post("/v5/order/create", params)

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

# ============================
# 🧠 MARKET ANALYZER
# ============================
class MarketAnalyzer:
    def __init__(self):
        self.funding_history = {}
        self.oi_history = {}
        self.funding_ema = {}
        self.load_oi()

    def load_oi(self):
        hist = load_json_safe("data/oi_history.json", {})
        for sym, data in hist.items():
            self.oi_history[sym] = deque(maxlen=50)
            for entry in data:
                self.oi_history[sym].append(tuple(entry))

    def save_oi(self):
        save_json_atomic("data/oi_history.json", {s: list(d) for s, d in self.oi_history.items()})

    def update_funding_history(self, funding_data):
        for opp in funding_data:
            sym = opp.get("symbol")
            if not sym: continue
            if sym not in self.funding_history:
                self.funding_history[sym] = deque(maxlen=100)
            self.funding_history[sym].append(opp.get("funding", 0))

    def get_funding_threshold(self, symbol, direction, category="crypto"):
        cfg = get_category_config(category)["indicators"]
        base = cfg["funding_seuil_base"]; percentile = cfg["funding_percentile_min"]
        if symbol not in self.funding_history or len(self.funding_history[symbol]) < 10:
            return base
        hist = list(self.funding_history[symbol])
        if direction == "LONG":
            return max(base, np.percentile(hist, percentile))
        else:
            return min(-base, np.percentile(hist, 100 - percentile))

    def get_funding_zscore(self, symbol, current_funding):
        if symbol not in self.funding_history or len(self.funding_history[symbol]) < 5:
            return 0
        hist = list(self.funding_history[symbol]); std = np.std(hist)
        return (current_funding - np.mean(hist)) / std if std else 0

    def update_oi(self, symbol, oi_value, price):
        if symbol not in self.oi_history:
            self.oi_history[symbol] = deque(maxlen=50)
        self.oi_history[symbol].append((time.time(), oi_value, price))

    def get_oi_divergence(self, symbol):
        if symbol not in self.oi_history or len(self.oi_history[symbol]) < 10: return None
        hist = list(self.oi_history[symbol])
        old_oi = hist[-10][1]; recent_oi = hist[-1][1]
        old_price = hist[-10][2]; recent_price = hist[-1][2]
        if old_oi == 0: return None
        oi_change = ((recent_oi - old_oi) / old_oi) * 100
        price_change = ((recent_price - old_price) / old_price) * 100 if old_price else 0
        if oi_change > 5 and price_change < -0.5: div = "BEARISH_DIVERGENCE"
        elif oi_change < -5 and price_change > 0.5: div = "BULLISH_DIVERGENCE"
        elif oi_change > 5 and price_change > 0.5: div = "BULLISH_CONFIRMATION"
        elif oi_change < -5 and price_change < -0.5: div = "BEARISH_CONFIRMATION"
        elif oi_change > 5 and abs(price_change) <= 0.5: div = "ACCUMULATION"
        elif oi_change < -5 and abs(price_change) <= 0.5: div = "DISTRIBUTION"
        else: div = "NEUTRAL"
        strength = "WEAK" if abs(price_change) < 0.3 else (
            "STRONG_BULLISH" if price_change > 0 and oi_change > 0 else
            "STRONG_BEARISH" if price_change < 0 and oi_change < 0 else
            "WEAK_BULLISH" if price_change > 0 else "WEAK_BEARISH")
        return {"div": div, "strength": strength, "oi_change": oi_change, "price_change": price_change}

    def analyze_volume(self, df, direction, volume_confirm_min=1.2):
        if df is None or len(df) < 20: return {"confirmed": False, "score": 0}
        avg5 = df['volume'].tail(5).mean(); avg20 = df['volume'].tail(20).mean()
        cur = df['volume'].iloc[-1]
        ratio5 = cur / avg5 if avg5 else 0; ratio20 = cur / avg20 if avg20 else 0
        whale = ratio20 > 2.0
        up_vol = df[df['close'] > df['open']]['volume'].sum()
        down_vol = df[df['close'] < df['open']]['volume'].sum()
        dir_ratio = up_vol / (up_vol + down_vol) if (up_vol + down_vol) > 0 else 0.5
        climax = cur > avg20 * 3
        score = 0; confirmed = False
        if direction == "LONG":
            if ratio5 > volume_confirm_min: score += 10; confirmed = True
            if whale: score += 15; confirmed = True
            if dir_ratio > 0.6: score += 10; confirmed = True
            if climax and df['close'].iloc[-1] < df['open'].iloc[-1]: score -= 10; confirmed = False
            if cur > avg20 * 2: score += 5; confirmed = True
        else:
            if ratio5 > volume_confirm_min: score += 10; confirmed = True
            if whale: score += 15; confirmed = True
            if dir_ratio < 0.4: score += 10; confirmed = True
            if climax and df['close'].iloc[-1] > df['open'].iloc[-1]: score -= 10; confirmed = False
            if cur > avg20 * 2: score += 5; confirmed = True
        return {"confirmed": confirmed, "score": min(score, 40)}

    def calculate_vwap(self, df, period=15):
        if df is None or len(df) < period: return None, None
        h = df["high"].values[-period:]; l = df["low"].values[-period:]
        c = df["close"].values[-period:]; v = df["volume"].values[-period:]
        vwap_arr = ind.vwap(h, l, c, v)
        if len(vwap_arr) == 0 or vwap_arr[-1] == 0: return None, None
        val = float(vwap_arr[-1])
        slope = (vwap_arr[-1] - vwap_arr[-3]) / vwap_arr[-3] * 100 if len(vwap_arr) >= 3 else 0
        return val, slope

    def calculate_atr(self, df, period=14):
        if df is None or len(df) < period + 1: return None
        h = df["high"].values; l = df["low"].values; c = df["close"].values
        tr = [max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1])) for i in range(1, len(h))]
        return float(np.mean(tr[-period:]))

    def calculate_vwap_adaptatif(self, df, category="crypto"):
        if df is None or len(df) < 5: return None, None
        cfg = get_category_config(category)["indicators"]["adaptative"]
        base_period = get_category_config(category)["indicators"]["vwap_period"]
        returns = df['close'].pct_change().dropna()
        if len(returns) < 5: return None, None
        vol = returns.tail(20).std() * 100
        fenetre = int(base_period * (1 - (vol / cfg["vwap_vol_base"]) * 0.5))
        fenetre = max(cfg["vwap_fenetre_min"], min(cfg["vwap_fenetre_max"], fenetre))
        return self.calculate_vwap(df, period=fenetre)

    def get_funding_adaptatif(self, symbol, current_funding, category="crypto"):
        cfg = get_category_config(category)["indicators"]["adaptative"]
        alpha_base = cfg["funding_alpha_base"]
        if symbol not in self.funding_ema:
            self.funding_ema[symbol] = current_funding
        else:
            alpha = max(0.05, min(0.5, alpha_base * (1 + abs(self.get_funding_zscore(symbol, current_funding)) / 3)))
            self.funding_ema[symbol] = alpha * current_funding + (1 - alpha) * self.funding_ema[symbol]
        if symbol not in self.funding_history or len(self.funding_history[symbol]) < 10:
            z = 0.0
        else:
            hist = list(self.funding_history[symbol])[-10:]
            std = np.std(hist)
            z = (current_funding - np.mean(hist)) / std if std > 0 else 0.0
        return self.funding_ema[symbol], z

    def analyze_structure(self, symbol, direction, funding, df1, df5, category="crypto"):
        cfg = get_category_config(category); ind_cfg = cfg["indicators"]
        res = {"score": 0, "funding": "", "oi": "", "vol": "", "div": None, "conf": "LOW",
               "trend": False, "uncertainty": False, "divergence": False}
        funding_lisse, z = self.get_funding_adaptatif(symbol, funding, category)
        th = self.get_funding_threshold(symbol, direction, category)
        if direction == "LONG":
            if funding_lisse >= th:
                res["funding"] = f"Funding haut {funding:.3f}%"; res["score"] += 15
                if abs(z) > 1.5: res["score"] += 10; res["funding"] += f" [Z {z:.1f}]"
            elif funding_lisse <= -th:
                res["funding"] = "SHORT overcrowded"; res["score"] -= 5; res["uncertainty"] = True
            else: res["funding"] = f"Funding normal {funding:.3f}%"
        else:
            if funding_lisse <= -th:
                res["funding"] = f"Funding bas {funding:.3f}%"; res["score"] += 15
                if abs(z) > 1.5: res["score"] += 10; res["funding"] += f" [Z {z:.1f}]"
            elif funding_lisse >= th:
                res["funding"] = "LONG overcrowded"; res["score"] -= 5; res["uncertainty"] = True
            else: res["funding"] = f"Funding normal {funding:.3f}%"
        oi_data = self.get_oi_divergence(symbol)
        if oi_data:
            res["div"] = oi_data["div"]
            if oi_data["div"] in ["NEUTRAL", "ACCUMULATION", "DISTRIBUTION"]: res["uncertainty"] = True
            div = oi_data["div"]; strength = oi_data["strength"]
            if direction == "LONG":
                if div == "BULLISH_CONFIRMATION" and strength == "STRONG_BULLISH":
                    res["score"] += 25; res["oi"] = f"OI confirme hausse ({oi_data['oi_change']:.1f}%)"
                elif div == "BULLISH_DIVERGENCE":
                    res["score"] += 20; res["oi"] = "Short squeeze potentiel"; res["divergence"] = True
                elif div == "ACCUMULATION": res["score"] += 15; res["oi"] = "Accumulation"
                elif div in ["BEARISH_DIVERGENCE", "BEARISH_CONFIRMATION"]: res["score"] -= 20; res["oi"] = "Éviter LONG"
                else: res["oi"] = "OI neutre"
            else:
                if div == "BEARISH_CONFIRMATION" and strength == "STRONG_BEARISH":
                    res["score"] += 25; res["oi"] = f"OI confirme baisse ({oi_data['oi_change']:.1f}%)"
                elif div == "BEARISH_DIVERGENCE":
                    res["score"] += 20; res["oi"] = "Long squeeze potentiel"; res["divergence"] = True
                elif div == "DISTRIBUTION": res["score"] += 15; res["oi"] = "Distribution"
                elif div in ["BULLISH_DIVERGENCE", "BULLISH_CONFIRMATION"]: res["score"] -= 20; res["oi"] = "Éviter SHORT"
                else: res["oi"] = "OI neutre"
        else: res["oi"] = "Pas assez d'OI"
        vol = self.analyze_volume(df5 if df5 is not None else df1, direction, ind_cfg["volume_confirm_min"])
        res["vol"] = f"Volume {'confirmé' if vol['confirmed'] else 'non confirmé'}"
        vol_score = vol["score"]
        if oi_data and oi_data["div"] in ["BULLISH_DIVERGENCE", "BEARISH_DIVERGENCE"] and vol["confirmed"]:
            vol_score = min(40, int(vol_score * 1.5)); res["vol"] += " (renforcé)"
        res["score"] += vol_score
        if oi_data and oi_data["div"] in ["BULLISH_DIVERGENCE", "BEARISH_DIVERGENCE"]:
            res["score"] += 15; res["uncertainty"] = False; res["divergence"] = True
        if res["score"] >= 60: res["conf"] = "HIGH"; res["trend"] = True
        elif res["score"] >= 40: res["conf"] = "MEDIUM"; res["trend"] = True
        elif res["score"] >= 25: res["conf"] = "LOW"
        else: res["conf"] = "NONE"
        return res

    def detect_order_blocks(self, df, window=20, threshold=1.5):
        if df is None or len(df) < window: return df
        df = df.copy()
        df['vol_mean'] = df['volume'].rolling(window=window).mean()
        df['vol_high'] = df['vol_mean'] * threshold
        df['bull_ob'] = (df['volume'] > df['vol_high']) & (df['close'] > df['open'])
        df['bear_ob'] = (df['volume'] > df['vol_high']) & (df['close'] < df['open'])
        return df

    def get_nearest_ob(self, df, price, trend):
        if df is None or df.empty: return None
        if trend == 'up':
            ob_df = df[(df['bull_ob'] == True) & (df['low'] < price)]
            if not ob_df.empty: return float(ob_df.iloc[-1]['low'])
        elif trend == 'down':
            ob_df = df[(df['bear_ob'] == True) & (df['high'] > price)]
            if not ob_df.empty: return float(ob_df.iloc[-1]['high'])
        return None

    def calculate_volume_profile(self, df, bins=20):
        if df is None or len(df) < 5: return None
        price_min = df['low'].min(); price_max = df['high'].max()
        if price_min == price_max: return None
        price_range = np.linspace(price_min, price_max, bins)
        volume_profile = pd.Series(np.zeros(bins), index=price_range)
        for _, row in df.iterrows():
            mid_price = (row['low'] + row['high']) / 2
            idx = min(np.searchsorted(price_range, mid_price), bins - 1)
            volume_profile.iloc[idx] += row['volume']
        return volume_profile

    def get_nearest_vp_zone(self, volume_profile, price, trend):
        if volume_profile is None or volume_profile.empty: return None
        if trend == 'up':
            vp_zone = volume_profile[volume_profile.index < price]
            if not vp_zone.empty: return float(vp_zone.idxmax())
        elif trend == 'down':
            vp_zone = volume_profile[volume_profile.index > price]
            if not vp_zone.empty: return float(vp_zone.idxmax())
        return None

    def get_priority_levels(self, entry_price, direction, df15, category="crypto"):
        h = df15['high'].values[-15:]; l = df15['low'].values[-15:]
        c = df15['close'].values[-15:]; v = df15['volume'].values[-15:]
        support = float(np.percentile(l, 20)); resistance = float(np.percentile(h, 80))
        breakout_high = float(np.max(h[:-3])); breakout_low = float(np.min(l[:-3]))
        vwap, _ = self.calculate_vwap_adaptatif(df15, category)
        if vwap is None: vwap = float(np.average(c, weights=v)) if sum(v) > 0 else float(np.mean(c))
        df_ob = self.detect_order_blocks(df15); vp = self.calculate_volume_profile(df15)
        if direction == "LONG":
            ob_level = self.get_nearest_ob(df_ob, entry_price, 'up')
            vp_level = self.get_nearest_vp_zone(vp, entry_price, 'up')
            swing_level = breakout_low; ss_level = support
        else:
            ob_level = self.get_nearest_ob(df_ob, entry_price, 'down')
            vp_level = self.get_nearest_vp_zone(vp, entry_price, 'down')
            swing_level = breakout_high; ss_level = resistance
        return [("OB", ob_level), ("Swing", swing_level), ("VP", vp_level), ("S/S", ss_level), ("VWAP", vwap)]

    def select_structural_level(self, entry_price, direction, risk_cfg, priority_levels):
        min_sl = risk_cfg["min_sl_pct"]; max_sl = risk_cfg["sl_max_pct"]
        tolerance = risk_cfg.get("structure_tolerance", 0.15)
        max_accept = max_sl * (1 + tolerance)
        best_priority_level = None; best_priority_name = None; best_priority_dist = None
        for name, level in priority_levels:
            if level is None: continue
            if direction == "LONG" and level >= entry_price: continue
            if direction == "SHORT" and level <= entry_price: continue
            dist_pct = abs(entry_price - level) / entry_price * 100
            if best_priority_level is None:
                best_priority_level = level; best_priority_name = name; best_priority_dist = dist_pct
            if min_sl <= dist_pct <= max_accept:
                return level, dist_pct, name, (best_priority_level, best_priority_name, best_priority_dist)
        return None, None, None, (best_priority_level, best_priority_name, best_priority_dist)

# (SUITE DANS LE BLOC 2)
# ==============================================================================
# 📛 FICHIER : bot_funding.py (BLOC 2/2)
# ✅ VERSION : V13.3 - Suite : TradeManager + FundingSuiviBot + Point d'entrée
# ==============================================================================

# ============================
# 💼 TRADE MANAGER
# ============================
class Trade:
    def __init__(self, symbol, direction, entry_price, sl, tp, ts, atr, funding, score, ms, category="crypto"):
        self.symbol = symbol
        self.direction = direction
        self.entry_price = entry_price
        self.stop_loss = sl
        self.initial_sl = sl
        self.take_profit = tp
        self.trailing_stop = ts
        self.atr = atr
        self.funding = funding
        self.score = score
        self.market_structure = ms
        self.category = category
        self.size = 0
        self.notional = 0
        self.margin = 0
        self.highest = entry_price
        self.lowest = entry_price
        self.be_active = False
        self.trail_active = False
        self.tp_hit = False
        self.entry_time = time.time()
        self.trailing_multiplier = get_category_config(category)["risk"]["trailing_atr_mult"]
        self.exit_reason = None
        self.bybit_qty = 0.0

    def update_extremes(self, high, low):
        self.highest = max(self.highest, high)
        self.lowest = min(self.lowest, low)

    def gain_pct(self):
        if self.direction == "LONG":
            return (self.highest - self.entry_price) / self.entry_price * 100
        else:
            return (self.entry_price - self.lowest) / self.entry_price * 100

class TradeManager:
    def __init__(self, initial_capital=CAPITAL_INITIAL, executor: Optional[BybitExecutor] = None):
        self.capital = initial_capital
        self.initial_capital = initial_capital
        self.capital_libre = initial_capital
        self.positions = {}
        self.closed_trades = []
        self.peak_capital = initial_capital
        self.drawdown_max = 0
        self.stats = {"total_trades": 0, "wins": 0, "losses": 0}
        self.daily_pnl = 0
        self.daily_start_capital = initial_capital
        self.last_day = datetime.now().date()
        self.executor = executor

    def get_max_positions(self):
        return 20 if self.capital < 1000 else min(int(self.capital // 50), 50)

    def get_min_notional(self):
        return 5.0 if self.capital < 1000 else 20.0

    def get_max_notional(self):
        return round(self.capital * 0.10, 2) if self.capital < 1000 else 200.0

    def get_exposition_max(self):
        return self.capital * 0.80

    def _exposition_actuelle(self):
        return sum(p.notional for p in self.positions.values())

    def calculer_notionnel(self, entry_price, sl_distance_pct, category="crypto"):
        risk_cfg = get_category_config(category)["risk"]
        risk_amount = self.capital * risk_cfg["risk_per_trade"]
        sl_distance = entry_price * sl_distance_pct / 100
        if sl_distance <= 0: return self.get_min_notional()
        size = risk_amount / sl_distance
        notionnel = size * entry_price
        return max(self.get_min_notional(), min(notionnel, self.get_max_notional()))

    async def ouvrir_position(self, symbol, direction, entry_price, sl, tp, ts, atr, funding, score, ms, sl_pct_effectif, category="crypto"):
        if not self.risk_ok(): return False, "Risk engine désactivé"
        if len(self.positions) >= self.get_max_positions(): return False, "Max positions atteint"
        if symbol in self.positions: return False, "Déjà en position"
        notionnel = self.calculer_notionnel(entry_price, sl_pct_effectif, category)
        if notionnel < self.get_min_notional() or notionnel > self.get_max_notional():
            return False, "Notionnel hors limites"
        if self._exposition_actuelle() + notionnel > self.get_exposition_max():
            return False, "Exposition max dépassée"
        marge = notionnel / 3
        if marge > self.capital_libre: return False, "Marge insuffisante"

        qty = notionnel / entry_price

        if self.executor is not None:
            side = "Buy" if direction == "LONG" else "Sell"
            result = await self.executor.place_order_market(symbol, side, qty, sl=sl)
            if not result:
                return False, "Ordre Bybit échoué"
            logger.info(f"🔵 Bybit ordre placé: {side} {symbol} qty={qty:.6f} SL={sl}", extra={'tier': 'BYBIT'})

        trade = Trade(symbol, direction, entry_price, sl, tp, ts, atr, funding, score, ms, category)
        trade.notional = notionnel; trade.margin = marge; trade.size = qty; trade.bybit_qty = qty
        self.capital_libre -= marge
        self.positions[symbol] = trade
        self.stats["total_trades"] += 1
        return True, "Position ouverte"

    async def gerer_sorties(self, symbol, high, low, close, atr):
        trade = self.positions.get(symbol)
        if not trade: return
        risk_cfg = get_category_config(trade.category)["risk"]
        trade.update_extremes(high, low)
        gain_pct = trade.gain_pct()

        if risk_cfg.get("trail_actif", True) and not trade.trail_active and gain_pct >= risk_cfg["trail_seuil_gain_pct"]:
            trade.trail_active = True
            if trade.direction == "LONG":
                new_sl = trade.highest * (1 - risk_cfg["trail_seuil_gain_pct"] / 100)
                trade.stop_loss = max(trade.stop_loss, trade.entry_price, new_sl)
            else:
                new_sl = trade.lowest * (1 + risk_cfg["trail_seuil_gain_pct"] / 100)
                trade.stop_loss = min(trade.stop_loss, trade.entry_price, new_sl)
            logger.info(f"🚀 Trailing activé {symbol} | SL -> {trade.stop_loss:.4f}")
            if self.executor is not None:
                await self.executor.set_trading_stop(symbol, trade.stop_loss)

        if trade.trail_active:
            step_pct = risk_cfg.get("trailing_step_pct", 0.3) / 100.0
            old_sl = trade.stop_loss
            if trade.direction == "LONG":
                new_stop = trade.highest * (1 - step_pct)
                if new_stop > trade.stop_loss:
                    trade.stop_loss = new_stop
            else:
                new_stop = trade.lowest * (1 + step_pct)
                if new_stop < trade.stop_loss:
                    trade.stop_loss = new_stop
            if self.executor is not None and abs(trade.stop_loss - old_sl) > 1e-9:
                await self.executor.set_trading_stop(symbol, trade.stop_loss)

        if not trade.tp_hit:
            if (trade.direction == "LONG" and high >= trade.take_profit) or (trade.direction == "SHORT" and low <= trade.take_profit):
                trade.tp_hit = True
                if trade.direction == "LONG":
                    trade.stop_loss = max(trade.stop_loss, trade.entry_price)
                else:
                    trade.stop_loss = min(trade.stop_loss, trade.entry_price)
                logger.info(f"🎯 TP atteint {symbol} | SL verrouillé breakeven -> {trade.stop_loss:.4f}")
                if self.executor is not None:
                    await self.executor.set_trading_stop(symbol, trade.stop_loss)

        sortie = False; prix_sortie = None; raison = None
        if (trade.direction == "LONG" and low <= trade.stop_loss) or (trade.direction == "SHORT" and high >= trade.stop_loss):
            prix_sortie = trade.stop_loss; sortie = True
            if abs(trade.stop_loss - trade.initial_sl) < 1e-9: raison = "SL"
            elif trade.tp_hit: raison = "TP"
            elif trade.trail_active: raison = "Trail"
            else: raison = "SL"
        elif time.time() - trade.entry_time > risk_cfg["max_trade_duration"] and not trade.tp_hit:
            prix_sortie = close; sortie = True; raison = "Échéance"

        if sortie:
            trade.exit_reason = raison
            if self.executor is not None:
                side = "Sell" if trade.direction == "LONG" else "Buy"
                await self.executor.close_position(symbol, side, trade.bybit_qty)
            await self.fermer_position(symbol, prix_sortie)

    async def fermer_position(self, symbol, prix_sortie):
        trade = self.positions.pop(symbol)
        if trade.direction == "LONG":
            prix_effectif = prix_sortie * (1 - SLIPPAGE_PCT / 100)
        else:
            prix_effectif = prix_sortie * (1 + SLIPPAGE_PCT / 100)

        if trade.direction == "LONG":
            pnl_brut = (prix_effectif - trade.entry_price) * trade.size
            pnl_pct = (prix_effectif - trade.entry_price) / trade.entry_price * 100
            evo_max = (trade.highest - trade.entry_price) / trade.entry_price * 100
            evo_min = (trade.lowest - trade.entry_price) / trade.entry_price * 100
            evolution_cloture = (prix_effectif - trade.entry_price) / trade.entry_price * 100
        else:
            pnl_brut = (trade.entry_price - prix_effectif) * trade.size
            pnl_pct = (trade.entry_price - prix_effectif) / trade.entry_price * 100
            evo_max = (trade.entry_price - trade.lowest) / trade.entry_price * 100
            evo_min = (trade.entry_price - trade.highest) / trade.entry_price * 100
            evolution_cloture = (trade.entry_price - prix_effectif) / trade.entry_price * 100

        frais = (trade.notional + prix_effectif * trade.size) * FEE_RATE
        pnl_net = pnl_brut - frais
        pnl_pct_net = (pnl_net / trade.notional) * 100 if trade.notional else 0
        pnl_pct_capital = (pnl_net / self.initial_capital) * 100 if self.initial_capital else 0

        self.capital_libre += trade.margin + pnl_net
        self.capital += pnl_net
        self.peak_capital = max(self.peak_capital, self.capital)
        dd = (self.peak_capital - self.capital) / self.peak_capital * 100 if self.peak_capital else 0
        self.drawdown_max = max(self.drawdown_max, dd)

        if pnl_pct_net > 0: self.stats["wins"] += 1
        else: self.stats["losses"] += 1

        raison = trade.exit_reason if trade.exit_reason else "SL"

        self.closed_trades.append({
            "symbol": trade.symbol, "direction": trade.direction,
            "entry": trade.entry_price, "exit": round(prix_effectif, 5),
            "entry_time": datetime.fromtimestamp(trade.entry_time).strftime("%d/%m %H:%M:%S"),
            "exit_time": datetime.now().strftime("%H:%M:%S"),
            "pnl": round(pnl_net, 2), "pnl_pct": round(pnl_pct_net, 2),
            "pnl_pct_capital": round(pnl_pct_capital, 2),
            "evo_max": round(evo_max, 2), "evo_min": round(evo_min, 2),
            "evolution_cloture": round(evolution_cloture, 2),
            "reason": raison,
            "duration_min": round((time.time() - trade.entry_time) / 60, 1),
            "market_structure": trade.market_structure.get("div", "N/A"),
            "trail_mult": trade.trailing_multiplier, "categorie": trade.category
        })
        if len(self.closed_trades) > 50: self.closed_trades.pop(0)

        logger.info(f"✅ Fermé {symbol} {trade.direction} | PnL {pnl_net:+.2f}$ ({pnl_pct_net:+.2f}% notionnel, {pnl_pct_capital:+.2f}% capital) | Raison: {raison}")

        if DISCORD_ENABLED and notifier:
            emoji = "🟢" if pnl_net > 0 else "🔴"
            await notifier.send(
                f"{emoji} **FERMETURE {trade.direction}** `{symbol}`\n"
                f"PnL: `{pnl_net:+.2f}$` ({pnl_pct_capital:+.2f}% capital)\n"
                f"Raison: `{raison}` | Durée: `{round((time.time() - trade.entry_time)/60,1)}min`"
            )

    def risk_ok(self):
        if self.drawdown_max > 15: return False
        daily_loss = (self.daily_pnl / self.daily_start_capital) * 100 if self.daily_start_capital else 0
        if daily_loss < -5: return False
        return True

    def update_daily_pnl(self):
        today = datetime.now().date()
        if today != self.last_day:
            self.daily_start_capital = self.capital; self.daily_pnl = 0; self.last_day = today
        else:
            self.daily_pnl = self.capital - self.daily_start_capital

    def get_metrics(self):
        total = len(self.closed_trades)
        wins = [t for t in self.closed_trades if t["pnl"] > 0]
        losses = [t for t in self.closed_trades if t["pnl"] <= 0]
        winrate = len(wins) / total * 100 if total else 0
        pf = sum(t["pnl"] for t in wins) / abs(sum(t["pnl"] for t in losses)) if losses and sum(t["pnl"] for t in losses) != 0 else 99.99
        exposition = self._exposition_actuelle() / self.capital * 100 if self.capital else 0
        return {"capital": round(self.capital, 2), "capital_libre": round(self.capital_libre, 2),
                "pnl_global": round(self.capital - self.initial_capital, 2),
                "total_trades": total, "wins": len(wins), "losses": len(losses),
                "winrate": round(winrate, 1), "profit_factor": round(pf, 2),
                "drawdown_max": round(self.drawdown_max, 1),
                "exposition_pct": round(exposition, 2),
                "positions_ouvertes": len(self.positions),
                "max_positions": self.get_max_positions(),
                "notionnel_max": self.get_max_notional(),
                "daily_pnl": round(self.daily_pnl, 2)}

# ============================
# 🤖 BOT PRINCIPAL
# ============================
class FundingSuiviBot:
    def __init__(self):
        self.pipeline = DataPipeline()
        self.analyzer = MarketAnalyzer()
        self.executor = None if DRY_RUN else BybitExecutor()
        self.trade_manager = TradeManager(CAPITAL_INITIAL, executor=self.executor)
        self.stop = False
        self.last_trade_time = {}
        self.pending_entries = {}
        self.start_time = time.time()
        self.oi_update_interval = 10
        self.oi_last_update = 0
        self.news_blocked_until = 0
        signal.signal(signal.SIGINT, self._stop)

    def _stop(self, *args):
        logger.info("Arrêt demandé...")
        self.stop = True
        self.pipeline.stop()
        self.analyzer.save_oi()

    async def get_candles(self, symbol, interval, limit):
        return await self.pipeline.get_data(symbol, interval, limit)

    def calculate_atr(self, df, period=14):
        return self.analyzer.calculate_atr(df, period)

    def calculate_adx(self, df, period=14):
        if df is None or len(df) < 30: return None, None, None
        plus_di, minus_di, adx = ind.adx(df['high'].values, df['low'].values, df['close'].values, period=period)
        if len(adx) < 3: return None, None, None
        return adx[-1], adx[-1] - adx[-3], plus_di[-1] > minus_di[-1]

    async def get_btc_regime(self):
        df = await self.get_candles("BTCUSDT", "15", 60)
        if df is None or len(df) < 40: return "Indisponible"
        adx, _, di_up = self.calculate_adx(df)
        if adx is None: return "Indisponible"
        if adx < 20: regime = "Neutre"
        elif di_up: regime = "Haussier"
        else: regime = "Baissier"
        return f"{regime} (ADX {adx:.1f})"

    async def update_news_calendar_loop(self):
        if not NEWS_ENABLED or news_calendar is None:
            logger.info("📅 Filtre news désactivé", extra={'tier': 'NEWS'})
            return
        while not self.stop:
            await news_calendar.fetch_events()
            await asyncio.sleep(NEWS_UPDATE_INTERVAL)

    async def calculate_dynamic_sl_tp(self, symbol, direction, entry_price, category="crypto"):
        cfg = get_category_config(category)
        risk_cfg = cfg["risk"]
        df15 = await self.get_candles(symbol, "15", 20)
        df5 = await self.get_candles(symbol, "5", 15)
        if df15 is None or df5 is None:
            return self.fallback_sl_tp(direction, entry_price, category)

        atr15 = self.calculate_atr(df15)
        atr5 = self.calculate_atr(df5)
        atr = atr15 or atr5

        h = df15['high'].values[-15:]; l = df15['low'].values[-15:]
        support = float(np.percentile(l, 20)); resistance = float(np.percentile(h, 80))

        priority_levels = self.analyzer.get_priority_levels(entry_price, direction, df15, category)
        level, dist_pct, type_niveau, pending_info = self.analyzer.select_structural_level(
            entry_price, direction, risk_cfg, priority_levels
        )

        if level is None:
            pending_level, pending_name, pending_dist = pending_info
            if risk_cfg.get("fallback_si_aucun_niveau", False):
                sl_pct = risk_cfg["sl_max_pct"]
                sl = entry_price * (1 - sl_pct/100) if direction == "LONG" else entry_price * (1 + sl_pct/100)
                struct_distance_pct = sl_pct; type_niveau = "Fallback"
                pending_level = None
            else:
                return {
                    "sl": 0, "tp": 0, "ts": 0, "sl_pct": 0, "tp_pct": 0,
                    "atr": atr, "support": support, "resistance": resistance,
                    "struct_distance_pct": None,
                    "pending_level": pending_level,
                    "pending_name": pending_name,
                    "pending_dist": pending_dist,
                    "reject_reason": "Aucun niveau structurel dans la fourchette acceptée",
                    "struct_type": None
                }
        else:
            if direction == "LONG": sl = level * 0.997
            else: sl = level * 1.003
            sl_pct = abs(entry_price - sl) / entry_price * 100
            struct_distance_pct = dist_pct
            pending_level = None

        tp_pct = sl_pct * risk_cfg["rr_ratio"]
        tp = entry_price * (1 + tp_pct/100) if direction == "LONG" else entry_price * (1 - tp_pct/100)
        if atr:
            trail_pct = min(risk_cfg["trailing_atr_mult"] * atr / entry_price * 100, risk_cfg["sl_max_pct"])
        else:
            trail_pct = sl_pct * 0.75
        ts = entry_price * (1 - trail_pct/100) if direction == "LONG" else entry_price * (1 + trail_pct/100)

        return {
            "sl": round(sl, 4), "tp": round(tp, 4), "ts": round(ts, 4),
            "sl_pct": round(sl_pct, 2), "tp_pct": round(tp_pct, 2),
            "atr": round(atr, 4) if atr else 0,
            "support": support, "resistance": resistance,
            "struct_distance_pct": round(struct_distance_pct, 2),
            "pending_level": None, "pending_name": None, "pending_dist": None,
            "reject_reason": None,
            "struct_type": type_niveau
        }

    def fallback_sl_tp(self, direction, entry_price, category="crypto"):
        risk_cfg = get_category_config(category)["risk"]
        sl_pct = risk_cfg["sl_max_pct"]; tp_pct = sl_pct * risk_cfg["rr_ratio"]
        sl = entry_price * (1 - sl_pct/100) if direction == "LONG" else entry_price * (1 + sl_pct/100)
        tp = entry_price * (1 + tp_pct/100) if direction == "LONG" else entry_price * (1 - tp_pct/100)
        ts = sl
        return {"sl": sl, "tp": tp, "ts": ts, "sl_pct": sl_pct, "tp_pct": tp_pct, "atr": 0,
                "support": 0, "resistance": 0, "struct_distance_pct": None,
                "pending_level": None, "pending_name": None, "pending_dist": None,
                "reject_reason": None, "struct_type": None}

    async def calculate_momentum_score(self, symbol, direction, funding, category="crypto"):
        cfg = get_category_config(category); risk_cfg = cfg["risk"]; ind_cfg = cfg["indicators"]; sel_cfg = cfg["selection"]
        df1 = await self.get_candles(symbol, "1", 30); df5 = await self.get_candles(symbol, "5", 30)
        if df1 is None or df5 is None: return 0, {"reason": "Données 1m/5m manquantes"}
        ms = self.analyzer.analyze_structure(symbol, direction, funding, df1, df5, category)
        if ms["score"] < 25 and not ms.get("divergence", False):
            return 0, {**ms, "reason": f"Score structure trop faible ({ms['score']})"}
        bars_1m = risk_cfg.get("momentum_bars_1m", 1); bars_5m = risk_cfg.get("momentum_bars_5m", 1)
        ret1 = df1["close"].pct_change().tail(bars_1m).sum() * 100
        ret5 = df5["close"].pct_change().tail(bars_5m).sum() * 100
        mom1 = min(15, max(0, ret1 * 12)) if direction == "LONG" else min(15, max(0, -ret1 * 12))
        mom5 = min(15, max(0, ret5 * 8)) if direction == "LONG" else min(15, max(0, -ret5 * 8))
        score = min(ms["score"], 40) + mom1 + mom5 + 10
        for opp in load_json_safe("data/top_funding.json", {}).get("data", []):
            if opp["symbol"] == symbol:
                score += min(10, opp.get("volume_24h", 0) / ind_cfg["volume_min_24h"]); break
        if ms.get("divergence", False) and score < sel_cfg["momentum_score_min"]:
            score = sel_cfg["momentum_score_min"]
        details = {**ms, "mom1": round(ret1, 2), "mom5": round(ret5, 2), "trend15": "Filtre 15m désactivé", "reason": ""}
        return max(0, min(score, 100)), details

    def _short_signal(self, text, max_len=10):
        if not text: return ""
        if text.startswith("Funding "):
            parts = text.split(); return f"F:{parts[-1]}" if parts else ""
        if text.startswith("OI "):
            parts = text.split(); return f"OI:{parts[-1]}" if parts else ""
        if text.startswith("Volume "):
            parts = text.split(); return f"Vol:{parts[-1]}" if parts else ""
        if text.startswith("Tendance 15m"):
            return "T15m:off" if "désactivé" in text else "T15m:on"
        return text[:max_len]

    async def process_pending_entries(self):
        if not self.pending_entries: return
        now = time.time(); to_remove = []
        for sym, info in self.pending_entries.items():
            if now - info["timestamp"] > info["timeout"]:
                to_remove.append(sym); continue
            df = await self.get_candles(sym, "1", 1)
            if df is None: continue
            current_price = float(df["close"].iloc[-1])
            level = info["level"]; direction = info["direction"]; category = info["category"]
            if direction == "LONG" and level * 0.999 <= current_price <= level * 1.001:
                sltp = await self.calculate_dynamic_sl_tp(sym, direction, current_price, category)
                if sltp.get("reject_reason") is None:
                    ok, msg = await self.trade_manager.ouvrir_position(
                        symbol=sym, direction=direction, entry_price=current_price,
                        sl=sltp["sl"], tp=sltp["tp"], ts=sltp["ts"], atr=sltp["atr"],
                        funding=info["funding"], score=info["score"], ms=info["ms"],
                        sl_pct_effectif=sltp["sl_pct"], category=category)
                    if ok:
                        logger.info(f"✅ Ouverture PENDING {direction} {sym} @ {current_price:.4f} | Niveau {sltp.get('struct_type','?')}")
                        self.last_trade_time[sym] = time.time()
                        if DISCORD_ENABLED and notifier:
                            await notifier.send(f"✅ **PENDING OUVERTURE {direction}** `{sym}` @ `{current_price:.4f}`")
                    to_remove.append(sym)
            elif direction == "SHORT" and level * 0.999 <= current_price <= level * 1.001:
                sltp = await self.calculate_dynamic_sl_tp(sym, direction, current_price, category)
                if sltp.get("reject_reason") is None:
                    ok, msg = await self.trade_manager.ouvrir_position(
                        symbol=sym, direction=direction, entry_price=current_price,
                        sl=sltp["sl"], tp=sltp["tp"], ts=sltp["ts"], atr=sltp["atr"],
                        funding=info["funding"], score=info["score"], ms=info["ms"],
                        sl_pct_effectif=sltp["sl_pct"], category=category)
                    if ok:
                        logger.info(f"✅ Ouverture PENDING {direction} {sym} @ {current_price:.4f} | Niveau {sltp.get('struct_type','?')}")
                        self.last_trade_time[sym] = time.time()
                        if DISCORD_ENABLED and notifier:
                            await notifier.send(f"✅ **PENDING OUVERTURE {direction}** `{sym}` @ `{current_price:.4f}`")
                    to_remove.append(sym)
        for sym in to_remove: self.pending_entries.pop(sym, None)

    def is_news_blocked(self):
        if not NEWS_ENABLED or news_calendar is None:
            return False, None
        return news_calendar.is_trade_blocked(buffer_minutes=NEWS_BUFFER_MINUTES)

    async def scan_and_trade(self):
        blocked, event = self.is_news_blocked()
        if blocked:
            now = time.time()
            if now > self.news_blocked_until:
                self.news_blocked_until = now + (NEWS_BUFFER_MINUTES * 60)
                msg = (f"🚫 **TRADE BLOQUÉ - Annonce éco**\n"
                       f"`{event['title']}` ({event['currency']})\n"
                       f"Heure: `{event['time_utc'].strftime('%H:%M UTC')}`\n"
                       f"Fenêtre: ±{NEWS_BUFFER_MINUTES} min")
                logger.info(msg.replace("\n", " | "), extra={'tier': 'NEWS'})
                if DISCORD_ENABLED and notifier:
                    await notifier.send(msg)
            return

        await self.process_pending_entries()
        btc_regime = await self.get_btc_regime()
        funding_data = load_json_safe("data/top_funding.json", {})
        funding_map = {opp["symbol"]: opp for opp in funding_data.get("data", [])}
        top100_data = load_json_safe("data/top100.json", {})
        all_symbols = list(funding_map.keys())
        for sym in top100_data.get("symbols", []):
            if sym not in funding_map: all_symbols.append(sym)
        all_symbols = all_symbols[:30]

        candidates = []
        for sym in all_symbols:
            opp = funding_map.get(sym)
            if opp:
                cat = opp.get("categorie", "crypto"); fund = opp.get("funding", 0.0)
                vol = opp.get("volume_24h", 0); est_figee = opp.get("est_figee", False)
            else:
                cat = top100_data.get("categories", {}).get(sym, "crypto"); fund = 0.0
                vol = top100_data.get("volumes", {}).get(sym, 0); est_figee = sym in top100_data.get("figes", [])
            cfg = get_category_config(cat)
            if vol < cfg["indicators"]["volume_min_24h"] and not est_figee: continue
            th_long = self.analyzer.get_funding_threshold(sym, "LONG", cat)
            th_short = self.analyzer.get_funding_threshold(sym, "SHORT", cat)
            if fund >= th_long: candidates.append((sym, "LONG", fund, cat))
            elif fund <= th_short: candidates.append((sym, "SHORT", fund, cat))
        if not candidates: return

        for sym, _, _, _ in candidates:
            await self.pipeline.update_recent_klines(sym, "1", 10)
            await self.pipeline.update_recent_klines(sym, "5", 5)
            await self.pipeline.update_recent_klines(sym, "15", 3)

        rows = []; scored_candidates = []
        for sym, direction, fund, cat in candidates:
            if sym in self.last_trade_time and time.time() - self.last_trade_time[sym] < 1800: continue
            if sym in self.pending_entries: continue
            score, details = await self.calculate_momentum_score(sym, direction, fund, cat)
            tradable = score >= get_category_config(cat)["selection"]["momentum_score_min"]
            statut = "✅" if tradable else "❌"
            rows.append([sym, cat, direction, f"{fund:.3f}%",
                         f"{self.analyzer.get_funding_threshold(sym, direction, cat):.3f}%",
                         f"{score:.0f}", details.get("div", "N/A"),
                         self._short_signal(details.get("funding", "")),
                         self._short_signal(details.get("oi", "")),
                         self._short_signal(details.get("vol", "")),
                         details.get("conf", "LOW"),
                         self._short_signal(details.get("trend15", "")),
                         statut, details.get("reason", "")[:20]])
            if tradable:
                scored_candidates.append({"symbol": sym, "direction": direction, "funding": fund,
                                          "categorie": cat, "score": score, "details": details})

        if rows:
            logger.info(f"📊 Régime BTC : {btc_regime}")
            logger.info("\n📋 ANALYSE DES CANDIDATS :\n" + tabulate(rows,
                headers=["Sym","Cat","Sens","Fund","Seuil","Score","Div","Sig Fund","Sig OI","Sig Vol","Conf","Tendance","Statut","Raison"],
                tablefmt="grid"))
        scored_candidates.sort(key=lambda x: x["score"], reverse=True)

        for c in scored_candidates[:10]:
            if len(self.trade_manager.positions) >= self.trade_manager.get_max_positions(): break
            df1 = await self.get_candles(c["symbol"], "1", 5)
            if df1 is None: continue
            entry_price = float(df1["close"].iloc[-1])
            sltp = await self.calculate_dynamic_sl_tp(c["symbol"], c["direction"], entry_price, c["categorie"])

            if sltp.get("reject_reason"):
                if sltp.get("pending_level") is not None:
                    self.pending_entries[c["symbol"]] = {
                        "level": sltp["pending_level"], "direction": c["direction"],
                        "category": c["categorie"], "funding": c["funding"], "score": c["score"],
                        "ms": {"div": c["details"].get("div"), "conf": c["details"].get("conf"),
                               "uncertainty": c["details"].get("unc")},
                        "timestamp": time.time(),
                        "timeout": get_category_config(c["categorie"])["risk"].get("pending_timeout", 300)}
                    logger.info(f"⏳ Pending {c['symbol']} sur {sltp.get('pending_name','?')} (dist {sltp.get('pending_dist','?')}%)")
                else:
                    logger.info(f"⛔ {sltp['reject_reason']} pour {c['symbol']}")
                continue

            ms = {"div": c["details"].get("div"), "conf": c["details"].get("conf"), "uncertainty": c["details"].get("unc")}
            ok, msg = await self.trade_manager.ouvrir_position(
                symbol=c["symbol"], direction=c["direction"], entry_price=entry_price,
                sl=sltp["sl"], tp=sltp["tp"], ts=sltp["ts"], atr=sltp["atr"],
                funding=c["funding"], score=c["score"], ms=ms,
                sl_pct_effectif=sltp["sl_pct"], category=c["categorie"])
            if ok:
                logger.info(f"✅ Ouverture {c['direction']} {c['symbol']} @ {entry_price:.4f} | SL {sltp['sl']:.4f} (SL {sltp['sl_pct']:.2f}%) | Niveau: {sltp.get('struct_type', 'N/A')} (dist {sltp.get('struct_distance_pct', 'N/A')}%) | TP {sltp['tp']:.4f} | Score {c['score']}")
                self.last_trade_time[c["symbol"]] = time.time()
                if DISCORD_ENABLED and notifier:
                    await notifier.send(
                        f"✅ **OUVERTURE {c['direction']}** `{c['symbol']}`\n"
                        f"Prix: `{entry_price:.4f}` | SL: `{sltp['sl']:.4f}` ({sltp['sl_pct']:.2f}%) | TP: `{sltp['tp']:.4f}`\n"
                        f"Score: `{c['score']:.0f}` | Niveau: `{sltp.get('struct_type','?')}` | Dist: `{sltp.get('struct_distance_pct','?')}%`"
                    )
            else:
                logger.info(f"⛔ {msg} pour {c['symbol']}")
            await asyncio.sleep(1)

    async def monitor_positions(self):
        if not self.trade_manager.positions: return
        for sym in list(self.trade_manager.positions.keys()):
            await self.pipeline.update_recent_klines(sym, "1", 30)
            df = await self.get_candles(sym, "1", 30)
            if df is None: continue
            high = float(df["high"].iloc[-1]); low = float(df["low"].iloc[-1]); close = float(df["close"].iloc[-1])
            atr = self.calculate_atr(df)
            await self.trade_manager.gerer_sorties(sym, high, low, close, atr)

    async def update_open_interest(self):
        symbols = []
        funding_data = load_json_safe("data/top_funding.json", {})
        if funding_data and "data" in funding_data:
            symbols.extend([x["symbol"] for x in funding_data["data"]])
        symbols.extend(list(self.trade_manager.positions.keys()))
        symbols = list(set(symbols))
        for sym in symbols:
            oi = await self.pipeline.get_open_interest(sym)
            if oi <= 0: continue
            df = await self.get_candles(sym, "1", 1)
            if df is not None and len(df) > 0:
                self.analyzer.update_oi(sym, oi, float(df["close"].iloc[-1]))

    async def heartbeats(self):
        await asyncio.gather(self._heartbeat_court_loop(), self._heartbeat_detail_loop())

    async def _heartbeat_court_loop(self):
        while not self.stop:
            await asyncio.sleep(300)
            m = self.trade_manager.get_metrics()
            btc_regime = await self.get_btc_regime()
            mode = "LIVE" if self.executor else "DRY"
            news_status = ""
            if NEWS_ENABLED and news_calendar:
                blocked, _ = self.is_news_blocked()
                news_status = " | 🚫 NEWS" if blocked else ""
            logger.info(f"💓 [{mode}] BTC: {btc_regime} | Capital: {m['capital']}$ | PnL: {m['pnl_global']:+.2f}$ | Trades: {m['total_trades']} (G:{m['wins']}/P:{m['losses']}) | WR: {m['winrate']}% | PF: {m['profit_factor']} | Pos: {m['positions_ouvertes']}{news_status}")
            if DISCORD_ENABLED and notifier:
                await notifier.send(
                    f"💓 **[{mode}]** BTC: `{btc_regime}`{news_status}\n"
                    f"Capital: `{m['capital']}$` | PnL: `{m['pnl_global']:+.2f}$`\n"
                    f"Trades: `{m['total_trades']}` (G:{m['wins']}/P:{m['losses']}) | WR: `{m['winrate']}%` | PF: `{m['profit_factor']}` | Pos: `{m['positions_ouvertes']}`"
                )

    async def _heartbeat_detail_loop(self):
        while not self.stop:
            await asyncio.sleep(900)
            m = self.trade_manager.get_metrics()
            btc_regime = await self.get_btc_regime()
            mode = "LIVE" if self.executor else "DRY"
            logger.info(f"=== DÉTAIL [{mode}] === BTC: {btc_regime} | Capital: {m['capital']}$ Libre: {m['capital_libre']}$ Expo: {m['exposition_pct']}% DD: {m['drawdown_max']}%")
            if self.trade_manager.positions:
                rows = [[sym, t.direction, t.entry_price, t.stop_loss, t.take_profit,
                         "✅" if t.be_active else "❌", "✅" if t.trail_active else "❌",
                         "✅" if t.tp_hit else "❌", t.trailing_multiplier,
                         round((time.time()-t.entry_time)/60,1), t.score, t.category]
                        for sym, t in self.trade_manager.positions.items()]
                logger.info("\n📋 POSITIONS OUVERTES:\n" + tabulate(rows,
                    headers=["Sym","Sens","Prix","SL","TP","BE","Trail","TP hit","TrailMult","Âge(min)","Score","Cat"],
                    tablefmt="grid"))
            if self.trade_manager.closed_trades:
                rows = [[t["symbol"], t["direction"], t["entry_time"], t["exit_time"],
                         f"{t['entry']:.4f}", f"{t['exit']:.4f}", f"{t['pnl']:+.2f}$",
                         f"{t['pnl_pct_capital']:+.2f}%", f"{t['evo_max']}%", f"{t['evo_min']}%",
                         f"{t['evolution_cloture']}%", t["reason"], f"{t['duration_min']}min", t["categorie"]]
                        for t in self.trade_manager.closed_trades[-20:]]
                logger.info("\n📊 20 DERNIERS TRADES:\n" + tabulate(rows,
                    headers=["Sym","Sens","Entrée","Sortie","Prix E","Prix S","PnL","PnL% Cap","EvoMax","EvoMin","Evo Clôture","Raison","Durée","Cat"],
                    tablefmt="grid"))

    async def run(self):
        mode = "LIVE (Testnet)" if BYBIT_TESTNET else ("LIVE (Mainnet)" if not DRY_RUN else "DRY_RUN")
        logger.info(f"🚀 Bot Funding Suivi V13.3 démarré - Mode: {mode}")

        # 🆕 Démarrage du serveur HTTP de santé (thread séparé)
        if FLASK_AVAILABLE:
            port = int(os.environ.get("PORT", 10000))
            threading.Thread(target=run_health_server, daemon=True).start()
            logger.info(f"🏥 Serveur HTTP de santé démarré sur le port {port}", extra={'tier': 'GLOBAL'})
        else:
            logger.warning("⚠️ Flask non disponible, serveur HTTP désactivé", extra={'tier': 'GLOBAL'})

        if DISCORD_ENABLED and notifier:
            await notifier.send(f"🚀 **Bot démarré** - Mode: `{mode}`")

        if self.executor is not None:
            await self.executor._init_session()
            balance = await self.executor.get_wallet_balance()
            if balance > 0:
                self.trade_manager.capital = balance
                self.trade_manager.initial_capital = balance
                self.trade_manager.capital_libre = balance
                self.trade_manager.peak_capital = balance
                logger.info(f"💰 Solde Bybit synchronisé: {balance}$", extra={'tier': 'BYBIT'})
                if DISCORD_ENABLED and notifier:
                    await notifier.send(f"💰 **Solde Bybit synchronisé**: `{balance}$`")
            else:
                logger.warning("⚠️ Impossible de lire le solde Bybit. DRY_RUN forcé.", extra={'tier': 'BYBIT'})

        collector_task = asyncio.create_task(self.pipeline.start_background_collector())
        news_task = asyncio.create_task(self.update_news_calendar_loop())
        await asyncio.sleep(15)
        tasks = [
            asyncio.create_task(self._scan_loop()),
            asyncio.create_task(self._monitor_loop()),
            asyncio.create_task(self._oi_loop()),
            asyncio.create_task(self.heartbeats()),
        ]
        try:
            await asyncio.gather(collector_task, news_task, *tasks)
        except asyncio.CancelledError:
            pass
        finally:
            self.stop = True
            self.analyzer.save_oi()
            if self.executor is not None:
                await self.executor.close()
            if NEWS_ENABLED and news_calendar:
                await news_calendar.close()
            if DISCORD_ENABLED and notifier:
                await notifier.send("🛑 **Bot arrêté**")
                await notifier.close()
            logger.info("Arrêt propre")

    async def _scan_loop(self):
        while not self.stop:
            await self.scan_and_trade()
            for _ in range(SCAN_INTERVAL):
                if self.stop: break
                await asyncio.sleep(1)

    async def _monitor_loop(self):
        while not self.stop:
            await self.monitor_positions()
            for _ in range(MONITOR_INTERVAL):
                if self.stop: break
                await asyncio.sleep(1)

    async def _oi_loop(self):
        while not self.stop:
            if time.time() - self.oi_last_update > self.oi_update_interval:
                await self.update_open_interest()
                self.oi_last_update = time.time()
                if time.time() - self.start_time > 120: self.oi_update_interval = 60
            await asyncio.sleep(1)

if __name__ == "__main__":
    print("🤖 Bot Funding Suivi V13.3")
    print(f"   DRY_RUN={DRY_RUN} | TESTNET={BYBIT_TESTNET} | DISCORD={DISCORD_ENABLED} | NEWS={NEWS_ENABLED} | FLASK={FLASK_AVAILABLE}")
    bot = FundingSuiviBot()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        pass