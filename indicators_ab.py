# ==============================================================================
# 📛 FICHIER : indicators_ab.py
# ✅ VERSION : V3 - INTEGRE VWAP + FUNDING FILTER + TIER1 INTACT
# - Garde tout ton AB_EMA + dérivée Atangana-Baleanu
# - Ajout VWAP + détection zone institu
# - Ajout score funding pour bot 100$
# - Sans TA-Lib (100% numpy)
# ==============================================================================

import numpy as np
import pandas as pd
import threading
from collections import defaultdict, deque

# ============================================================
# ✅ DÉRIVÉE FRACTIONNAIRE ATANGANA-BALEUR
# ============================================================
def ab_derivative_calc(series, alpha=0.35):
    s = np.asarray(series, dtype=np.float64)
    mask_valides = np.isfinite(s)
    if not np.any(mask_valides):
        return np.zeros_like(s)
    s = np.nan_to_num(s, nan=np.mean(s[mask_valides]))
    n = len(s)
    if n < 5:
        return np.zeros_like(s)
    per_fast = max(2, int(round(1 / alpha)))
    per_slow = max(3, int(round(3 / alpha)))
    ema_fast = ema(s, per_fast)
    ema_slow = ema(s, per_slow)
    return ema_fast - ema_slow

# ============================================================
# 📊 EMA
# ============================================================
def ema(series, period):
    series = np.asarray(series, dtype=np.float64)
    period = max(1, period)
    alpha = 2 / (period + 1)
    result = np.zeros_like(series)
    idx0 = 0
    while idx0 < len(series) and not np.isfinite(series[idx0]):
        idx0 += 1
    val_init = series[idx0] if idx0 < len(series) else 0.0
    result[0] = val_init
    for i in range(1, len(series)):
        val = series[i] if np.isfinite(series[i]) else result[i-1]
        result[i] = alpha * val + (1 - alpha) * result[i-1]
    return result

# ============================================================
# 📊 RSI
# ============================================================
def rsi(close, period=14):
    close = np.asarray(close, dtype=np.float64)
    n = len(close)
    period = max(2, min(period, n // 2))
    delta = np.diff(close, prepend=close[0])
    gain = np.where(delta > 1e-9, delta, 0.0)
    loss = np.where(delta < -1e-9, -delta, 0.0)
    avg_gain = np.zeros(n)
    avg_loss = np.zeros(n)
    avg_gain[period] = np.mean(gain[:period]) if np.any(gain[:period]) else 1e-6
    avg_loss[period] = np.mean(loss[:period]) if np.any(loss[:period]) else 1e-6
    for i in range(period + 1, n):
        avg_gain[i] = (avg_gain[i-1] * (period - 1) + gain[i]) / period
        avg_loss[i] = (avg_loss[i-1] * (period - 1) + loss[i]) / period
    rs = np.divide(avg_gain, avg_loss + 1e-9, out=np.ones(n), where=avg_loss > 0)
    rsi_arr = np.clip(100.0 - (100.0 / (1.0 + rs)), 0.0, 100.0)
    rsi_arr[:period + 1] = 50.0
    return rsi_arr

# ============================================================
# 📊 ATR
# ============================================================
def atr(high, low, close, period=14):
    high = np.asarray(high, dtype=np.float64)
    low = np.asarray(low, dtype=np.float64)
    close = np.asarray(close, dtype=np.float64)
    n = len(close)
    period = max(2, min(period, n // 2))
    close_shift = np.concatenate([[close[0]], close[:-1]])
    tr1 = high - low
    tr2 = np.abs(high - close_shift)
    tr3 = np.abs(low - close_shift)
    tr = np.max(np.stack([tr1, tr2, tr3]), axis=0)
    atr_arr = np.zeros(n)
    for i in range(period - 1, n):
        atr_arr[i] = np.mean(tr[i - period + 1:i + 1])
    premiere_valeur = atr_arr[period - 1] if np.isfinite(atr_arr[period - 1]) else np.mean(tr) + 1e-6
    atr_arr[:period - 1] = premiere_valeur
    atr_arr = np.maximum(atr_arr, 1e-9)
    return atr_arr

# ============================================================
# 📊 ADX
# ============================================================
def adx(high, low, close, period=14):
    high = np.asarray(high, dtype=np.float64)
    low = np.asarray(low, dtype=np.float64)
    close = np.asarray(close, dtype=np.float64)
    n = len(close)
    period = max(2, min(period, n // 3))
    close_shift = np.concatenate([[close[0]], close[:-1]])
    tr1 = high - low
    tr2 = np.abs(high - close_shift)
    tr3 = np.abs(low - close_shift)
    tr = np.max(np.stack([tr1, tr2, tr3]), axis=0)
    up = np.diff(high, prepend=high[0])
    down = -np.diff(low, prepend=low[0])
    plus_dm = np.where((up > down) & (up > 1e-9), up, 0.0)
    minus_dm = np.where((down > up) & (down > 1e-9), down, 0.0)
    plus_di = np.zeros(n)
    minus_di = np.zeros(n)
    atr_arr = np.zeros(n)
    for i in range(period - 1, n):
        tr_per = tr[i - period + 1:i + 1]
        atr_i = np.mean(tr_per) + 1e-9
        atr_arr[i] = atr_i
        plus_di[i] = 100.0 * np.mean(plus_dm[i - period + 1:i + 1]) / atr_i
        minus_di[i] = 100.0 * np.mean(minus_dm[i - period + 1:i + 1]) / atr_i
    pd_init = plus_di[period - 1] if np.isfinite(plus_di[period - 1]) else 25.0
    md_init = minus_di[period - 1] if np.isfinite(minus_di[period - 1]) else 25.0
    plus_di[:period - 1] = pd_init
    minus_di[:period - 1] = md_init
    dx = np.zeros(n)
    for i in range(period - 1, n):
        denom = plus_di[i] + minus_di[i] + 1e-9
        dx[i] = 100.0 * abs(plus_di[i] - minus_di[i]) / denom
    adx_arr = np.zeros(n)
    debut_adx = 2 * period - 1
    for i in range(debut_adx, n):
        adx_arr[i] = np.mean(dx[i - period + 1:i + 1])
    if debut_adx < n and np.isfinite(adx_arr[debut_adx]):
        adx_arr[:debut_adx] = adx_arr[debut_adx]
    else:
        adx_arr[:debut_adx] = 20.0
    return adx_arr, plus_di, minus_di

# ============================================================
# 📊 MACD
# ============================================================
def macd(close, fast=6, slow=26):
    return ema(close, fast) - ema(close, slow)

def macd_proba(close, fast=6, slow=26, std_period=20):
    m = macd(close, fast, slow)
    n = len(m)
    std_period = min(std_period, max(5, n // 3))
    if n < std_period + 2:
        return np.full(n, 0.5) if n > 1 else 0.5
    macd_slope = np.diff(m[-std_period - 1:])
    if len(macd_slope) < 2:
        return 0.5
    mean_slope = np.mean(macd_slope)
    std_slope = np.std(macd_slope) + 1e-9
    z_score = abs(mean_slope) / std_slope
    return np.clip(z_score / 3.0, 0.0, 1.0)

# ============================================================
# 🆕 VWAP — PRIX D'EQUILIBRE INSTITUTIONNEL
# ============================================================
def vwap(high, low, close, volume, period=100):
    high = np.asarray(high, float); low = np.asarray(low, float)
    close = np.asarray(close, float); volume = np.asarray(volume, float)
    if len(close) < 10:
        return np.full_like(close, close[-1] if len(close)>0 else 0)
    prix_typ = (high + low + close) / 3.0
    pv = prix_typ * volume
    pv_cum = np.cumsum(pv)
    v_cum = np.cumsum(volume) + 1e-9
    vwap_arr = pv_cum / v_cum
    # VWAP glissant sur period pour intraday
    if len(vwap_arr) > period:
        vwap_arr = ema(vwap_arr, period//2)
    return vwap_arr

def detecter_zone_vwap(prix_actuel, vwap_val, seuil_pct=1.0):
    """Retourne True si on est en zone institu (collé au VWAP)"""
    if vwap_val < 1e-6:
        return False, 0.0
    dist_pct = (prix_actuel - vwap_val) / vwap_val * 100.0
    en_zone = abs(dist_pct) <= seuil_pct
    return en_zone, dist_pct

# ============================================================
# 🆕 SCORE FUNDING — POUR 100$
# ============================================================
def score_funding(funding_rate_pct):
    """0 à 10 : plus le funding est haut, plus le score est haut (opportunité short)"""
    if funding_rate_pct >= 0.3: return 10.0
    if funding_rate_pct >= 0.15: return 8.0 + (funding_rate_pct-0.15)/0.15*2
    if funding_rate_pct >= 0.08: return 5.0 + (funding_rate_pct-0.08)/0.07*3
    return 0.0

# ============================================================
# 🔄 IndicatorsV131 — COMPAT
# ============================================================
class IndicatorsV131:
    @staticmethod
    def compute_all(df_1m, df_5m, alpha=0.35):
        df_1m = df_1m.copy(); df_5m = df_5m.copy()
        h1, l1, c1 = df_1m['high'].values, df_1m['low'].values, df_1m['close'].values
        adx1, _, _ = adx(h1, l1, c1, period=14)
        atr1 = atr(h1, l1, c1, period=14)
        df_1m['adx'] = adx1; df_1m['atr'] = atr1
        if 'volume' in df_1m.columns:
            df_1m['vwap'] = vwap(df_1m['high'].values, df_1m['low'].values, df_1m['close'].values, df_1m['volume'].values)
        c5 = df_5m['close'].values
        rsi5 = rsi(c5, period=14)
        df_5m['rsi'] = rsi5
        df_5m = IndicatorsV131._compute_ab_alpha(df_5m, alpha)
        df_1m['rsi'] = rsi(c1, period=14)
        df_5m_aligned = df_5m['ab_proba_trend'].reindex(df_1m.index, method='ffill')
        df_1m['ab_proba_trend'] = df_5m_aligned
        def get_trend(row):
            if any(pd.isna(x) for x in [row['adx'], row['rsi'], row['ab_proba_trend'], row['close']]):
                return 0
            adx_ok = row['adx'] > 18
            proba_ok = row['ab_proba_trend'] > 0.5
            if adx_ok and proba_ok and row['rsi'] > 50: return 1
            if adx_ok and proba_ok and row['rsi'] < 50: return -1
            return 0
        df_1m['trend'] = df_1m.apply(get_trend, axis=1)
        return df_1m, df_5m

    @staticmethod
    def _compute_ab_alpha(df, alpha):
        close = df['close'].ffill()
        mom = close.pct_change(3).fillna(0)
        vol = close.rolling(10, min_periods=5).std().fillna(close.std()) / (close + 1e-6)
        trend_strength = abs(mom) / (vol + 1e-6)
        df['ab_proba_trend'] = 1 / (1 + np.exp(-alpha * (trend_strength - 0.5)))
        df['ab_proba_trend'] = df['ab_proba_trend'].clip(0, 1)
        df['ab_expected_bars'] = np.clip(0.02 / (abs(mom) + 1e-6), 8, 60)
        return df

# ============================================================
# 🆕 AB_EMA SCORE V3
# ============================================================
class ABEmaScore:
    def __init__(self, maxlen=100):
        self.historique = defaultdict(lambda: deque(maxlen=maxlen))
        self.lock = threading.RLock()

    def prefill_history(self, symbol, highs, lows, closes, period_ema=21, period_atr=14, nb_prefill=30, timeframe="1m"):
        n = len(closes)
        if n < period_ema + period_atr: return
        symbol_key = f"{symbol}_{timeframe}" if symbol!= "GENERIC" else symbol
        step = max(1, (n - period_ema - period_atr) // nb_prefill)
        with self.lock:
            self.historique[symbol_key].clear()
            for end_idx in range(period_ema + period_atr, n, step):
                start_idx = end_idx - period_ema - period_atr
                if start_idx < 0: start_idx = 0
                window_high = highs[start_idx:end_idx]
                window_low = lows[start_idx:end_idx]
                window_close = closes[start_idx:end_idx]
                if len(window_close) < period_ema + period_atr: continue
                ab_brut = self.calculer_ab_brut_ema(window_high, window_low, window_close, period_ema=period_ema, period_atr=period_atr)
                self.historique[symbol_key].append(ab_brut)

    def calculer_ab_brut_ema(self, highs, lows, closes, period_ema=21, period_atr=14):
        ema_high = ema(highs, period_ema); ema_low = ema(lows, period_ema); ema_close = ema(closes, period_ema)
        if np.isnan(ema_high[-1]) or np.isnan(ema_low[-1]): return 0.0
        expansion_ema = ema_high[-1] - ema_low[-1]
        tr_ema = np.maximum(ema_high - ema_low, np.maximum(np.abs(ema_high - np.roll(ema_close, 1)), np.abs(ema_low - np.roll(ema_close, 1))))
        atr_ema = np.mean(tr_ema[-period_atr:])
        if atr_ema < 1e-6: return 0.0
        momentum_ema = ema_close[-1] - ema_close[-2] if len(ema_close) > 1 else 0
        signe = 1 if momentum_ema >= 0 else -1
        return float((expansion_ema / atr_ema) * signe)

    def calculer_ab_norm(self, symbol, ab_brut):
        with self.lock:
            self.historique[symbol].append(ab_brut)
            hist = list(self.historique[symbol])
        if len(hist) < 5: return 0.5
        mean = np.mean(hist); std = np.std(hist)
        if std < 1e-6: return 0.5
        return float(np.clip((ab_brut - mean) / std, -3.0, 3.0))

    def calculer_alpha_force_serie_ema(self, highs, lows, closes, symbol="GENERIC", period_ema=21, timeframe="1m", volumes=None):
        symbol_key = f"{symbol}_{timeframe}" if symbol!= "GENERIC" else symbol
        ab_brut = self.calculer_ab_brut_ema(highs, lows, closes, period_ema)
        ab_norm = self.calculer_ab_norm(symbol_key, ab_brut)
        ema_fast = ema(closes, max(2, period_ema // 2))
        ema_slow = ema(closes, max(3, int(period_ema * 1.5)))
        ema_signal = ema(closes, period_ema)
        if np.isnan(ema_fast[-1]) or np.isnan(ema_slow[-1]):
            return 0, 0, 0, 0, 0, 0, ab_norm, 0, False, 0
        alpha_force = float(ema_fast[-1] - ema_slow[-1])
        prix_moyen = float(np.mean(closes[-period_ema:]))
        alpha_norm = float(alpha_force / prix_moyen * 100) if prix_moyen > 0 else 0
        if ab_norm > 0.1 and alpha_force > 0: sens = 1
        elif ab_norm < -0.1 and alpha_force < 0: sens = -1
        else: sens = 0
        sens_f = sens
        adx_arr, _, _ = adx(highs, lows, closes, 14)
        adx_val = float(adx_arr[-1]) if len(adx_arr) > 0 else 0
        force_moy = float(np.mean(np.abs(ema_signal[-20:] - ema_signal[-21:-1]))) if len(ema_signal) > 21 else 0
        # 🆕 VWAP
        if volumes is not None and len(volumes) == len(closes):
            vwap_arr = vwap(highs, lows, closes, volumes)
            vwap_val = float(vwap_arr[-1])
            en_zone, dist = detecter_zone_vwap(float(closes[-1]), vwap_val, 1.0)
        else:
            vwap_val = float(closes[-1]); en_zone=False; dist=0.0
        return alpha_force, alpha_norm, sens_f, force_moy, adx_val, sens, ab_norm, vwap_val, en_zone, dist

    def calculer_score_composite_v2_ema(self, alpha_norm, ab_norm, adx, vol_ratio, tendance_forte, sens_f, rsi_val, stoch_rsi, stoch_rsi_prev, en_zone_vwap=False, funding_score=0):
        score = 0.0
        score += min(abs(alpha_norm) / 3.0, 3.5)
        score_ab = min(abs(ab_norm) / 3.0 * 3.0, 3.0)
        if (ab_norm > 0 and sens_f == 1) or (ab_norm < 0 and sens_f == -1):
            score_ab *= 1.2
        score += score_ab
        score += min(max(adx - 10, 0) / 20 * 2.0, 2.0)
        score += min(max(vol_ratio - 0.8, 0) / 2.0 * 1.0, 1.0)
        bonus = 0.0
        if tendance_forte: bonus += 0.8
        if sens_f == 1 and 40 < rsi_val < 68: bonus += 0.3
        if sens_f == -1 and 32 < rsi_val < 60: bonus += 0.3
        if sens_f == 1 and stoch_rsi > stoch_rsi_prev: bonus += 0.3
        if sens_f == -1 and stoch_rsi < stoch_rsi_prev: bonus += 0.3
        if en_zone_vwap: bonus += 0.5 # Bonus institu
        score += min(bonus, 1.5)
        # Funding ne pénalise pas le momentum, c'est un score parallèle
        return round(min(score, 10.0), 2)

    def calculer_levier(self, score, ab_norm, max_levier=10):
        lev = score * 0.8 + abs(ab_norm) * 1.2
        return int(np.clip(lev, 2, max_levier))