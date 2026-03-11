# ==========================================
# MARKET SCANNER PRO — VERSION 2.0
# Enhanced: Bug Fixes + ATR Filter + Tier System + 
#           Probabilistic Scoring + SL/TP Management
# ==========================================

import yfinance as yf
import pandas as pd
import numpy as np
import ta
import gspread
from gspread_dataframe import set_with_dataframe
from datetime import datetime, timezone, timedelta
import warnings
import json
import os
import time
from google.oauth2.service_account import Credentials

warnings.filterwarnings('ignore')

SPREADSHEET_ID = "1YqI5IEDknRU4wQDMUyKDXVKbUlT8qIbpQtTgI8K_cwQ"

# ==========================================
# GOOGLE SHEET CONNECTION
# ==========================================
def connect_gsheet(target_sheet_name):
    try:
        creds_json = os.environ.get("GCP_SA_KEY")
        if not creds_json:
            print("❌ GCP_SA_KEY tidak ditemukan di environment.")
            return None

        creds_dict = json.loads(creds_json)
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SPREADSHEET_ID)

        try:
            worksheet = sh.worksheet(target_sheet_name)
        except:
            print(f"Membuat sheet baru: {target_sheet_name}")
            worksheet = sh.add_worksheet(title=target_sheet_name, rows="300", cols="30")

        return worksheet

    except Exception as e:
        print(f"❌ Error Koneksi GSheet: {e}")
        return None


# ==========================================
# HELPER FUNCTIONS
# ==========================================
def is_near(price1, price2, pct=0.001):
    """Toleransi 0.1% untuk harga yang 'identik'"""
    return abs(price1 - price2) / max(price1, price2) <= pct

def close_near_high(row, threshold=0.2):
    length = row['High'] - row['Low']
    return (row['High'] - max(row['Open'], row['Close'])) <= (threshold * length) if length > 0 else True

def close_near_low(row, threshold=0.2):
    length = row['High'] - row['Low']
    return (min(row['Open'], row['Close']) - row['Low']) <= (threshold * length) if length > 0 else True

def calculate_trade_params(price, atr, pattern_direction, pattern_tier):
    """
    ATR-based SL/TP.
    Tier lebih tinggi = kita lebih yakin = SL lebih ketat, RR tetap dijaga.
    Win rate 50% butuh RR >= 2:1 agar expectancy positif.
    """
    sl_atr_mult = {1: 1.5, 2: 1.2, 3: 1.0}.get(pattern_tier, 1.5)
    tp1_atr_mult = sl_atr_mult * 2.0   # Minimal 2:1
    tp2_atr_mult = sl_atr_mult * 3.0   # Trail ke 3:1

    if pattern_direction == 'bullish':
        sl  = round(price - (atr * sl_atr_mult), 0)
        tp1 = round(price + (atr * tp1_atr_mult), 0)
        tp2 = round(price + (atr * tp2_atr_mult), 0)
    elif pattern_direction == 'bearish':
        sl  = round(price + (atr * sl_atr_mult), 0)
        tp1 = round(price - (atr * tp1_atr_mult), 0)
        tp2 = round(price - (atr * tp2_atr_mult), 0)
    else:
        return {'SL': '-', 'TP1': '-', 'TP2': '-', 'RR': '-'}

    risk = abs(price - sl)
    reward = abs(tp1 - price)
    rr = round(reward / risk, 2) if risk > 0 else 0

    return {'SL': sl, 'TP1': tp1, 'TP2': tp2, 'RR': rr}

def get_tier_stars(tier):
    return {1: '⭐', 2: '⭐⭐', 3: '⭐⭐⭐'}.get(tier, '')

def get_pattern_score(base_prob, tier, confluence_count):
    """
    Hitung skor probabilistik 0–100.
    base_prob  : historical win rate dasar pola (0.0–1.0)
    tier       : 1 / 2 / 3
    confluence : jumlah filter konfirmasi yang terpenuhi (0–5)
    """
    tier_mult       = {1: 1.00, 2: 1.15, 3: 1.30}.get(tier, 1.00)
    confluence_bonus = 1 + (confluence_count * 0.05)
    return round(min(95, base_prob * 100 * tier_mult * confluence_bonus), 1)


# ==========================================
# CORE ANALYSIS FUNCTION
# ==========================================
def analyze_stock(ticker):
    try:
        df = yf.download(ticker, period="6mo", interval="1d", progress=False)
        if df.empty: return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(1)
        df.dropna(inplace=True)
        if len(df) < 60: return None

        # --------------------------------------------------
        # KALKULASI INDIKATOR
        # --------------------------------------------------

        # Moving Averages
        ma_periods = [10, 20, 50, 100, 200]
        for p in ma_periods:
            df[f'SMA_{p}'] = ta.trend.sma_indicator(df['Close'], window=p)
            df[f'EMA_{p}'] = ta.trend.ema_indicator(df['Close'], window=p)

        # Ichimoku
        ichi = ta.trend.IchimokuIndicator(high=df['High'], low=df['Low'],
                                           window1=9, window2=26, window3=52)
        df['ISA'] = ichi.ichimoku_a()
        df['ISB'] = ichi.ichimoku_b()
        df['ITS'] = ichi.ichimoku_conversion_line()
        df['IKS'] = ichi.ichimoku_base_line()

        # RSI
        df['RSI'] = ta.momentum.rsi(df['Close'], window=14)

        # Stochastic
        stoch = ta.momentum.StochasticOscillator(
            high=df['High'], low=df['Low'], close=df['Close'], window=14, smooth_window=3)
        df['STOCH_K'] = stoch.stoch()
        df['STOCH_D'] = stoch.stoch_signal()

        # CCI
        df['CCI'] = ta.trend.cci(high=df['High'], low=df['Low'], close=df['Close'], window=20)

        # ADX
        adx_ind = ta.trend.ADXIndicator(high=df['High'], low=df['Low'], close=df['Close'], window=14)
        df['ADX']  = adx_ind.adx()
        df['+DI']  = adx_ind.adx_pos()
        df['-DI']  = adx_ind.adx_neg()

        # Awesome Oscillator
        df['AO'] = ta.momentum.awesome_oscillator(
            high=df['High'], low=df['Low'], window1=5, window2=34)

        # Momentum
        df['MOM'] = df['Close'].diff(10)

        # MACD
        macd_ind = ta.trend.MACD(close=df['Close'], window_slow=26, window_fast=12, window_sign=9)
        df['MACD']        = macd_ind.macd()
        df['MACD_SIGNAL'] = macd_ind.macd_signal()

        # Stochastic RSI
        stochrsi_ind = ta.momentum.StochRSIIndicator(
            close=df['Close'], window=14, smooth1=3, smooth2=3)
        df['SRSI_K'] = stochrsi_ind.stochrsi_k() * 100
        df['SRSI_D'] = stochrsi_ind.stochrsi_d() * 100

        # Williams %R
        df['WILLR'] = ta.momentum.williams_r(
            high=df['High'], low=df['Low'], close=df['Close'], lbp=14)

        # Elder Ray (Bull & Bear Power)
        df['EMA_13']  = ta.trend.ema_indicator(df['Close'], window=13)
        df['BULLP']   = df['High'] - df['EMA_13']
        df['BEARP']   = df['Low']  - df['EMA_13']

        # Ultimate Oscillator
        df['UO'] = ta.momentum.ultimate_oscillator(
            high=df['High'], low=df['Low'], close=df['Close'], window1=7, window2=14, window3=28)

        # Volume MA
        df['VOL_SMA_20'] = df['Volume'].rolling(window=20).mean()

        # *** ATR — FILTER BARU ***
        df['ATR'] = ta.volatility.AverageTrueRange(
            high=df['High'], low=df['Low'], close=df['Close'], window=14
        ).average_true_range()
        df['ATR_SMA20'] = df['ATR'].rolling(window=20).mean()

        # --------------------------------------------------
        # SLICE CANDLES (d1=paling lama, d5=hari ini)
        # --------------------------------------------------
        d1, d2, d3, d4, d5 = df.iloc[-5], df.iloc[-4], df.iloc[-3], df.iloc[-2], df.iloc[-1]

        # Alias: day1=2 hari lalu, day2=kemarin, day3=hari ini
        day1, day2, day3 = d3, d4, d5

        # --------------------------------------------------
        # PRE-COMPUTED CANDLE METRICS
        # --------------------------------------------------
        body_day1  = abs(day1['Close'] - day1['Open'])
        body_day2  = abs(day2['Close'] - day2['Open'])
        body_day3  = abs(day3['Close'] - day3['Open'])

        range_day1 = day1['High'] - day1['Low']
        range_day2 = day2['High'] - day2['Low']
        range_day3 = day3['High'] - day3['Low']

        upper_shade_1 = day1['High'] - max(day1['Open'], day1['Close'])
        lower_shade_1 = min(day1['Open'], day1['Close']) - day1['Low']
        upper_shade_2 = day2['High'] - max(day2['Open'], day2['Close'])
        lower_shade_2 = min(day2['Open'], day2['Close']) - day2['Low']
        upper_shade_3 = day3['High'] - max(day3['Open'], day3['Close'])
        lower_shade_3 = min(day3['Open'], day3['Close']) - day3['Low']

        upper_wick_day3 = upper_shade_3
        lower_wick_day3 = lower_shade_3

        mid_point_day1 = (day1['Open'] + day1['Close']) / 2
        mid_point_day2 = (day2['Open'] + day2['Close']) / 2
        mid_point_day3 = (day3['Open'] + day3['Close']) / 2

        # FIX BUG: mid_1 untuk Abandoned Baby
        mid_1 = (day1['Open'] + day1['Close']) / 2

        bull_1 = day1['Close'] > day1['Open']
        bull_2 = day2['Close'] > day2['Open']
        bull_3 = day3['Close'] > day3['Open']
        bear_1 = day1['Open'] > day1['Close']
        bear_2 = day2['Open'] > day2['Close']
        bear_3 = day3['Open'] > day3['Close']

        atr_day3 = day3['ATR']
        valid_atr = atr_day3 > 0

        # --------------------------------------------------
        # FILTER KONTEKS — dipakai bersama oleh semua pola
        # --------------------------------------------------

        # Trend context (lebih ketat dari sebelumnya — pakai lag SMA)
        is_proper_downtrend = (
            (day3['SMA_20'] < day3['SMA_50']) and
            (day3['SMA_20'] < df['SMA_20'].iloc[-6]) and   # SMA20 masih turun
            (day3['Close']  < day3['SMA_50'])
        )
        is_proper_uptrend = (
            (day3['SMA_20'] > day3['SMA_50']) and
            (day3['SMA_20'] > df['SMA_20'].iloc[-6]) and   # SMA20 masih naik
            (day3['Close']  > day3['SMA_50'])
        )

        # Untuk pola yang hanya butuh SMA20 vs SMA50 dasar
        is_downtrend_basic = day3['SMA_20'] < day3['SMA_50']
        is_uptrend_basic   = day3['SMA_20'] > day3['SMA_50']

        # Volatility regime — pasar harus cukup bergerak
        is_volatile_regime = (day3['ATR'] > day3['ATR_SMA20'] * 0.75) if valid_atr else True

        # ATR filter untuk candle — candle harus bermakna secara volatilitas
        candle_is_significant = (range_day3 >= atr_day3 * 0.7) if valid_atr else True

        # Volume confirmation
        is_volume_thrust      = day3['Volume'] > day3['VOL_SMA_20'] * 1.5
        is_volume_above_avg   = day3['Volume'] > day3['VOL_SMA_20']

        # RSI context
        is_oversold_context   = day3['RSI'] < 45
        is_overbought_context = day3['RSI'] > 55
        is_extreme_oversold   = day3['RSI'] < 35
        is_extreme_overbought = day3['RSI'] > 65

        # S/R Proximity (proxy via 20-day rolling range)
        rolling_high_20 = df['High'].rolling(20).max().iloc[-1]
        rolling_low_20  = df['Low'].rolling(20).min().iloc[-1]
        price_range_20  = rolling_high_20 - rolling_low_20
        price_position  = (
            (day3['Close'] - rolling_low_20) / price_range_20
            if price_range_20 > 0 else 0.5
        )
        is_near_support    = price_position < 0.25
        is_near_resistance = price_position > 0.75

        # ADX / Trend strength
        is_strong_trend  = day3['ADX'] > 25
        is_ranging_market = day3['ADX'] < 20

        # MACD momentum alignment
        macd_bullish = day3['MACD'] > day3['MACD_SIGNAL']
        macd_bearish = day3['MACD'] < day3['MACD_SIGNAL']

        # --------------------------------------------------
        # TRADINGVIEW SCORE (tidak diubah logikanya)
        # --------------------------------------------------
        score, counted = 0, 0
        def add_score(val):
            nonlocal score, counted
            score += val
            counted += 1

        for p in ma_periods:
            if pd.notna(day3[f'SMA_{p}']):
                add_score(1 if day3[f'SMA_{p}'] < day3['Close'] else -1 if day3[f'SMA_{p}'] > day3['Close'] else 0)
            if pd.notna(day3[f'EMA_{p}']):
                add_score(1 if day3[f'EMA_{p}'] < day3['Close'] else -1 if day3[f'EMA_{p}'] > day3['Close'] else 0)

        if (day3['ISA'] > day3['ISB']) and (day3['IKS'] > day3['ISA']) and (day3['ITS'] > day3['IKS']) and (day3['Close'] > day3['ITS']): add_score(1)
        elif (day3['ISA'] < day3['ISB']) and (day3['IKS'] < day3['ISA']) and (day3['ITS'] < day3['IKS']) and (day3['Close'] < day3['ITS']): add_score(-1)
        else: add_score(0)

        if day3['RSI'] < 30 and day3['RSI'] > day2['RSI']:    add_score(1)
        elif day3['RSI'] > 70 and day3['RSI'] < day2['RSI']:  add_score(-1)
        else: add_score(0)

        if day3['STOCH_K'] < 20 and day3['STOCH_D'] < 20 and day3['STOCH_K'] > day3['STOCH_D']:    add_score(1)
        elif day3['STOCH_K'] > 80 and day3['STOCH_D'] > 80 and day3['STOCH_K'] < day3['STOCH_D']:  add_score(-1)
        else: add_score(0)

        if day3['CCI'] < -100 and day3['CCI'] > day2['CCI']:   add_score(1)
        elif day3['CCI'] > 100 and day3['CCI'] < day2['CCI']:  add_score(-1)
        else: add_score(0)

        if day3['+DI'] > day3['-DI'] and day3['ADX'] > 20 and day3['ADX'] > day2['ADX']:    add_score(1)
        elif day3['+DI'] < day3['-DI'] and day3['ADX'] > 20 and day3['ADX'] > day2['ADX']: add_score(-1)
        else: add_score(0)

        ao_saucer_buy  = (day3['AO'] > 0) and (d3['AO'] > d4['AO']) and (d5['AO'] > d4['AO'])
        ao_cross_buy   = (d4['AO'] < 0) and (d5['AO'] > 0)
        ao_saucer_sell = (day3['AO'] < 0) and (d3['AO'] < d4['AO']) and (d5['AO'] < d4['AO'])
        ao_cross_sell  = (d4['AO'] > 0) and (d5['AO'] < 0)
        if ao_saucer_buy or ao_cross_buy:     add_score(1)
        elif ao_saucer_sell or ao_cross_sell: add_score(-1)
        else: add_score(0)

        if day3['MOM'] > day2['MOM']:    add_score(1)
        elif day3['MOM'] < day2['MOM']:  add_score(-1)
        else: add_score(0)

        if day3['MACD'] > day3['MACD_SIGNAL']:    add_score(1)
        elif day3['MACD'] < day3['MACD_SIGNAL']:  add_score(-1)
        else: add_score(0)

        tren_naik = day3['EMA_13'] > day2['EMA_13']
        if not tren_naik and day3['SRSI_K'] < 20 and day3['SRSI_D'] < 20 and day3['SRSI_K'] > day3['SRSI_D']:   add_score(1)
        elif tren_naik and day3['SRSI_K'] > 80 and day3['SRSI_D'] > 80 and day3['SRSI_K'] < day3['SRSI_D']:     add_score(-1)
        else: add_score(0)

        if day3['WILLR'] < -80 and day3['WILLR'] > day2['WILLR']:   add_score(1)
        elif day3['WILLR'] > -20 and day3['WILLR'] < day2['WILLR']: add_score(-1)
        else: add_score(0)

        if tren_naik and day3['BEARP'] < 0 and day3['BEARP'] > day2['BEARP']:   add_score(1)
        elif tren_naik and day3['BULLP'] > 0 and day3['BULLP'] < day2['BULLP']: add_score(-1)
        else: add_score(0)

        if day3['UO'] > 70:    add_score(1)
        elif day3['UO'] < 30:  add_score(-1)
        else: add_score(0)

        final_value = score / counted if counted > 0 else 0
        if   -1.0 <= final_value < -0.5: rec = "Penjualan Kuat"
        elif -0.5 <= final_value < -0.1: rec = "Penjualan"
        elif -0.1 <= final_value <= 0.1: rec = "Netral"
        elif  0.1 < final_value <= 0.5:  rec = "Pembelian"
        elif  0.5 < final_value <= 1.0:  rec = "Pembelian Kuat"
        else: rec = "Netral"

        # --------------------------------------------------
        # PATTERN DETECTION — DENGAN TIER & BUG FIX
        # --------------------------------------------------

        # Helper: body dalam candle tertentu (body top & bottom)
        body_top_1    = max(day1['Open'], day1['Close'])
        body_bottom_1 = min(day1['Open'], day1['Close'])
        body_top_2    = max(day2['Open'], day2['Close'])
        body_bottom_2 = min(day2['Open'], day2['Close'])
        body_top_3    = max(day3['Open'], day3['Close'])
        body_bottom_3 = min(day3['Open'], day3['Close'])

        # Helper untuk middle candles (Rising/Falling 3 Methods)
        middle = df.iloc[-4:-1]

        # ==================================================
        # POLA 3 CANDLE
        # ==================================================

        # ---- 3 White Soldiers ----
        # FIX: Volume harus naik tiap candle (konfirmasi institusional)
        vol_accel_3ws = (
            (day1['Volume'] > day1['VOL_SMA_20']) and
            (day2['Volume'] >= day1['Volume']) and
            (day3['Volume'] >= day2['Volume'])
        )

        tier_3ws = 0
        is_3ws_base = (
            is_downtrend_basic and
            bull_1 and bull_2 and bull_3 and
            (body_day1 >= 0.5 * range_day1) and
            (body_day2 >= 0.5 * range_day2) and
            (body_day3 >= 0.5 * range_day3) and
            (day2['Close'] > day1['Close']) and
            (day3['Close'] > day2['Close']) and
            (day2['Open'] >= day1['Open'])  and (day2['Open'] <= day1['Close']) and
            (day3['Open'] >= day2['Open'])  and (day3['Open'] <= day2['Close']) and
            close_near_high(day1) and close_near_high(day2) and close_near_high(day3) and
            candle_is_significant
        )
        if is_3ws_base:
            sc = sum([vol_accel_3ws, is_extreme_oversold, is_near_support,
                      macd_bullish, is_volatile_regime])
            tier_3ws = 3 if sc >= 4 else 2 if sc >= 2 else 1

        # ---- 3 Black Crows ----
        vol_accel_3bc = (
            (day1['Volume'] > day1['VOL_SMA_20']) and
            (day2['Volume'] >= day1['Volume']) and
            (day3['Volume'] >= day2['Volume'])
        )

        tier_3bc = 0
        is_3bc_base = (
            is_uptrend_basic and
            bear_1 and bear_2 and bear_3 and
            (body_day1 >= 0.5 * range_day1) and
            (body_day2 >= 0.5 * range_day2) and
            (body_day3 >= 0.5 * range_day3) and
            (day2['Close'] < day1['Close']) and
            (day3['Close'] < day2['Close']) and
            (day2['Open'] <= day1['Open'])  and (day2['Open'] >= day1['Close']) and
            (day3['Open'] <= day2['Open'])  and (day3['Open'] >= day2['Close']) and
            close_near_low(day1) and close_near_low(day2) and close_near_low(day3) and
            candle_is_significant
        )
        if is_3bc_base:
            sc = sum([vol_accel_3bc, is_extreme_overbought, is_near_resistance,
                      macd_bearish, is_volatile_regime])
            tier_3bc = 3 if sc >= 4 else 2 if sc >= 2 else 1

        # ---- Morning Star ----
        sm_body_2_ms = (range_day2 > 0) and (body_day2 <= 0.3 * range_day2)
        mid_day1_ms  = (day1['Open'] + day1['Close']) / 2

        tier_morning_star = 0
        is_ms_base = (
            is_proper_downtrend and
            bear_1 and (range_day1 > 0) and (body_day1 >= 0.6 * range_day1) and
            sm_body_2_ms and
            bull_3 and (range_day3 > 0) and (body_day3 >= 0.6 * range_day3) and
            (day3['Close'] >= mid_day1_ms) and
            candle_is_significant
        )
        if is_ms_base:
            sc = sum([is_extreme_oversold, is_near_support, is_volume_thrust,
                      macd_bullish, is_volatile_regime])
            tier_morning_star = 3 if sc >= 4 else 2 if sc >= 2 else 1

        # ---- Evening Star ----
        sm_body_2_es = (range_day2 > 0) and (body_day2 <= 0.3 * range_day2)
        mid_day1_es  = (day1['Open'] + day1['Close']) / 2

        tier_evening_star = 0
        is_es_base = (
            is_proper_uptrend and
            bull_1 and (range_day1 > 0) and (body_day1 >= 0.6 * range_day1) and
            sm_body_2_es and
            bear_3 and (range_day3 > 0) and (body_day3 >= 0.6 * range_day3) and
            (day3['Close'] <= mid_day1_es) and
            candle_is_significant
        )
        if is_es_base:
            sc = sum([is_extreme_overbought, is_near_resistance, is_volume_thrust,
                      macd_bearish, is_volatile_regime])
            tier_evening_star = 3 if sc >= 4 else 2 if sc >= 2 else 1

        # ---- Abandoned Baby Bullish ----
        tier_bull_abandoned = 0
        is_bull_abandoned_base = (
            is_downtrend_basic and
            bear_1 and bull_3 and
            (body_day1 >= 0.5 * range_day1) and
            (body_day3 >= 0.5 * range_day3) and
            (body_day2 <= 0.1 * range_day2) and   # Doji
            (day2['High'] < day1['Low']) and        # Gap kiri
            (day2['High'] < day3['Low']) and        # Gap kanan
            (day3['Close'] > mid_1)                 # FIX: mid_1 sudah didefinisikan
        )
        if is_bull_abandoned_base:
            sc = sum([is_extreme_oversold, is_near_support, is_volume_thrust,
                      macd_bullish])
            tier_bull_abandoned = 3 if sc >= 3 else 2 if sc >= 2 else 1

        # ---- Abandoned Baby Bearish ----
        tier_bear_abandoned = 0
        is_bear_abandoned_base = (
            is_uptrend_basic and
            bull_1 and bear_3 and
            (body_day1 >= 0.5 * range_day1) and
            (body_day3 >= 0.5 * range_day3) and
            (body_day2 <= 0.1 * range_day2) and   # Doji
            (day2['Low']  > day1['High']) and       # Gap kiri
            (day2['Low']  > day3['High']) and       # Gap kanan
            (day3['Close'] < mid_1)                 # FIX: mid_1 sudah didefinisikan
        )
        if is_bear_abandoned_base:
            sc = sum([is_extreme_overbought, is_near_resistance, is_volume_thrust,
                      macd_bearish])
            tier_bear_abandoned = 3 if sc >= 3 else 2 if sc >= 2 else 1

        # ---- Harami Inside (untuk 3 Inside Up/Down) ----
        is_bull_harami_inside = (
            is_downtrend_basic and
            bear_1 and (body_day1 >= 0.5 * range_day1) and
            bull_2 and
            (body_day2 <= 0.6 * body_day1) and
            (body_top_2   <= body_top_1) and
            (body_bottom_2 >= body_bottom_1)
        )

        is_bear_harami_inside = (
            is_uptrend_basic and
            bull_1 and (body_day1 >= 0.5 * range_day1) and
            bear_2 and
            (body_day2 <= 0.6 * body_day1) and  # FIX: sebelumnya 0.6 * body_day2 (self-reference bug)
            (body_top_2   <= body_top_1) and
            (body_bottom_2 >= body_bottom_1)
        )

        # ---- 3 Inside Up ----
        tier_3_inside_up = 0
        is_3iu_base = (
            is_bull_harami_inside and
            bull_3 and
            (day3['Close'] > day1['High'])
        )
        if is_3iu_base:
            sc = sum([is_oversold_context, is_near_support, is_volume_above_avg, macd_bullish])
            tier_3_inside_up = 3 if sc >= 3 else 2 if sc >= 2 else 1

        # ---- 3 Inside Down ----
        tier_3_inside_down = 0
        is_3id_base = (
            is_bear_harami_inside and
            bear_3 and
            (day3['Close'] < day1['Low'])
        )
        if is_3id_base:
            sc = sum([is_overbought_context, is_near_resistance, is_volume_above_avg, macd_bearish])
            tier_3_inside_down = 3 if sc >= 3 else 2 if sc >= 2 else 1

        # ---- 3 Outside Up ----
        tier_3_outside_up = 0
        is_3ou_base = (
            is_downtrend_basic and
            bear_1 and bull_2 and
            (day2['Close'] > day1['Open']) and
            (day2['Open']  < day1['Close']) and
            (body_day2 > body_day1 * 1.2) and
            bull_3 and (body_day3 >= 0.5 * range_day3) and
            (day3['Close'] > day2['High']) and
            candle_is_significant
        )
        if is_3ou_base:
            sc = sum([is_oversold_context, is_near_support, is_volume_thrust, macd_bullish])
            tier_3_outside_up = 3 if sc >= 3 else 2 if sc >= 2 else 1

        # ---- 3 Outside Down ----
        tier_3_outside_down = 0
        is_3od_base = (
            is_uptrend_basic and
            bull_1 and bear_2 and
            (day2['Close'] < day1['Open']) and
            (day2['Open']  > day1['Close']) and
            (body_day2 > body_day1 * 1.2) and
            bear_3 and (body_day3 >= 0.5 * range_day3) and
            (day3['Close'] < day2['Low']) and
            candle_is_significant
        )
        if is_3od_base:
            sc = sum([is_overbought_context, is_near_resistance, is_volume_thrust, macd_bearish])
            tier_3_outside_down = 3 if sc >= 3 else 2 if sc >= 2 else 1

        # ---- Rising 3 Methods ----
        tier_rising_3 = 0
        try:
            body_d1_r3  = abs(d1['Close'] - d1['Open'])
            body_d5_r3  = abs(d5['Close'] - d5['Open'])
            range_d1_r3 = d1['High'] - d1['Low']
            range_d5_r3 = d5['High'] - d5['Low']
            mid_bodies  = abs(middle['Close'] - middle['Open'])

            is_rising_3_base = (
                (d1['Close'] > d1['Open']) and (body_d1_r3 > range_d1_r3 * 0.6) and
                all(middle['Close'] < middle['Open']) and
                all(middle['High'] < d1['High']) and
                all(middle['Low']  > d1['Low'])  and
                all(mid_bodies < body_d1_r3 * 0.5) and
                (d5['Close'] > d5['Open']) and (body_d5_r3 > range_d5_r3 * 0.6) and
                (d5['Close'] > d1['Close'])
            )
            if is_rising_3_base:
                sc = sum([is_uptrend_basic, is_volume_thrust, macd_bullish])
                tier_rising_3 = 3 if sc == 3 else 2 if sc == 2 else 1
        except Exception:
            pass

        # ---- Falling 3 Methods ----
        tier_falling_3 = 0
        try:
            is_falling_3_base = (
                (d1['Close'] < d1['Open']) and
                (d5['Close'] < d5['Open']) and
                (d5['Close'] < d1['Close']) and
                all(df.iloc[-4:-1]['Close'] > df.iloc[-4:-1]['Open']) and
                all(df.iloc[-4:-1]['High']  < d1['High']) and
                all(df.iloc[-4:-1]['Low']   > d1['Low'])
            )
            if is_falling_3_base:
                sc = sum([is_downtrend_basic, is_volume_thrust, macd_bearish])
                tier_falling_3 = 3 if sc == 3 else 2 if sc == 2 else 1
        except Exception:
            pass

        # ==================================================
        # POLA 2 CANDLE
        # ==================================================

        # ---- Bullish Engulfing ----
        tier_bull_engulf = 0
        is_be_base = (
            bear_2 and bull_3 and
            (day3['Close'] > day2['Open']) and
            (day3['Open']  < day2['Close']) and           # Gap down (lebih ketat)
            (body_day3 > body_day2 * 1.08) and
            (body_day2 >= 0.5 * range_day2) and
            (body_day3 >= 0.5 * range_day3) and
            (day3['Close'] < day3['SMA_50']) and
            candle_is_significant
        )
        if is_be_base:
            sc = sum([is_extreme_oversold, is_near_support, is_volume_thrust,
                      is_ranging_market, macd_bullish])
            tier_bull_engulf = 3 if sc >= 4 else 2 if sc >= 2 else 1

        # ---- Bearish Engulfing ----
        tier_bear_engulf = 0
        is_bae_base = (
            bull_2 and bear_3 and
            (day3['Open']  >= day2['Close']) and          # Gap up
            (day3['Close'] < day2['Open']) and
            (body_day3 > body_day2 * 1.08) and
            (body_day2 >= 0.5 * range_day2) and
            (body_day3 >= 0.5 * range_day3) and
            (day3['Close'] > day3['SMA_50']) and
            candle_is_significant
        )
        if is_bae_base:
            sc = sum([is_extreme_overbought, is_near_resistance, is_volume_thrust,
                      is_ranging_market, macd_bearish])
            tier_bear_engulf = 3 if sc >= 4 else 2 if sc >= 2 else 1

        # ---- Piercing Line ----
        body_bear_pl    = day2['Open'] - day2['Close']
        pen_ratio_pl    = (day3['Close'] - day2['Close']) / body_bear_pl if body_bear_pl > 0 else 0

        tier_piercing = 0
        is_pl_base = (
            is_proper_downtrend and
            bear_2 and (body_bear_pl >= 0.6 * range_day2) and
            bull_3 and
            (day3['Open'] < day2['Close'] * 0.999) and   # FIX: gap down nyata
            (pen_ratio_pl >= 0.5) and
            (day3['Close'] < day2['Open']) and            # Tidak jadi Engulfing
            candle_is_significant
        )
        if is_pl_base:
            sc = sum([is_extreme_oversold, is_near_support, is_volume_thrust, macd_bullish])
            tier_piercing = 3 if sc >= 3 else 2 if sc >= 2 else 1

        # ---- Dark Cloud Cover ----
        tier_dark_cloud = 0
        is_dc_base = (
            bull_2 and bear_3 and
            (day3['Open']  > day2['High']) and            # Gap up nyata
            (day3['Close'] <= mid_point_day2) and
            (day3['Close'] > day2['Open']) and
            (day3['Close'] > day3['SMA_50']) and
            candle_is_significant
        )
        if is_dc_base:
            sc = sum([is_extreme_overbought, is_near_resistance, is_volume_thrust, macd_bearish])
            tier_dark_cloud = 3 if sc >= 3 else 2 if sc >= 2 else 1

        # ---- Bullish Harami ----
        tier_bull_harami = 0
        is_bh_base = (
            is_downtrend_basic and
            bear_2 and (body_day2 >= 0.5 * range_day2) and
            bull_3 and
            (body_day3 >= 0.2 * range_day3) and
            (body_day3 <= 0.6 * body_day2) and
            (body_top_3    <= body_top_2)   and
            (body_bottom_3 >= body_bottom_2)
        )
        if is_bh_base:
            sc = sum([is_oversold_context, is_near_support, is_volume_above_avg, macd_bullish])
            tier_bull_harami = 3 if sc >= 3 else 2 if sc >= 2 else 1

        # ---- Bearish Harami ----
        tier_bear_harami = 0
        is_bah_base = (
            is_uptrend_basic and
            bull_2 and (body_day2 >= 0.5 * range_day2) and
            bear_3 and
            (body_day3 <= 0.6 * body_day2) and
            (body_top_3    <= body_top_2)   and
            (body_bottom_3 >= body_bottom_2)
        )
        if is_bah_base:
            sc = sum([is_overbought_context, is_near_resistance, is_volume_above_avg, macd_bearish])
            tier_bear_harami = 3 if sc >= 3 else 2 if sc >= 2 else 1

        # ---- Kicker Bullish ----
        tier_bull_kicker = 0
        is_bk_base = (
            bear_2 and (body_day2 >= 0.5 * range_day2) and
            bull_3 and (body_day3 >= 0.5 * range_day3) and
            (day3['Open'] > day2['High']) and
            (day3['Low']  > day2['High']) and
            candle_is_significant
        )
        if is_bk_base:
            sc = sum([is_oversold_context, is_near_support, is_volume_thrust, macd_bullish])
            tier_bull_kicker = 3 if sc >= 3 else 2 if sc >= 2 else 1

        # ---- Kicker Bearish ----
        tier_bear_kicker = 0
        is_bak_base = (
            bull_2 and (body_day2 >= 0.5 * range_day2) and
            bear_3 and (body_day3 >= 0.5 * range_day3) and
            (day3['Open'] < day2['Low']) and
            candle_is_significant
        )
        if is_bak_base:
            sc = sum([is_overbought_context, is_near_resistance, is_volume_thrust, macd_bearish])
            tier_bear_kicker = 3 if sc >= 3 else 2 if sc >= 2 else 1

        # ---- Island Reversal Bullish ----
        tier_bull_island = 0
        is_bui_base = (
            (d1['Low'] > max(d2['High'], d3['High'], d4['High'])) and
            (d5['Low'] > max(d2['High'], d3['High'], d4['High'])) and
            (d5['Close'] > d5['Open'])
        )
        if is_bui_base:
            sc = sum([is_oversold_context, is_volume_thrust, macd_bullish])
            tier_bull_island = 3 if sc == 3 else 2 if sc == 2 else 1

        # ---- Island Reversal Bearish ----
        tier_bear_island = 0
        is_bai_base = (
            (d1['High'] < min(d2['Low'], d3['Low'], d4['Low'])) and
            (d5['High'] < min(d2['Low'], d3['Low'], d4['Low'])) and
            (d5['Close'] < d5['Open'])
        )
        if is_bai_base:
            sc = sum([is_overbought_context, is_volume_thrust, macd_bearish])
            tier_bear_island = 3 if sc == 3 else 2 if sc == 2 else 1

        # ---- Tweezer Bottom ----
        tier_tweezer_bottom = 0
        is_tweezer_bottom = (
            is_downtrend_basic and
            bear_2 and (body_day2 >= 0.5 * range_day2) and
            bull_3 and (body_day3 >= 0.5 * range_day3) and
            is_near(day2['Low'], day3['Low']) and
            (day3['Close'] > day2['Close']) and
            (range_day3 >= atr_day3 * 0.7) if valid_atr else True
        )
        if is_tweezer_bottom:
            sc = sum([is_oversold_context, is_near_support, is_volume_above_avg, macd_bullish])
            tier_tweezer_bottom = 3 if sc >= 3 else 2 if sc >= 2 else 1

        # ---- Tweezer Top ----  FIX: variabel terpisah, tidak lagi menimpa bottom
        tier_tweezer_top = 0
        is_tweezer_top = (
            is_uptrend_basic and
            bull_2 and (body_day2 >= 0.5 * range_day2) and
            bear_3 and (body_day3 >= 0.5 * range_day3) and
            is_near(day2['High'], day3['High']) and
            (day3['Close'] < day2['Close']) and
            (range_day3 >= atr_day3 * 0.7) if valid_atr else True
        )
        if is_tweezer_top:
            sc = sum([is_overbought_context, is_near_resistance, is_volume_above_avg, macd_bearish])
            tier_tweezer_top = 3 if sc >= 3 else 2 if sc >= 2 else 1

        # ==================================================
        # POLA 1 CANDLE
        # ==================================================

        valid_range_day3 = range_day3 > 0

        # ---- Doji variants ----
        is_doji = valid_range_day3 and (body_day3 <= 0.08 * range_day3)

        is_long_legged_doji = (
            is_doji and
            (upper_wick_day3 >= 0.35 * range_day3) and
            (lower_wick_day3 >= 0.35 * range_day3)
        )
        is_gravestone_doji = (
            is_doji and
            (upper_wick_day3 >= 0.70 * range_day3) and
            (lower_wick_day3 <= 0.10 * range_day3)
        )
        is_dragonfly_doji = (
            is_doji and
            (lower_wick_day3 >= 0.70 * range_day3) and
            (upper_wick_day3 <= 0.10 * range_day3)
        )

        # ---- Hammer ----
        tier_hammer = 0
        is_hammer_base = (
            valid_range_day3 and (body_day3 > 0) and
            (body_day3     <= 0.30 * range_day3) and
            (lower_shade_3 >= 2   * body_day3)   and
            (upper_shade_3 <= 0.15 * range_day3) and
            ((day3['High'] - day3['Close']) <= 0.25 * range_day3) and
            (day3['Close'] < day3['SMA_20']) and
            candle_is_significant
        )
        if is_hammer_base:
            sc = sum([is_proper_downtrend, is_extreme_oversold, is_near_support,
                      is_volume_above_avg])
            tier_hammer = 3 if sc >= 3 else 2 if sc >= 2 else 1

        # ---- Inverted Hammer ----
        tier_inv_hammer = 0
        is_inv_hammer_base = (
            valid_range_day3 and (body_day3 > 0) and
            (body_day3     <= 0.30 * range_day3) and
            (upper_wick_day3 >= 2  * body_day3)  and
            (lower_wick_day3 <= 0.15 * range_day3) and
            ((day3['Close'] - day3['Low']) <= 0.25 * range_day3) and
            (day3['SMA_20'] < day3['SMA_50']) and
            candle_is_significant
        )
        if is_inv_hammer_base:
            sc = sum([is_proper_downtrend, is_oversold_context, is_near_support])
            tier_inv_hammer = 3 if sc == 3 else 2 if sc == 2 else 1

        # ---- Shooting Star ----
        is_valid_uptrend_ss = (
            (day3['SMA_20'] > day3['SMA_50']) and
            (day3['SMA_20'] > df['SMA_20'].iloc[-3]) and
            (day3['Close']  > df['Close'].iloc[-3])  and
            (df['Close'].iloc[-3] > df['Close'].iloc[-6])
        )
        tier_shooting_star = 0
        is_ss_base = (
            valid_range_day3 and (body_day3 > 0) and
            (body_day3     <= 0.30 * range_day3) and
            (upper_shade_3 >= 2   * body_day3)   and
            (upper_shade_3 >= 0.60 * range_day3) and
            (lower_shade_3 <= 0.15 * range_day3) and
            ((day3['Close'] - day3['Low']) <= 0.25 * range_day3) and
            bear_3 and
            is_valid_uptrend_ss and
            candle_is_significant
        )
        if is_ss_base:
            sc = sum([is_proper_uptrend, is_extreme_overbought, is_near_resistance,
                      is_volume_above_avg])
            tier_shooting_star = 3 if sc >= 3 else 2 if sc >= 2 else 1

        # ---- Hanging Man ----
        tier_hanging_man = 0
        is_hm_base = (
            valid_range_day3 and (body_day3 > 0) and
            (body_day3     <= 0.30 * range_day3) and
            (lower_wick_day3 >= 2  * body_day3)  and
            (upper_wick_day3 <= 0.15 * range_day3) and
            ((day3['High'] - day3['Close']) <= 0.25 * range_day3) and
            (day3['SMA_20'] > day3['SMA_50']) and bull_2 and
            candle_is_significant
        )
        if is_hm_base:
            sc = sum([is_proper_uptrend, is_overbought_context, is_near_resistance])
            tier_hanging_man = 3 if sc == 3 else 2 if sc == 2 else 1

        # ---- Spinning Top ----
        is_spinning_top = (
            valid_range_day3 and
            (0.10 * range_day3 < body_day3 <= 0.35 * range_day3) and
            (upper_wick_day3 >= 0.25 * range_day3) and
            (lower_wick_day3 >= 0.25 * range_day3)
        )

        # ==================================================
        # PRIORITY LADDER + SCORING
        # Urutan: pola paling rare/kuat di atas
        # ==================================================

        # Base historical win rates (estimasi konservatif)
        BASE_PROB = {
            'bull_abandoned':   0.65, 'bear_abandoned':   0.65,
            '3ws':              0.60, '3bc':              0.60,
            'morning_star':     0.60, 'evening_star':     0.60,
            '3_outside_up':     0.58, '3_outside_down':   0.58,
            'bull_kicker':      0.58, 'bear_kicker':      0.58,
            'bull_island':      0.57, 'bear_island':      0.57,
            '3_inside_up':      0.56, '3_inside_down':    0.56,
            'rising_3':         0.55, 'falling_3':        0.55,
            'bear_engulf':      0.54, 'bull_engulf':      0.54,
            'dark_cloud':       0.53, 'piercing':         0.53,
            'tweezer_bottom':   0.52, 'tweezer_top':      0.52,
            'bear_harami':      0.51, 'bull_harami':      0.51,
            'shooting_star':    0.52, 'hammer':           0.52,
            'gravestone_doji':  0.50, 'dragonfly_doji':   0.50,
            'inv_hammer':       0.50, 'hanging_man':      0.50,
        }

        pola      = "-"
        tier      = 0
        direction = "neutral"
        pat_score = 0

        # --- 3 Candle ---
        if tier_bull_abandoned > 0:
            pola, tier, direction = "Bullish: Abandoned Baby", tier_bull_abandoned, "bullish"
            pat_score = get_pattern_score(BASE_PROB['bull_abandoned'], tier, sum([is_extreme_oversold, is_near_support, is_volume_thrust, macd_bullish]))
        elif tier_bear_abandoned > 0:
            pola, tier, direction = "Bearish: Abandoned Baby", tier_bear_abandoned, "bearish"
            pat_score = get_pattern_score(BASE_PROB['bear_abandoned'], tier, sum([is_extreme_overbought, is_near_resistance, is_volume_thrust, macd_bearish]))
        elif tier_3ws > 0:
            pola, tier, direction = "Bullish: 3 White Soldiers", tier_3ws, "bullish"
            pat_score = get_pattern_score(BASE_PROB['3ws'], tier, sum([vol_accel_3ws, is_extreme_oversold, is_near_support, macd_bullish]))
        elif tier_3bc > 0:
            pola, tier, direction = "Bearish: 3 Black Crows", tier_3bc, "bearish"
            pat_score = get_pattern_score(BASE_PROB['3bc'], tier, sum([vol_accel_3bc, is_extreme_overbought, is_near_resistance, macd_bearish]))
        elif tier_morning_star > 0:
            pola, tier, direction = "Bullish: Morning Star", tier_morning_star, "bullish"
            pat_score = get_pattern_score(BASE_PROB['morning_star'], tier, sum([is_extreme_oversold, is_near_support, is_volume_thrust, macd_bullish]))
        elif tier_evening_star > 0:
            pola, tier, direction = "Bearish: Evening Star", tier_evening_star, "bearish"
            pat_score = get_pattern_score(BASE_PROB['evening_star'], tier, sum([is_extreme_overbought, is_near_resistance, is_volume_thrust, macd_bearish]))
        elif tier_3_outside_up > 0:
            pola, tier, direction = "Bullish: 3 Outside Up", tier_3_outside_up, "bullish"
            pat_score = get_pattern_score(BASE_PROB['3_outside_up'], tier, sum([is_oversold_context, is_near_support, is_volume_thrust, macd_bullish]))
        elif tier_3_outside_down > 0:
            pola, tier, direction = "Bearish: 3 Outside Down", tier_3_outside_down, "bearish"
            pat_score = get_pattern_score(BASE_PROB['3_outside_down'], tier, sum([is_overbought_context, is_near_resistance, is_volume_thrust, macd_bearish]))
        elif tier_3_inside_up > 0:
            pola, tier, direction = "Bullish: 3 Inside Up", tier_3_inside_up, "bullish"
            pat_score = get_pattern_score(BASE_PROB['3_inside_up'], tier, sum([is_oversold_context, is_near_support, is_volume_above_avg, macd_bullish]))
        elif tier_3_inside_down > 0:
            pola, tier, direction = "Bearish: 3 Inside Down", tier_3_inside_down, "bearish"
            pat_score = get_pattern_score(BASE_PROB['3_inside_down'], tier, sum([is_overbought_context, is_near_resistance, is_volume_above_avg, macd_bearish]))
        elif tier_rising_3 > 0:
            pola, tier, direction = "Bullish Cont: Rising 3 Methods", tier_rising_3, "bullish"
            pat_score = get_pattern_score(BASE_PROB['rising_3'], tier, sum([is_uptrend_basic, is_volume_thrust, macd_bullish]))
        elif tier_falling_3 > 0:
            pola, tier, direction = "Bearish Cont: Falling 3 Methods", tier_falling_3, "bearish"
            pat_score = get_pattern_score(BASE_PROB['falling_3'], tier, sum([is_downtrend_basic, is_volume_thrust, macd_bearish]))

        # --- 2 Candle ---
        elif tier_bull_kicker > 0:
            pola, tier, direction = "Bullish: Kicker", tier_bull_kicker, "bullish"
            pat_score = get_pattern_score(BASE_PROB['bull_kicker'], tier, sum([is_oversold_context, is_near_support, is_volume_thrust, macd_bullish]))
        elif tier_bear_kicker > 0:
            pola, tier, direction = "Bearish: Kicker", tier_bear_kicker, "bearish"
            pat_score = get_pattern_score(BASE_PROB['bear_kicker'], tier, sum([is_overbought_context, is_near_resistance, is_volume_thrust, macd_bearish]))
        elif tier_bull_island > 0:
            pola, tier, direction = "Bullish: Island Reversal", tier_bull_island, "bullish"
            pat_score = get_pattern_score(BASE_PROB['bull_island'], tier, sum([is_oversold_context, is_volume_thrust, macd_bullish]))
        elif tier_bear_island > 0:
            pola, tier, direction = "Bearish: Island Reversal", tier_bear_island, "bearish"
            pat_score = get_pattern_score(BASE_PROB['bear_island'], tier, sum([is_overbought_context, is_volume_thrust, macd_bearish]))
        elif tier_bear_engulf > 0:
            pola, tier, direction = "Bearish: Engulfing", tier_bear_engulf, "bearish"
            pat_score = get_pattern_score(BASE_PROB['bear_engulf'], tier, sum([is_extreme_overbought, is_near_resistance, is_volume_thrust, is_ranging_market, macd_bearish]))
        elif tier_bull_engulf > 0:
            pola, tier, direction = "Bullish: Engulfing", tier_bull_engulf, "bullish"
            pat_score = get_pattern_score(BASE_PROB['bull_engulf'], tier, sum([is_extreme_oversold, is_near_support, is_volume_thrust, is_ranging_market, macd_bullish]))
        elif tier_dark_cloud > 0:
            pola, tier, direction = "Bearish: Dark Cloud Cover", tier_dark_cloud, "bearish"
            pat_score = get_pattern_score(BASE_PROB['dark_cloud'], tier, sum([is_extreme_overbought, is_near_resistance, is_volume_thrust, macd_bearish]))
        elif tier_piercing > 0:
            pola, tier, direction = "Bullish: Piercing Line", tier_piercing, "bullish"
            pat_score = get_pattern_score(BASE_PROB['piercing'], tier, sum([is_extreme_oversold, is_near_support, is_volume_thrust, macd_bullish]))
        elif tier_tweezer_bottom > 0:
            pola, tier, direction = "Bullish: Tweezer Bottom", tier_tweezer_bottom, "bullish"
            pat_score = get_pattern_score(BASE_PROB['tweezer_bottom'], tier, sum([is_oversold_context, is_near_support, is_volume_above_avg, macd_bullish]))
        elif tier_tweezer_top > 0:
            pola, tier, direction = "Bearish: Tweezer Top", tier_tweezer_top, "bearish"
            pat_score = get_pattern_score(BASE_PROB['tweezer_top'], tier, sum([is_overbought_context, is_near_resistance, is_volume_above_avg, macd_bearish]))
        elif tier_bear_harami > 0:
            pola, tier, direction = "Bearish: Harami", tier_bear_harami, "bearish"
            pat_score = get_pattern_score(BASE_PROB['bear_harami'], tier, sum([is_overbought_context, is_near_resistance, is_volume_above_avg, macd_bearish]))
        elif tier_bull_harami > 0:
            pola, tier, direction = "Bullish: Harami", tier_bull_harami, "bullish"
            pat_score = get_pattern_score(BASE_PROB['bull_harami'], tier, sum([is_oversold_context, is_near_support, is_volume_above_avg, macd_bullish]))

        # --- 1 Candle ---
        elif tier_shooting_star > 0:
            pola, tier, direction = "Bearish: Shooting Star", tier_shooting_star, "bearish"
            pat_score = get_pattern_score(BASE_PROB['shooting_star'], tier, sum([is_proper_uptrend, is_extreme_overbought, is_near_resistance, is_volume_above_avg]))
        elif tier_hammer > 0:
            pola, tier, direction = "Bullish: Hammer", tier_hammer, "bullish"
            pat_score = get_pattern_score(BASE_PROB['hammer'], tier, sum([is_proper_downtrend, is_extreme_oversold, is_near_support, is_volume_above_avg]))
        elif is_gravestone_doji:
            pola, tier, direction = "Bearish: Gravestone Doji", 1, "bearish"
            pat_score = get_pattern_score(BASE_PROB['gravestone_doji'], 1, sum([is_overbought_context, is_near_resistance]))
        elif is_dragonfly_doji:
            pola, tier, direction = "Bullish: Dragonfly Doji", 1, "bullish"
            pat_score = get_pattern_score(BASE_PROB['dragonfly_doji'], 1, sum([is_oversold_context, is_near_support]))
        elif tier_inv_hammer > 0:
            pola, tier, direction = "Bullish: Inverted Hammer", tier_inv_hammer, "bullish"
            pat_score = get_pattern_score(BASE_PROB['inv_hammer'], tier, sum([is_proper_downtrend, is_oversold_context, is_near_support]))
        elif tier_hanging_man > 0:
            pola, tier, direction = "Bearish: Hanging Man", tier_hanging_man, "bearish"
            pat_score = get_pattern_score(BASE_PROB['hanging_man'], tier, sum([is_proper_uptrend, is_overbought_context, is_near_resistance]))
        elif is_long_legged_doji:
            pola, tier, direction = "⚠ Indecision Doji", 1, "neutral"
        elif is_spinning_top:
            pola, tier, direction = "Netral: Spinning Top", 0, "neutral"

        # Tambahkan bintang tier ke nama pola
        if tier > 0:
            pola = f"{pola} {get_tier_stars(tier)}"

        # --------------------------------------------------
        # TRADE PARAMETERS (SL / TP / RR)
        # --------------------------------------------------
        trade = calculate_trade_params(
            price=day3['Close'],
            atr=atr_day3 if valid_atr else 0,
            pattern_direction=direction,
            pattern_tier=tier
        )

        # --------------------------------------------------
        # S/R LABEL
        # --------------------------------------------------
        sr_label = "Support" if is_near_support else "Resistance" if is_near_resistance else "-"

        return {
            "Ticker"         : ticker,
            "Harga"          : round(day3['Close'], 0),
            "Skor TV"        : round(final_value, 2),
            "Rek TV"         : rec,
            "Pola"           : pola,
            "Tier"           : tier,                           # 0–3 (filter di GSheet)
            "Skor Pola"      : pat_score,                      # 0–95 (sort descending)
            "Arah"           : direction,
            "SL"             : trade['SL'],
            "TP1"            : trade['TP1'],
            "TP2"            : trade['TP2'],
            "RR"             : trade['RR'],
            "ATR"            : round(atr_day3, 0) if valid_atr else '-',
            "RSI"            : round(day3['RSI'], 1),
            "ADX"            : round(day3['ADX'], 1),
            "Vol Thrust"     : "✅" if is_volume_thrust else "-",
            "S/R Zone"       : sr_label,
            "Price Pos%"     : round(price_position * 100, 0),  # 0=support, 100=resistance
            "Waktu"          : datetime.now(timezone(timedelta(hours=7))).strftime("%Y-%m-%d %H:%M")
        }

    except Exception as e:
        print(f"❌ Error pada {ticker}: {e}")
        return None


# ==========================================
# ANALYZE SECTOR
# ==========================================
def analyze_sector(sheet_name, saham_list):
    print(f"\n📊 Scan sektor: {sheet_name} ({len(saham_list)} emiten)")
    results = []
    for ticker in saham_list:
        res = analyze_stock(ticker)
        if res:
            results.append(res)

    if results:
        df_out = pd.DataFrame(results)
        # Sort: Tier ⬇, Skor Pola ⬇
        df_out = df_out.sort_values(by=['Tier', 'Skor Pola'], ascending=[False, False])
        return df_out
    return pd.DataFrame()


# ==========================================
# SECTOR CONFIG
# ==========================================
SECTOR_CONFIG = {
    "IDXINDUST": [
        "AMFG.JK", "AMIN.JK", "APII.JK", "ARKA.JK", "ARNA.JK", "ASGR.JK", "ASII.JK", "BHIT.JK", "BINO.JK", "BLUE.JK", 
        "BNBR.JK", "CAKK.JK", "CCSI.JK", "CRSN.JK", "CTTH.JK", "DYAN.JK", "FOLK.JK", "GPSO.JK", "HEXA.JK", "HOPE.JK", 
        "HYGN.JK", "IBFN.JK", "ICON.JK", "IKAI.JK", "IKBI.JK", "IMPC.JK", "INDX.JK", "INTA.JK", "JECC.JK", "JTPE.JK", 
        "KBLI.JK", "KBLM.JK", "KIAS.JK", "KING.JK", "KOBX.JK", "KOIN.JK", "KONI.JK", "KUAS.JK", "LABA.JK", "LION.JK", "MARK.JK", 
        "MDRN.JK", "MFMI.JK", "MHKI.JK", "MLIA.JK", "MUTU.JK", "NAIK.JK", "NTBK.JK", "PADA.JK", "PIPA.JK", "PTMP.JK", 
        "SCCO.JK", "SINI.JK", "SKRN.JK", "SMIL.JK", "SOSS.JK", "SPTO.JK", "TIRA.JK", "TOTO.JK", "TRIL.JK", "UNTR.JK", 
        "VISI.JK", "VOKS.JK", "ZBRA.JK"
    ],
    "IDXNONCYC": [
        "AALI.JK", "ADES.JK", "AGAR.JK", "AISA.JK", "ALTO.JK", "AMMS.JK", "AMRT.JK", "ANDI.JK", "ANJT.JK", "ASHA.JK", 
        "AYAM.JK", "BEEF.JK", "BEER.JK", "BISI.JK", "BOBA.JK", "BRRC.JK", "BTEK.JK", "BUAH.JK", "BUDI.JK", "BWPT.JK", 
        "CAMP.JK", "CBUT.JK", "CEKA.JK", "CLEO.JK", "CMRY.JK", "COCO.JK", "CPIN.JK", "CRAB.JK", "CPRO.JK", "CSRA.JK", 
        "DAYA.JK", "DEWI.JK", "DLTA.JK", "DMND.JK", "DPUM.JK", "DSFI.JK", "DSNG.JK", "ENZO.JK", "EPMT.JK", "EURO.JK", "FAPA.JK", 
        "FISH.JK", "FLMC.JK", "FOOD.JK", "FORE.JK", "GGRM.JK", "GOLL.JK", "GOOD.JK", "GRPM.JK", "GULA.JK", "GUNA.JK", "GZCO.JK", 
        "HERO.JK", "HMSD.JK", "HMSP.JK", "HOKI.JK","IBOS.JK", "ICBP.JK", "IKAN.JK", "INDF.JK", "IPPE.JK", "ISEA.JK", "ITIC.JK", "JARR.JK", 
        "JAWA.JK", "JPFA.JK", "KEJU.JK", "KINO.JK", "KMDS.JK", "LAPD.JK", "LSIP.JK", "MAGP.JK", "MAIN.JK", "MAXI.JK", 
        "MBTO.JK", "MGRO.JK", "MIDI.JK", "MKTR.JK", "MLBI.JK", "MLPL.JK", "MPPA.JK", "MRAT.JK", "MSJA.JK", "MYOR.JK","NANO.JK", 
        "NASI.JK", "NAYZ.JK", "NEST.JK", "NSSS.JK", "OILS.JK", "PCAR.JK", "PGUN.JK", "PMMP.JK", "PNGO.JK", "PSDN.JK", "PSGO.JK", 
        "PTPS.JK", "RANC.JK", "RLCO.JK", "ROTI.JK", "SDPC.JK", "SGRO.JK", "SIMP.JK", "SIPD.JK", "SKBM.JK", "SKLT.JK", 
        "SMAR.JK","SOUL.JK", "SSMS.JK", "STAA.JK", "STRK.JK", "STTP.JK", "TAPG.JK", "TAYS.JK", "TBLA.JK", "TCID.JK", "TGKA.JK", 
        "TGUK.JK", "TLDN.JK", "TRGU.JK", "UCID.JK", "UDNG.JK", "ULTJ.JK", "UNSP.JK", "UNVR.JK", "VICI.JK", "WAPO.JK", 
        "WICO.JK", "WIIM.JK", "WINE.JK", "WMPP.JK", "WMUU.JK", "YUPI.JK"
    ],
    "IDXFINANCE": [
        "ABDA.JK", "ADMF.JK", "AGRO.JK", "AGRS.JK", "AHAP.JK", "AMAG.JK", "AMAR.JK", "AMOR.JK", "APIC.JK", "ARTO.JK", 
        "ASBI.JK", "ASDM.JK", "ASJT.JK", "ASMI.JK", "ASRM.JK", "BABP.JK", "BACA.JK", "BANK.JK", "BBCA.JK", "BBHI.JK", 
        "BBKP.JK", "BBLD.JK", "BBMD.JK", "BBNI.JK", "BBRI.JK", "BBSI.JK", "BBTN.JK", "BBYB.JK", "BCAP.JK", "BCIC.JK", 
        "BDMN.JK", "BEKS.JK", "BFIN.JK", "BGTG.JK", "BHAT.JK", "BHIT.JK", "BINA.JK", "BJBR.JK", "BJTM.JK", "BKSW.JK", "BMAS.JK", 
        "BMRI.JK", "BNBA.JK", "BNGA.JK", "BNII.JK", "BNLI.JK", "BPFI.JK", "BPII.JK", "BRIS.JK", "BSIM.JK", "BSWD.JK", 
        "BTPN.JK", "BTPS.JK", "BVIC.JK", "CASA.JK", "CFIN.JK", "COIN.JK", "DEFI.JK", "DNAR.JK", "DNET.JK", "FUJI.JK", "GSMF.JK", "HBAT.JK", 
        "HDFA.JK", "INPC.JK", "IPAC.JK", "JMAS.JK","KIJA.JK", "LIFE.JK", "LPGI.JK", "LPPS.JK", "MASB.JK", "MAYA.JK", "MCOR.JK", "MEGA.JK", 
        "MREI.JK", "MSIE.JK", "MTWI.JK", "NICK.JK", "NISP.JK", "NOBU.JK", "OCAP.JK", "PADI.JK", "PALM.JK", "PANS.JK", "PEGE.JK", 
        "PLAS.JK", "PNBN.JK", "PNBS.JK", "PNIN.JK", "PNLF.JK", "POLA.JK", "POOL.JK", "RELF.JK","RELI.JK", "SDRA.JK", "SFAN.JK", 
        "SMMA.JK", "SRTG.JK", "STAR.JK", "SUPA.JK", "TIFA.JK", "TRIM.JK", "TRUS.JK", "TUGU.JK", "VICO.JK", "VINS.JK", 
        "VRNA.JK", "VTNY.JK", "WIDI.JK", "WOMF.JK", "YOII.JK", "YULE.JK"
    ],
    "IDXCYCLIC": [
        "ABBA.JK", "ACES.JK", "ACRO.JK", "AEGS.JK", "AKKU.JK", "ARGO.JK", "ARTA.JK", "ASLC.JK", "AUTO.JK", "BABY.JK", 
        "BAIK.JK", "BATA.JK", "BAUT.JK", "BAYU.JK", "BELL.JK", "BIKE.JK", "BIMA.JK", "BLTZ.JK", "BMBL.JK", "BMTR.JK", "BOGA.JK", 
        "BOLA.JK", "BOLT.JK", "BRAM.JK", "BUVA.JK", "CARS.JK", "CBMF.JK", "CINT.JK", "CLAY.JK", "CNMA.JK", "CNTX.JK", 
        "CSAP.JK", "CSMI.JK", "DEPO.JK", "DFAM.JK", "DIGI.JK", "DOOH.JK", "DOSS.JK", "DRMA.JK", "DUCK.JK", "EAST.JK", 
        "ECII.JK", "ENAK.JK", "ERAA.JK", "ERAL.JK", "ERTX.JK", "ESTA.JK", "ESTI.JK", "FAST.JK", "FILM.JK", "FITT.JK", 
        "FORU.JK", "FUTR.JK", "GDYR.JK", "GEMA.JK", "GJTL.JK", "GLOB.JK", "GOLF.JK", "GRPH.JK", "GWSA.JK","HAJJ.JK", "HOME.JK", 
        "HOTL.JK", "HRME.JK", "HRTA.JK", "IDEA.JK", "IIKP.JK", "IMAS.JK", "INDR.JK", "INDS.JK", "INOV.JK", "IPTV.JK", "ISAP.JK", 
        "JGLE.JK", "JIHD.JK", "JSPT.JK", "KAQI.JK", "KDTN.JK", "KICI.JK", "KLIN.JK", "KOTA.JK", "KPIG.JK", "LFLO.JK", "LIVE.JK", "LMAX.JK", 
        "LMPI.JK", "LPIN.JK", "LPPF.JK", "LUCY.JK", "MABA.JK", "MAPA.JK", "MAPB.JK", "MAPI.JK", "MARI.JK", "MDIA.JK", "MDIY.JK", "MEJA.JK", 
        "MERI.JK", "MGNA.JK", "MGLV.JK", "MICE.JK", "MINA.JK", "MNCN.JK", "MPMX.JK", "MSIN.JK", "MSKY.JK", "MYTX.JK", "NATO.JK", 
        "NETV.JK", "NUSA.JK", "OLIV.JK", "PANR.JK", "PART.JK", "PBRX.JK", "PDES.JK", "PGLI.JK", "PJAA.JK", "PLAN.JK","PMJS.JK", "PMUI.JK", 
        "PNSE.JK", "POLU.JK", "POLY.JK", "PSKT.JK", "PTSP.JK", "PZZA.JK", "RAAM.JK", "RAFI.JK", "RALS.JK", "RICY.JK", 
        "SBAT.JK", "SCMA.JK", "SCNP.JK", "SHID.JK", "SLIS.JK", "SMSM.JK", "SNLK.JK","SOFA.JK" "SONA.JK", "SOTS.JK", 
       "SPRE.JK", "SRIL.JK", "SSTM.JK", "SWID.JK", "TELE.JK", "TFCO.JK", "TMPO.JK", "TOOL.JK", "TOYS.JK", "TRIO.JK", 
       "TRIS.JK", "TYRE.JK",  "UFOE.JK", "UNIT.JK", "UNTD.JK", "VERN.JK", "VIVA.JK", "VKTR.JK", "WOOD.JK", "YELO.JK", 
       "ZATA.JK", "ZONE.JK"
    ],
    "IDXTECHNO": [
        "AREA.JK", "ATIC.JK", "AWAN.JK", "AXIO.JK", "BELI.JK", "BUKA.JK", "CASH.JK", "CHIP.JK", "CYBR.JK", "DCII.JK", 
        "DIVA.JK", "DMMX.JK", "EDGE.JK", "ELIT.JK", "EMTK.JK", "ENVY.JK", "GLVA.JK", "GOTO.JK", "HDIT.JK", "IOTF.JK", 
        "IRSX.JK", "JATI.JK", "KIOS.JK", "KREN.JK", "LMAS.JK", "LUCK.JK", "MCAS.JK", "MLPT.JK", "MPIX.JK", "MSTI.JK", 
        "MTDL.JK", "NFCX.JK", "NINE.JK", "PGJO.JK", "PTSN.JK", "RUNS.JK", "SKYB.JK", "TECH.JK", "TFAS.JK", "TOSK.JK", 
        "TRON.JK", "UVCR.JK", "WGSH.JK", "WIFI.JK", "WIRG.JK", "ZYRX.JK"
    ],
    "IDXBASIC": [
        "ADMG.JK", "AGII.JK", "AKPI.JK", "ALDO.JK", "ALKA.JK", "ALMI.JK", "AMMN.JK", "ANTM.JK", "APLI.JK", "ARCI.JK", 
        "ASPR.JK", "AVIA.JK", "AYLS.JK", "BAJA.JK", "BATR.JK", "BEBS.JK", "BLES.JK", "BMSR.JK", "BRMS.JK", "BRNA.JK", 
        "BRPT.JK", "BTON.JK", "CHEM.JK", "CITA.JK", "CLPI.JK", "CMNT.JK", "CTBN.JK", "DAAZ.JK", "DGWG.JK", "DKFT.JK", 
        "DPNS.JK", "EKAD.JK", "EMAS.JK", "EPAC.JK", "ESIP.JK", "ESSA.JK", "ETWA.JK", "FASW.JK", "FPNI.JK", "FWCT.JK", 
        "GDST.JK", "GGRP.JK", "HKMU.JK", "IFII.JK", "IFSH.JK", "IGAR.JK", "INAI.JK", "INCF.JK", "INCI.JK", "INCO.JK", 
        "INKP.JK", "INRU.JK", "INTD.JK", "INTP.JK", "IPOL.JK", "ISSP.JK", "KAYU.JK", "KBRI.JK", "KDSI.JK", "KKES.JK", 
        "KMTR.JK", "KRAS.JK", "LMSH.JK", "LTLS.JK", "MBMA.JK", "MDKA.JK", "MDKI.JK", "MINE.JK", "MOLI.JK", "NCKL.JK", 
        "NICE.JK", "NICL.JK", "NIKL.JK", "NPGF.JK", "OBMD.JK", "OKAS.JK", "OPMS.JK", "PACK.JK", "PBID.JK", "PDPP.JK", 
        "PICO.JK", "PPRI.JK", "PSAB.JK", "PTMR.JK", "PURE.JK", "SAMF.JK", "SBMA.JK", "SIMA.JK", "SMBR.JK", "SMCB.JK", 
        "SMGA.JK", "SMGR.JK", "SMKL.JK", "SMLE.JK", "SOLA.JK", "SPMA.JK", "SQMI.JK", "SRSN.JK", "SULI.JK", "SWAT.JK", 
        "TALF.JK", "TBMS.JK", "TDPM.JK", "TINS.JK", "TIRT.JK", "TKIM.JK", "TPIA.JK", "TRST.JK", "UNIC.JK", "WSBP.JK", 
        "WTON.JK", "YPAS.JK", "ZINC.JK"
    ],
    "IDXENERGY": [
        "AADI.JK", "ABMM.JK", "ADMR.JK", "ADRO.JK", "AIMS.JK", "AKRA.JK", "ALII.JK", "APEX.JK", "ARII.JK", "ARTI.JK", 
        "ATLA.JK", "BBRM.JK", "BESS.JK", "BIPI.JK", "BOAT.JK", "BOSS.JK", "BSML.JK", "BSSR.JK", "BULL.JK", "BUMI.JK", 
        "BYAN.JK", "CANI.JK", "CBRE.JK", "CGAS.JK", "CNKO.JK", "COAL.JK", "CUAN.JK", "DEWA.JK", "DOID.JK", "DSSA.JK", 
        "DWGL.JK", "ELSA.JK", "ENRG.JK", "FIRE.JK", "GEMS.JK", "GTBO.JK", "GTSI.JK", "HILL.JK", "HITS.JK", "HRUM.JK", 
        "HUMI.JK", "IATA.JK", "INDY.JK", "INPS.JK", "ITMA.JK", "ITMG.JK", "JSKY.JK", "KKGI.JK", "KOPI.JK", "LEAD.JK", 
        "MAHA.JK", "MBAP.JK", "MBSS.JK", "MCOL.JK", "MEDC.JK", "MKAP.JK", "MTFN.JK", "MYOH.JK", "PGAS.JK", "PKPK.JK", 
        "PSAT.JK", "PSSI.JK", "PTBA.JK", "PTIS.JK", "PTRO.JK", "RAJA.JK", "RATU.JK", "RGAS.JK", "RIGS.JK", "RMKE.JK", 
        "RMKO.JK", "RUIS.JK", "SEMA.JK", "SGER.JK", "SHIP.JK", "SICO.JK", "SMMT.JK", "SMRU.JK", "SOCI.JK", "SUGI.JK", 
        "SUNI.JK", "SURE.JK", "TAMU.JK", "TCPI.JK", "TEBE.JK", "TOBA.JK", "TPMA.JK", "TRAM.JK", "UNIQ.JK", "WINS.JK", 
        "WOWS.JK"
    ],
    "IDXHEALTH": [
        "BMHS.JK", "CARE.JK", "CHEK.JK", "DGNS.JK", "DKHH.JK", "DVLA.JK", "HALO.JK", "HEAL.JK", "IKPM.JK", "INAF.JK", 
        "IRRA.JK", "KAEF.JK", "KLBF.JK", "LABS.JK", "MDLA.JK", "MEDS.JK", "MERK.JK", "MIKA.JK", "MMIX.JK", "MTMH.JK", 
        "OBAT.JK", "OMED.JK", "PEHA.JK", "PEVE.JK", "PRDA.JK", "PRAY.JK", "PRIM.JK", "PYFA.JK", "RSCH.JK", "RSGK.JK", 
        "SAME.JK", "SCPI.JK", "SIDO.JK", "SILO.JK", "SOHO.JK", "SRAJ.JK", "SURI.JK", "TSPC.JK"
    ],
    "IDXINFRA": [
        "ACST.JK", "ADHI.JK", "ARKO.JK", "ASLI.JK", "BALI.JK", "BDKR.JK", "BREN.JK", "BTEL.JK", "BUKK.JK", "CASS.JK", 
        "CDIA.JK", "CENT.JK", "CMNP.JK", "DATA.JK", "DGIK.JK", "EXCL.JK", "GHON.JK", "GMFI.JK", "GOLD.JK", "HADE.JK", 
        "HGII.JK", "IBST.JK", "IDPR.JK", "INET.JK", "IPCC.JK", "IPCM.JK", "ISAT.JK", "JAST.JK", "JKON.JK", "JSMR.JK", 
        "KARW.JK", "KBLV.JK", "KEEN.JK", "KETR.JK", "KOKA.JK", "KRYA.JK", "LCKM.JK", "LINK.JK", "META.JK", "MORA.JK", 
        "MPOW.JK", "MTEL.JK", "MTPS.JK", "MTRA.JK", "NRCA.JK", "OASA.JK", "PBSA.JK", "PGEO.JK", "PORT.JK", "POWR.JK", 
        "PPRE.JK", "PTDU.JK", "PTPP.JK", "PTPW.JK", "RONY.JK", "SMKM.JK","SSIA.JK", "SUPR.JK", "TAMA.JK", "TBIG.JK", 
        "TGRA.JK", "TLKM.JK", "TOPS.JK", "TOTL.JK", "TOWR.JK", "WEGE.JK", "WIKA.JK", "WSKT.JK"
    ],
    "IDXPROPERT": [
        "ADCP.JK", "AMAN.JK", "APLN.JK", "ARMY.JK", "ASPI.JK", "ASRI.JK", "ATAP.JK", "BAPA.JK", "BAPI.JK", "BBSS.JK", 
        "BCIP.JK", "BEST.JK", "BIKA.JK", "BIPP.JK", "BKDP.JK", "BKSL.JK", "BSBK.JK", "BSDE.JK", "CBDK.JK", "CBPE.JK", 
        "CITY.JK", "COWL.JK", "CPRI.JK", "CSIS.JK", "CTRA.JK", "DADA.JK", "DART.JK", "DILD.JK", "DMAS.JK", "DUTI.JK", 
        "ELTY.JK", "EMDE.JK", "FMII.JK", "GAMA.JK", "GMTD.JK", "GPRA.JK", "GRIA.JK", "HOMI.JK", "INDO.JK", "INPP.JK", 
        "JRPT.JK", "KBAG.JK", "KLJA.JK", "KOCI.JK", "KSIX.JK", "LAND.JK", "LCGP.JK", "LPCK.JK", "LPKR.JK", "LPLI.JK", "MANG.JK", 
        "MDLN.JK", "MKPI.JK", "MMLP.JK", "MPRO.JK", "MTLA.JK", "MTSM.JK", "NASA.JK", "NIRO.JK", "NZIA.JK", "OMRE.JK", 
        "PAMG.JK", "PANI.JK", "PLIN.JK", "POLI.JK", "POLL.JK", "POSA.JK", "PPRO.JK", "PUDP.JK", "PURI.JK", "PWON.JK", 
        "RBMS.JK", "RDTX.JK", "REAL.JK", "RIMO.JK", "RISE.JK", "ROCK.JK", "RODA.JK", "SAGE.JK", "SATU.JK", "SMDM.JK", 
        "SMRA.JK", "TARA.JK", "TRIN.JK", "TRUE.JK", "UANG.JK", "URBN.JK", "VAST.JK", "WINR.JK"
    ],
    "IDXTRANS": [
        "AKSI.JK", "ASSA.JK", "BIRD.JK", "BLOG.JK", "BLTA.JK", "BPTR.JK", "CMPP.JK", "DEAL.JK", "ELPI.JK", "GIAA.JK", 
        "GTRA.JK", "HAIS.JK", "HATM.JK", "HELI.JK", "IMJS.JK", "JAYA.JK", "KJEN.JK", "KLAS.JK", "LAJU.JK", "LOPI.JK", 
        "LRNA.JK", "MIRA.JK", "MITI.JK", "MPXL.JK", "NELY.JK", "PJHB.JK", "PPGL.JK", "PURA.JK", "RCCC.JK", "SAFE.JK", "SAPX.JK", 
        "SDMU.JK", "SMDR.JK", "TAXI.JK", "TMAS.JK", "TNCA.JK", "TRJA.JK", "TRUK.JK", "WEHA.JK"
    ]
}


# ==========================================
# MAIN EXECUTION
# ==========================================
if __name__ == "__main__":
    print("🤖 START MARKET SCANNER PRO v2.0 🤖")
    print("=" * 50)
    print("Fitur baru: ATR Filter | Tier System | SL/TP | Probabilistic Score")
    print("=" * 50)

    for sheet_name, saham_list in SECTOR_CONFIG.items():
        df_final = analyze_sector(sheet_name, saham_list)

        if df_final.empty:
            print(f"⚠️  Tidak ada data untuk {sheet_name}")
            continue

        ws = connect_gsheet(sheet_name)
        if ws:
            try:
                ws.clear()
                set_with_dataframe(ws, df_final)
                # Print ringkasan sinyal penting
                top = df_final[df_final['Tier'] >= 2]
                print(f"✅ {sheet_name} — {len(df_final)} emiten | {len(top)} sinyal Tier 2-3")
            except Exception as e:
                print(f"❌ Upload Error {sheet_name}: {e}")

        time.sleep(2)  # Jeda untuk limit API Google

    print("\n🏁 SEMUA SEKTOR SELESAI 🏁")
