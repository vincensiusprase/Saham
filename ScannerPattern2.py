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
        "ASII.JK", "UNTR.JK", "PIPA.JK", "BNBR.JK", "HEXA.JK", "IMPC.JK", "MHKI.JK", "LABA.JK", "SMIL.JK", "NTBK.JK",
        "PADA.JK", "NAIK.JK", "TOTO.JK", "BHIT.JK", "MARK.JK", "INDX.JK", "PTMP.JK", "ARNA.JK", "KOBX.JK", "ASGR.JK",
        "GPSO.JK", "SINI.JK", "HOPE.JK", "FOLK.JK", "KUAS.JK", "KBLI.JK", "SPTO.JK", "IKAI.JK", "SKRN.JK", "MUTU.JK",
        "CTTH.JK", "CAKK.JK", "CCSI.JK", "MLIA.JK", "JTPE.JK", "BLUE.JK", "CRSN.JK", "DYAN.JK", "LION.JK", "MDRN.JK",
        "VOKS.JK", "SCCO.JK", "KBLM.JK", "JECC.JK", "INTA.JK", "BINO.JK", "VISI.JK", "ARKA.JK", "SOSS.JK", "IKBI.JK",
        "AMIN.JK", "ICON.JK", "KONI.JK", "AMFG.JK", "IBFN.JK", "ZBRA.JK", "TIRA.JK", "KIAS.JK", "APII.JK", "HYGN.JK",
        "KOIN.JK", "MFMI.JK", "TRIL.JK"
    ],
    "IDXNONCYC": [
        "UNVR.JK", "INDF.JK", "ICBP.JK", "GGRM.JK", "AALI.JK", "JPFA.JK", "MYOR.JK", "CPIN.JK", "RLCO.JK", "LSIP.JK",
        "HMSD.JK", "AMRT.JK", "GZCO.JK", "MLPL.JK", "JARR.JK", "BWPT.JK", "ULTJ.JK", "FORE.JK", "TAPG.JK", "CPRO.JK",
        "SIMP.JK", "CLEO.JK", "WIIM.JK", "MPPA.JK", "HOKI.JK", "COCO.JK", "DSNG.JK", "IKAN.JK", "SMAR.JK", "BRRC.JK",
        "PTPS.JK", "SSMS.JK", "GOOD.JK", "PGUN.JK", "SGRO.JK", "BEEF.JK", "BTEK.JK", "MAIN.JK", "MIDI.JK", "AYAM.JK",
        "WMUU.JK", "DEWI.JK", "ASHA.JK", "CMRY.JK", "AISA.JK", "CAMP.JK", "OILS.JK", "ANJT.JK", "TBLA.JK", "ROTI.JK",
        "NASI.JK", "ITIC.JK", "DSFI.JK", "ISEA.JK", "JAWA.JK", "NSSS.JK", "CSRA.JK", "STAA.JK", "KEJU.JK", "UNSP.JK",
        "MRAT.JK", "DPUM.JK", "PSDN.JK", "STRK.JK", "WAPO.JK", "MBTO.JK", "GULA.JK", "LAPD.JK", "RANC.JK", "NEST.JK",
        "TAYS.JK", "CEKA.JK", "ADES.JK", "KINO.JK", "BISI.JK", "ANDI.JK", "PMMP.JK", "DMND.JK", "UCID.JK", "YUPI.JK",
        "BEER.JK", "PSGO.JK", "BUDI.JK", "SKBM.JK", "MLBI.JK", "TLDN.JK", "FOOD.JK", "MKTR.JK", "SKLT.JK", "MAXI.JK",
        "STTP.JK", "PNGO.JK", "PCAR.JK", "GUNA.JK", "EPMT.JK", "DLTA.JK", "HERO.JK", "TGUK.JK", "VICI.JK", "MSJA.JK",
        "SDPC.JK", "MGRO.JK", "FISH.JK", "BOBA.JK", "KMDS.JK", "TCID.JK", "BUAH.JK", "TGKA.JK", "WINE.JK", "ENZO.JK",
        "CBUT.JK", "WMPP.JK", "IPPE.JK", "DAYA.JK", "SIPD.JK", "CRAB.JK", "FAPA.JK", "AGAR.JK", "TRGU.JK", "ALTO.JK",
        "MAGP.JK", "GOLL.JK", "WICO.JK"
    ],
    "IDXFINANCE": [
        "BBCA.JK", "BBRI.JK", "BMRI.JK", "BBNI.JK", "BRIS.JK", "SUPA.JK", "COIN.JK", "BBTN.JK", "ARTO.JK", "BBYB.JK",
        "BNGA.JK", "BBKP.JK", "BTPS.JK", "BJTM.JK", "SRTG.JK", "PNLF.JK", "PADI.JK", "AGRO.JK", "NISP.JK", "INPC.JK",
        "BJBR.JK", "BBHI.JK", "BFIN.JK", "BDMN.JK", "BABP.JK", "PNBS.JK", "BGTG.JK", "AHAP.JK", "BANK.JK", "BACA.JK",
        "BNLI.JK", "BNII.JK", "BCAP.JK", "PNBN.JK", "MEGA.JK", "BVIC.JK", "ADMF.JK", "DNAR.JK", "MAYA.JK", "CFIN.JK",
        "BTPN.JK", "BSIM.JK", "BEKS.JK", "TUGU.JK", "PEGE.JK", "NOBU.JK", "PALM.JK", "BNBA.JK", "LPPS.JK", "AGRS.JK",
        "DNET.JK", "AMAR.JK", "GSMF.JK", "JMAS.JK", "TRIM.JK", "MCOR.JK", "PNIN.JK", "SMMA.JK", "PANS.JK", "BKSW.JK",
        "VINS.JK", "BCIC.JK", "BINA.JK", "WOMF.JK", "LPGI.JK", "LIFE.JK", "VTNY.JK", "VICO.JK", "STAR.JK", "YOII.JK",
        "FUJI.JK", "MTWI.JK", "POLA.JK", "BBSI.JK", "ASJT.JK", "SDRA.JK", "BMAS.JK", "AMAG.JK", "ASMI.JK", "HDFA.JK",
        "VRNA.JK", "AMOR.JK", "APIC.JK", "MREI.JK", "ASDM.JK", "TIFA.JK", "BHIT.JK", "ASRM.JK", "RELI.JK", "NICK.JK",
        "TRUS.JK", "ASBI.JK", "DEFI.JK", "BBLD.JK", "BBMD.JK", "MASB.JK", "BPFI.JK", "YULE.JK", "BPII.JK", "POOL.JK",
        "BSWD.JK", "SFAN.JK", "ABDA.JK", "OCAP.JK", "PLAS.JK"
    ],
    "IDXCYCLIC": [
        "MNCN.JK", "SCMA.JK", "LPPF.JK", "MINA.JK", "BUVA.JK", "ACES.JK", "ERAA.JK", "HRTA.JK", "FUTR.JK", "MAPI.JK",
        "AUTO.JK", "GJTL.JK", "FAST.JK", "VKTR.JK", "DOOH.JK", "BMTR.JK", "MPMX.JK", "FILM.JK", "RALS.JK", "KPIG.JK",
        "MAPA.JK", "SLIS.JK", "ZATA.JK", "SMSM.JK", "JGLE.JK", "ASLC.JK", "IMAS.JK", "MERI.JK", "NETV.JK", "KAQI.JK",
        "CNMA.JK", "MSIN.JK", "WOOD.JK", "BELL.JK", "PSKT.JK", "VIVA.JK", "MSKY.JK", "BABY.JK", "YELO.JK", "IPTV.JK",
        "TMPO.JK", "JIHD.JK", "DOSS.JK", "PMUI.JK", "SRIL.JK", "ERAL.JK", "DRMA.JK", "GOLF.JK", "ESTA.JK", "DFAM.JK",
        "PBRX.JK", "PZZA.JK", "BAIK.JK", "MDIA.JK", "CARS.JK", "ABBA.JK", "GEMA.JK", "PART.JK", "SWID.JK", "EAST.JK",
        "MARI.JK", "UNTD.JK", "KDTN.JK", "ACRO.JK", "ERTX.JK", "VERN.JK", "BOLA.JK", "KOTA.JK", "MDIY.JK", "FITT.JK",
        "TOOL.JK", "INDR.JK", "LIVE.JK", "PJAA.JK", "RAAM.JK", "INOV.JK", "CINT.JK", "KICI.JK", "FORU.JK", "ECII.JK",
        "GRPH.JK", "PANR.JK", "NATO.JK", "LPIN.JK", "CSMI.JK", "TRIS.JK", "UFOE.JK", "BOGA.JK", "SSTM.JK", "MGNA.JK",
        "DEPO.JK", "ESTI.JK", "POLU.JK", "SOTS.JK", "INDS.JK", "RAFI.JK", "BAYU.JK", "TOYS.JK", "GDYR.JK", "SONA.JK",
        "MAPB.JK", "PGLI.JK", "BAUT.JK", "GWSA.JK", "HRME.JK", "BIKE.JK", "DIGI.JK", "JSPT.JK", "MICE.JK", "LMPI.JK",
        "CSAP.JK", "BIMA.JK", "POLY.JK", "SHID.JK", "PTSP.JK", "SBAT.JK", "SCNP.JK", "RICY.JK", "BRAM.JK", "ENAK.JK",
        "PMJS.JK", "SNLK.JK", "TELE.JK", "BATA.JK", "ARGO.JK", "ZONE.JK", "BOLT.JK", "PNSE.JK", "DUCK.JK", "TYRE.JK",
        "CLAY.JK", "ARTA.JK", "IIKP.JK", "PDES.JK", "CBMF.JK", "BLTZ.JK", "HOME.JK", "TFCO.JK", "GLOB.JK", "AKKU.JK",
        "MYTX.JK", "CNTX.JK", "UNIT.JK", "TRIO.JK", "NUSA.JK", "HOTL.JK", "MABA.JK"
    ],
    "IDXTECHNO": [
        "GOTO.JK", "WIFI.JK", "EMTK.JK", "BUKA.JK", "WIRG.JK", "DCII.JK", "IOTF.JK", "MTDL.JK", "ELIT.JK", "MLPT.JK",
        "DMMX.JK", "TOSK.JK", "JATI.JK", "KIOS.JK", "IRSX.JK", "UVCR.JK", "TRON.JK", "KREN.JK", "CYBR.JK", "LUCK.JK",
        "PTSN.JK", "HDIT.JK", "EDGE.JK", "DIVA.JK", "TFAS.JK", "ZYRX.JK", "MSTI.JK", "MCAS.JK", "MPIX.JK", "BELI.JK",
        "AXIO.JK", "AWAN.JK", "AREA.JK", "NFCX.JK", "ATIC.JK", "TECH.JK", "GLVA.JK", "ENVY.JK", "LMAS.JK", "SKYB.JK"
    ],
    "IDXBASIC": [
        "ANTM.JK", "BRMS.JK", "SMGR.JK", "BRPT.JK", "INTP.JK", "EMAS.JK", "MDKA.JK", "INCO.JK", "TINS.JK", "ARCI.JK",
        "TPIA.JK", "MBMA.JK", "INKP.JK", "PSAB.JK", "NCKL.JK", "AMMN.JK", "ESSA.JK", "TKIM.JK", "KRAS.JK", "DKFT.JK",
        "NICL.JK", "FPNI.JK", "WSBP.JK", "SMBR.JK", "WTON.JK", "SMGA.JK", "AGII.JK", "AVIA.JK", "NIKL.JK", "SOLA.JK",
        "ISSP.JK", "MINE.JK", "DAAZ.JK", "OKAS.JK", "OPMS.JK", "BAJA.JK", "NICE.JK", "CHEM.JK", "ZINC.JK", "PPRI.JK",
        "AYLS.JK", "SRSN.JK", "EKAD.JK", "PBID.JK", "PICO.JK", "ESIP.JK", "CITA.JK", "MOLI.JK", "GDST.JK", "SULI.JK",
        "TIRT.JK", "MDKI.JK", "ADMG.JK", "SPMA.JK", "SMLE.JK", "CLPI.JK", "ASPR.JK", "NPGF.JK", "BLES.JK", "BATR.JK",
        "DGWG.JK", "GGRP.JK", "FWCT.JK", "TBMS.JK", "PDPP.JK", "LTLS.JK", "SAMF.JK", "BMSR.JK", "BEBS.JK", "SBMA.JK",
        "PTMR.JK", "IPOL.JK", "UNIC.JK", "OBMD.JK", "KAYU.JK", "SMCB.JK", "IGAR.JK", "INCI.JK", "INCF.JK", "EPAC.JK",
        "INAI.JK", "ALDO.JK", "HKMU.JK", "SQMI.JK", "SMKL.JK", "IFII.JK", "IFSH.JK", "PURE.JK", "SWAT.JK", "BTON.JK",
        "TALF.JK", "KDSI.JK", "INRU.JK", "CMNT.JK", "INTD.JK", "ALKA.JK", "KMTR.JK", "CTBN.JK", "YPAS.JK", "KKES.JK",
        "AKPI.JK", "DPNS.JK", "APLI.JK", "TRST.JK", "BRNA.JK", "LMSH.JK", "ALMI.JK", "FASW.JK", "ETWA.JK", "TDPM.JK",
        "SIMA.JK", "KBRI.JK"
    ],
    "IDXENERGY": [
        "ADRO.JK", "BUMI.JK", "PGAS.JK", "PTBA.JK", "ITMG.JK", "DEWA.JK", "CUAN.JK", "HRUM.JK", "PTRO.JK", "RAJA.JK",
        "MEDC.JK", "ADMR.JK", "HUMI.JK", "ENRG.JK", "BULL.JK", "TOBA.JK", "AADI.JK", "RATU.JK", "CBRE.JK", "INDY.JK",
        "AKRA.JK", "ELSA.JK", "GTSI.JK", "BIPI.JK", "COAL.JK", "BSSR.JK", "LEAD.JK", "APEX.JK", "TEBE.JK", "ATLA.JK",
        "SOCI.JK", "FIRE.JK", "PSAT.JK", "GEMS.JK", "DOID.JK", "DSSA.JK", "SGER.JK", "IATA.JK", "BBRM.JK", "BYAN.JK",
        "ABMM.JK", "TPMA.JK", "MAHA.JK", "BOAT.JK", "KKGI.JK", "MBSS.JK", "WOWS.JK", "CGAS.JK", "RMKE.JK", "WINS.JK",
        "MTFN.JK", "MBAP.JK", "UNIQ.JK", "RMKO.JK", "SMMT.JK", "SICO.JK", "BSML.JK", "PSSI.JK", "DWGL.JK", "TAMU.JK",
        "ALII.JK", "ITMA.JK", "RUIS.JK", "CNKO.JK", "TCPI.JK", "HILL.JK", "BOSS.JK", "PKPK.JK", "MYOH.JK", "SEMA.JK",
        "ARII.JK", "GTBO.JK", "MCOL.JK", "RGAS.JK", "SHIP.JK", "BESS.JK", "RIGS.JK", "JSKY.JK", "KOPI.JK", "PTIS.JK",
        "CANI.JK", "ARTI.JK", "INPS.JK", "MKAP.JK", "AIMS.JK", "HITS.JK", "SUNI.JK", "TRAM.JK", "SURE.JK", "SMRU.JK",
        "SUGI.JK"
    ],
    "IDXHEALTH": [
        "KLBF.JK", "SIDO.JK", "KAEF.JK", "PYFA.JK", "MIKA.JK", "DKHH.JK", "SILO.JK", "HEAL.JK", "TSPC.JK", "INAF.JK",
        "CHEK.JK", "IRRA.JK", "SAME.JK", "MEDS.JK", "PRDA.JK", "MDLA.JK", "SURI.JK", "PRIM.JK", "HALO.JK", "OBAT.JK",
        "CARE.JK", "MERK.JK", "DGNS.JK", "SOHO.JK", "BMHS.JK", "PEHA.JK", "SRAJ.JK", "MMIX.JK", "DVLA.JK", "OMED.JK",
        "PEVE.JK", "LABS.JK", "RSCH.JK", "MTMH.JK", "IKPM.JK", "PRAY.JK", "SCPI.JK", "RSGK.JK"
    ],
    "IDXINFRA": [
        "TLKM.JK", "CDIA.JK", "ADHI.JK", "JSMR.JK", "WIKA.JK", "PTPP.JK", "INET.JK", "WSKT.JK", "BREN.JK", "PGEO.JK",
        "EXCL.JK", "ISAT.JK", "TOWR.JK", "SSIA.JK", "DATA.JK", "OASA.JK", "PPRE.JK", "TBIG.JK", "POWR.JK", "NRCA.JK",
        "WEGE.JK", "TOTL.JK", "KETR.JK", "IPCC.JK", "KOKA.JK", "KBLV.JK", "MTEL.JK", "CENT.JK", "KRYA.JK", "GMFI.JK",
        "JAST.JK", "KEEN.JK", "JKON.JK", "ACST.JK", "ASLI.JK", "PBSA.JK", "IPCM.JK", "MORA.JK", "ARKO.JK", "MPOW.JK",
        "CMNP.JK", "LINK.JK", "HGII.JK", "DGIK.JK", "BDKR.JK", "META.JK", "KARW.JK", "CASS.JK", "BUKK.JK", "TGRA.JK",
        "GOLD.JK", "BALI.JK", "PTDU.JK", "IDPR.JK", "PORT.JK", "TOPS.JK", "HADE.JK", "TAMA.JK", "BTEL.JK", "GHON.JK",
        "SUPR.JK", "MTPS.JK", "RONY.JK", "IBST.JK", "LCKM.JK", "PTPW.JK", "MTRA.JK"
    ],
    "IDXPROPERT": [
        "CTRA.JK", "BSDE.JK", "PWON.JK", "SMRA.JK", "KLJA.JK", "PANI.JK", "BKSL.JK", "DADA.JK", "CBDK.JK", "DMAS.JK",
        "ASRI.JK", "LPKR.JK", "BSBK.JK", "REAL.JK", "ELTY.JK", "APLN.JK", "TRUE.JK", "TRIN.JK", "UANG.JK", "CSIS.JK",
        "DILD.JK", "KOCI.JK", "BEST.JK", "LAND.JK", "DUTI.JK", "EMDE.JK", "LPLI.JK", "GRIA.JK", "VAST.JK", "BAPI.JK",
        "MTLA.JK", "SAGE.JK", "BBSS.JK", "HOMI.JK", "PUDP.JK", "RBMS.JK", "URBN.JK", "TARA.JK", "CBPE.JK", "MPRO.JK",
        "RODA.JK", "SATU.JK", "NASA.JK", "FMII.JK", "BKDP.JK", "GMTD.JK", "PPRO.JK", "BAPA.JK", "PAMG.JK", "MMLP.JK",
        "PURI.JK", "GPRA.JK", "LPCK.JK", "MDLN.JK", "BCIP.JK", "ADCP.JK", "CITY.JK", "RISE.JK", "WINR.JK", "JRPT.JK",
        "AMAN.JK", "SMDM.JK", "INDO.JK", "ATAP.JK", "ASPI.JK", "KSIX.JK", "KBAG.JK", "NZIA.JK", "NIRO.JK", "DART.JK",
        "BIPP.JK", "PLIN.JK", "RDTX.JK", "ROCK.JK", "MKPI.JK", "INPP.JK", "MTSM.JK", "POLL.JK", "POLI.JK", "OMRE.JK",
        "GAMA.JK", "POSA.JK", "BIKA.JK", "CPRI.JK", "ARMY.JK", "COWL.JK", "RIMO.JK", "LCGP.JK"
    ],
    "IDXTRANS": [
        "PJHB.JK", "GIAA.JK", "SMDR.JK", "BIRD.JK", "BLOG.JK", "IMJS.JK", "ASSA.JK", "TMAS.JK", "LAJU.JK", "HAIS.JK",
        "KLAS.JK", "MITI.JK", "JAYA.JK", "NELY.JK", "WEHA.JK", "TNCA.JK", "CMPP.JK", "MPXL.JK", "KJEN.JK", "SDMU.JK",
        "TRUK.JK", "PURA.JK", "HATM.JK", "TAXI.JK", "ELPI.JK", "AKSI.JK", "GTRA.JK", "TRJA.JK", "MIRA.JK", "BLTA.JK",
        "SAPX.JK", "SAFE.JK", "LRNA.JK", "DEAL.JK", "BPTR.JK", "HELI.JK"
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
