# ==========================================
# MARKET SCANNER - PRO
# Keltner + VWAP + HA + UT BOT + RUBBER BAND
# + SMC LuxAlgo Style (OB Internal & Swing, BOS/CHoCH, FVG)
# Timeframe: Harian (1d)
# Tujuan: Deteksi saham yang menyentuh area Bullish Order Block
# ==========================================

import numpy as np
import yfinance as yf
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from datetime import datetime
import pytz
import warnings
import json
import os
import time
from google.oauth2.service_account import Credentials

warnings.filterwarnings('ignore')

SPREADSHEET_ID = "1QbdNwITMBF0MZXh3ousJ8WwHFYIaAxNxzNPwHOtSXlo"

# ==========================================
# PARAMETER SMC (Mirip LuxAlgo)
# ==========================================
INTERNAL_SWING_LENGTH = 5    # Internal Structure (sama dengan LuxAlgo default internal)
SWING_LENGTH          = 50   # Swing Structure (sama dengan LuxAlgo default swing)
OB_FILTER_ATR_PERIOD  = 200  # ATR period untuk filter OB volatilitas tinggi (LuxAlgo pakai 200)
MAX_OB_LOOKBACK       = 50   # Maksimal candle lookback saat cari OB setelah BOS


# ==========================================
# GOOGLE SHEET CONNECTION
# ==========================================
def connect_gsheet(target_sheet_name):
    try:
        creds_json = os.environ.get("GCP_SA_KEY")
        if not creds_json:
            print("❌ GCP_SA_KEY tidak ditemukan di environment variables.")
            return None
        creds_dict = json.loads(creds_json)
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        gc    = gspread.authorize(creds)
        sh    = gc.open_by_key(SPREADSHEET_ID)
        try:
            worksheet = sh.worksheet(target_sheet_name)
        except Exception:
            worksheet = sh.add_worksheet(title=target_sheet_name, rows="300", cols="30")
        return worksheet
    except Exception as e:
        print(f"❌ Error Koneksi GSheet: {e}")
        return None


# ==========================================
# HELPER: ATR (Simple, mirip ta.atr Pine)
# ==========================================
def calc_atr(df, period):
    hl  = df['High'] - df['Low']
    hc  = (df['High'] - df['Close'].shift(1)).abs()
    lc  = (df['Low']  - df['Close'].shift(1)).abs()
    tr  = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    return tr.rolling(window=period, min_periods=1).mean()


# ==========================================
# SMC STEP 1: Parsed High/Low
# Filter candle high-volatility (high-low >= 2*ATR)
# Mirip LuxAlgo: parsedHigh = highVolBar ? low : high
#                parsedLow  = highVolBar ? high : low
# ==========================================
def get_parsed_hl(df, atr_200):
    high_vol = (df['High'] - df['Low']) >= (2.0 * atr_200)
    parsed_high = np.where(high_vol, df['Low'],  df['High'])
    parsed_low  = np.where(high_vol, df['High'], df['Low'])
    return pd.Series(parsed_high, index=df.index), pd.Series(parsed_low, index=df.index)


# ==========================================
# SMC STEP 2: Deteksi Swing High / Swing Low
# Menggunakan rolling window (center=True) sesuai Pine leg()
# ==========================================
def get_swing_points(df, length):
    """
    Mengembalikan boolean Series swing_high dan swing_low.
    pivot high: high[length] adalah max dari 2*length bar sekitarnya
    pivot low : low[length]  adalah min dari 2*length bar sekitarnya
    Sesuai LuxAlgo leg(size): high[size] > ta.highest(size)
    """
    win = 2 * length + 1
    roll_max = df['High'].rolling(window=win, center=True).max()
    roll_min = df['Low'].rolling(window=win, center=True).min()
    swing_high = (df['High'] == roll_max)
    swing_low  = (df['Low']  == roll_min)
    return swing_high, swing_low


# ==========================================
# SMC STEP 3: Deteksi BOS / CHoCH + Order Blocks
# Mirip LuxAlgo displayStructure()
# ==========================================
def detect_structure_and_ob(df, parsed_high, parsed_low, swing_high, swing_low, atr_200, label="Swing"):
    """
    Mengembalikan list of dict Order Block:
    {
        'type'       : 'Bullish' atau 'Bearish',
        'structure'  : 'BOS' atau 'CHoCH',
        'ob_high'    : float,
        'ob_low'     : float,
        'ob_idx'     : int (bar index OB),
        'active'     : bool,
        'label'      : label (Internal/Swing)
    }
    """
    closes       = df['Close'].values
    highs        = df['High'].values
    lows         = df['Low'].values
    ph_arr       = parsed_high.values
    pl_arr       = parsed_low.values
    sh_arr       = swing_high.values   # bool
    sl_arr       = swing_low.values    # bool

    n = len(df)

    # State
    last_swing_high_price = np.nan
    last_swing_high_idx   = -1
    last_swing_low_price  = np.nan
    last_swing_low_idx    = -1
    trend_bias            = 0   # 0=unknown, 1=BULLISH, -1=BEARISH

    # Simpan semua pivot yang terdeteksi (untuk crossed logic)
    swing_high_crossed = False
    swing_low_crossed  = False

    order_blocks = []

    for i in range(n):
        # Update swing pivot saat terdeteksi
        if sh_arr[i]:
            last_swing_high_price  = highs[i]
            last_swing_high_idx    = i
            swing_high_crossed     = False

        if sl_arr[i]:
            last_swing_low_price   = lows[i]
            last_swing_low_idx     = i
            swing_low_crossed      = False

        # --- Deteksi Bullish BOS/CHoCH (close crossover swing high) ---
        if (not np.isnan(last_swing_high_price)
                and closes[i] > last_swing_high_price
                and not swing_high_crossed
                and last_swing_high_idx >= 0):

            swing_high_crossed = True
            structure_type     = 'CHoCH' if trend_bias == -1 else 'BOS'
            trend_bias         = 1  # BULLISH

            # Cari Bullish OB: candle dengan parsed_low minimum
            # antara last_swing_high_idx dan i (mirip storeOrderBlock BULLISH)
            search_start = last_swing_high_idx
            search_end   = i
            if search_end > search_start:
                segment   = pl_arr[search_start:search_end]
                local_idx = int(np.argmin(segment))
                ob_idx    = search_start + local_idx
                ob_high   = ph_arr[ob_idx]
                ob_low    = pl_arr[ob_idx]

                order_blocks.append({
                    'type'       : 'Bullish',
                    'structure'  : structure_type,
                    'ob_high'    : ob_high,
                    'ob_low'     : ob_low,
                    'ob_idx'     : ob_idx,
                    'active'     : True,
                    'label'      : label
                })

        # --- Deteksi Bearish BOS/CHoCH (close crossunder swing low) ---
        if (not np.isnan(last_swing_low_price)
                and closes[i] < last_swing_low_price
                and not swing_low_crossed
                and last_swing_low_idx >= 0):

            swing_low_crossed  = True
            structure_type     = 'CHoCH' if trend_bias == 1 else 'BOS'
            trend_bias         = -1  # BEARISH

            # Cari Bearish OB: candle dengan parsed_high maximum
            search_start = last_swing_low_idx
            search_end   = i
            if search_end > search_start:
                segment   = ph_arr[search_start:search_end]
                local_idx = int(np.argmax(segment))
                ob_idx    = search_start + local_idx
                ob_high   = ph_arr[ob_idx]
                ob_low    = pl_arr[ob_idx]

                order_blocks.append({
                    'type'       : 'Bearish',
                    'structure'  : structure_type,
                    'ob_high'    : ob_high,
                    'ob_low'     : ob_low,
                    'ob_idx'     : ob_idx,
                    'active'     : True,
                    'label'      : label
                })

    # --- Mitigasi OB (mirip deleteOrderBlocks LuxAlgo: High/Low mode) ---
    # Bullish OB mitigated jika low < ob_low
    # Bearish OB mitigated jika high > ob_high
    # Cek dari bar setelah OB terbentuk hingga bar terakhir
    for ob in order_blocks:
        start = ob['ob_idx'] + 1
        for j in range(start, n):
            if ob['type'] == 'Bullish' and lows[j] < ob['ob_low']:
                ob['active'] = False
                break
            if ob['type'] == 'Bearish' and highs[j] > ob['ob_high']:
                ob['active'] = False
                break

    return order_blocks


# ==========================================
# SMC STEP 4: Fair Value Gap (FVG)
# LuxAlgo: bullishFVG = low[0] > high[2] dan close[1] > high[2]
# ==========================================
def detect_fvg(df):
    """
    Mengembalikan status FVG terakhir yang masih aktif (belum dimitigasi)
    Bullish FVG : low[bar]  > high[bar-2] (gap naik)
    Bearish FVG : high[bar] < low[bar-2]  (gap turun)
    Mitigasi Bullish  : low < fvg_bottom
    Mitigasi Bearish  : high > fvg_top
    """
    closes = df['Close'].values
    highs  = df['High'].values
    lows   = df['Low'].values
    opens  = df['Open'].values
    n      = len(df)

    fvg_list = []

    for i in range(2, n):
        bar_delta_pct = (closes[i-1] - opens[i-1]) / (opens[i-1] + 1e-9) * 100

        # Bullish FVG
        if lows[i] > highs[i-2] and closes[i-1] > highs[i-2]:
            fvg_list.append({
                'type'   : 'Bullish',
                'top'    : lows[i],
                'bottom' : highs[i-2],
                'bar_idx': i,
                'active' : True
            })

        # Bearish FVG
        if highs[i] < lows[i-2] and closes[i-1] < lows[i-2]:
            fvg_list.append({
                'type'   : 'Bearish',
                'top'    : lows[i-2],
                'bottom' : highs[i],
                'bar_idx': i,
                'active' : True
            })

    # Mitigasi
    for fvg in fvg_list:
        start = fvg['bar_idx'] + 1
        for j in range(start, n):
            if fvg['type'] == 'Bullish' and lows[j] < fvg['bottom']:
                fvg['active'] = False
                break
            if fvg['type'] == 'Bearish' and highs[j] > fvg['top']:
                fvg['active'] = False
                break

    return fvg_list


# ==========================================
# FUNGSI UTAMA STATUS OB TERHADAP HARGA
# ==========================================
def get_ob_touch_status(price, active_obs):
    """
    Cek apakah harga saat ini menyentuh / berada di dalam OB aktif.
    Prioritas: harga di dalam OB > harga baru bounce dari OB (dalam 3%)
    """
    best_bull_internal = None
    best_bull_swing    = None
    best_bear_internal = None
    best_bear_swing    = None

    for ob in active_obs:
        if not ob['active']:
            continue

        if ob['type'] == 'Bullish':
            # Harga di dalam OB
            inside = ob['ob_low'] <= price <= ob['ob_high']
            # Harga baru bounce (dalam 3% di atas OB)
            near   = ob['ob_high'] < price <= ob['ob_high'] * 1.03

            if inside or near:
                if ob['label'] == 'Internal':
                    if best_bull_internal is None:
                        best_bull_internal = (ob, inside)
                else:
                    if best_bull_swing is None:
                        best_bull_swing = (ob, inside)

        elif ob['type'] == 'Bearish':
            inside = ob['ob_low'] <= price <= ob['ob_high']
            near   = ob['ob_low'] * 0.97 <= price < ob['ob_low']

            if inside or near:
                if ob['label'] == 'Internal':
                    if best_bear_internal is None:
                        best_bear_internal = (ob, inside)
                else:
                    if best_bear_swing is None:
                        best_bear_swing = (ob, inside)

    return best_bull_internal, best_bull_swing, best_bear_internal, best_bear_swing


# ==========================================
# ANALYZE FUNCTION
# ==========================================
def analyze_sector(sector_name, ticker_list):
    tz_jkt       = pytz.timezone("Asia/Jakarta")
    waktu_update = datetime.now(tz_jkt).strftime("%Y-%m-%d %H:%M:%S")

    results = []
    print(f"\n🚀 Scan {sector_name} | Total: {len(ticker_list)} saham")

    for ticker in ticker_list:
        try:
            # Download data harian — lebih panjang agar swing 50 bisa berjalan
            df = yf.download(
                ticker, period="2y", interval="1d",
                progress=False, auto_adjust=True, threads=False
            )

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            if df.empty or len(df) < 120:
                continue

            df = df.copy()
            df.reset_index(inplace=True)

            # ============================================
            # 1. KELTNER CHANNEL
            # ============================================
            df['EMA_20'] = df['Close'].ewm(span=20, adjust=False).mean()

            hl  = df['High'] - df['Low']
            hc  = (df['High'] - df['Close'].shift(1)).abs()
            lc  = (df['Low']  - df['Close'].shift(1)).abs()
            tr  = pd.concat([hl, hc, lc], axis=1).max(axis=1)
            df['ATR_10']    = tr.ewm(alpha=1/10, adjust=False).mean()
            df['KCUe_20_2'] = df['EMA_20'] + (2.0 * df['ATR_10'])
            df['KCLe_20_2'] = df['EMA_20'] - (2.0 * df['ATR_10'])
            df['KCMa_20_2'] = df['EMA_20']

            # ============================================
            # 2. VWAP BANDS (WEEKLY)
            # ============================================
            if df['Date'].dtype != 'datetime64[ns]':
                df['Date'] = pd.to_datetime(df['Date'])
            if hasattr(df['Date'].iloc[0], 'tzinfo') and df['Date'].iloc[0].tzinfo is not None:
                df['Date'] = df['Date'].dt.tz_localize(None)

            df['Week']  = df['Date'].dt.to_period('W')
            df['TP']    = (df['High'] + df['Low'] + df['Close']) / 3
            df['TPV']   = df['TP'] * df['Volume']

            df['Cum_TPV']       = df.groupby('Week')['TPV'].cumsum()
            df['Cum_Vol']       = df.groupby('Week')['Volume'].cumsum()
            df['VWAP']          = df['Cum_TPV'] / df['Cum_Vol']
            df['Dev']           = df['TP'] - df['VWAP']
            df['Dev_Sq_Vol']    = (df['Dev'] ** 2) * df['Volume']
            df['Cum_Dev_Sq_Vol']= df.groupby('Week')['Dev_Sq_Vol'].cumsum()
            df['VWAP_Stdev']    = np.sqrt(df['Cum_Dev_Sq_Vol'] / df['Cum_Vol'])
            df['VWAP_Upper']    = df['VWAP'] + (2.0 * df['VWAP_Stdev'])
            df['VWAP_Lower']    = df['VWAP'] - (2.0 * df['VWAP_Stdev'])

            # ============================================
            # 3. HEIKIN ASHI
            # ============================================
            df['HA_Close'] = (df['Open'] + df['High'] + df['Low'] + df['Close']) / 4
            ha_open        = np.zeros(len(df))
            ha_open[0]     = (df['Open'].iloc[0] + df['Close'].iloc[0]) / 2
            for i in range(1, len(df)):
                ha_open[i] = (ha_open[i-1] + df['HA_Close'].iloc[i-1]) / 2
            df['HA_Open']  = ha_open
            ha_status      = "🟢 BULL" if df['HA_Close'].iloc[-1] > df['HA_Open'].iloc[-1] else "🔴 BEAR"

            # ============================================
            # 4. UT BOT
            # ============================================
            df['nLoss']    = 1.0 * df['ATR_10']
            trail_stop     = np.zeros(len(df))
            trend_ut       = np.zeros(len(df))
            closes_arr     = df['Close'].values
            nlosses_arr    = df['nLoss'].values
            trail_stop[0]  = closes_arr[0]
            trend_ut[0]    = 1

            for i in range(1, len(df)):
                if np.isnan(nlosses_arr[i]):
                    trail_stop[i] = closes_arr[i]; trend_ut[i] = 1; continue
                prev_t = trail_stop[i-1]
                prev_d = trend_ut[i-1]
                cc     = closes_arr[i]
                nl     = nlosses_arr[i]
                if prev_d == 1:
                    trail_stop[i] = max(prev_t, cc - nl) if cc > prev_t else cc + nl
                    trend_ut[i]   = 1 if cc > prev_t else -1
                else:
                    trail_stop[i] = min(prev_t, cc + nl) if cc < prev_t else cc - nl
                    trend_ut[i]   = -1 if cc < prev_t else 1

            t_now, t_prev = trend_ut[-1], trend_ut[-2]
            if   t_now ==  1 and t_prev == -1: ut_signal = "🟢 BUY"
            elif t_now == -1 and t_prev ==  1: ut_signal = "🔴 SELL"
            elif t_now ==  1:                  ut_signal = "🔼 Hold BUY"
            else:                              ut_signal = "🔽 Hold SELL"

            # ============================================
            # 5. SMC — ATR 200 untuk filter OB
            # ============================================
            atr_200 = calc_atr(df, OB_FILTER_ATR_PERIOD)

            parsed_high, parsed_low = get_parsed_hl(df, atr_200)

            # --- Swing Points Internal (length=5) ---
            sh_internal, sl_internal = get_swing_points(df, INTERNAL_SWING_LENGTH)

            # --- Swing Points Swing (length=50) ---
            sh_swing, sl_swing = get_swing_points(df, SWING_LENGTH)

            # --- Order Blocks Internal ---
            ob_internal = detect_structure_and_ob(
                df, parsed_high, parsed_low,
                sh_internal, sl_internal, atr_200, label="Internal"
            )

            # --- Order Blocks Swing ---
            ob_swing = detect_structure_and_ob(
                df, parsed_high, parsed_low,
                sh_swing, sl_swing, atr_200, label="Swing"
            )

            all_obs = ob_internal + ob_swing

            # --- Fair Value Gap ---
            fvg_list = detect_fvg(df)

            # ============================================
            # 6. EKSTRAKSI HARGA & SCORING
            # ============================================
            price_today      = float(df["Close"].iloc[-1])
            upper_kc         = float(df['KCUe_20_2'].iloc[-1])
            middle_kc        = float(df['KCMa_20_2'].iloc[-1])
            lower_kc         = float(df['KCLe_20_2'].iloc[-1])
            vwap_today       = float(df['VWAP'].iloc[-1])
            vwap_upper_today = float(df['VWAP_Upper'].iloc[-1])
            vwap_lower_today = float(df['VWAP_Lower'].iloc[-1])
            atr_today        = float(df['ATR_10'].iloc[-1])
            atr_pct          = (atr_today / price_today) * 100
            potensi_tp_pct   = ((middle_kc - price_today) / price_today) * 100
            target_tp_price  = middle_kc

            # ============================================
            # 7. STATUS SMC — OB TOUCH DETECTION
            # ============================================
            active_obs       = [o for o in all_obs if o['active']]
            bull_int, bull_sw, bear_int, bear_sw = get_ob_touch_status(price_today, active_obs)

            # Kumpulkan semua OB aktif (untuk info kolom)
            all_active_bull_int = [o for o in ob_internal if o['active'] and o['type']=='Bullish']
            all_active_bull_sw  = [o for o in ob_swing   if o['active'] and o['type']=='Bullish']
            all_active_bear_int = [o for o in ob_internal if o['active'] and o['type']=='Bearish']
            all_active_bear_sw  = [o for o in ob_swing   if o['active'] and o['type']=='Bearish']

            # OB terdekat (yang paling baru / paling atas index)
            def latest_ob(obs_list):
                if not obs_list: return None
                return max(obs_list, key=lambda x: x['ob_idx'])

            nearest_bull_int = latest_ob(all_active_bull_int)
            nearest_bull_sw  = latest_ob(all_active_bull_sw)
            nearest_bear_int = latest_ob(all_active_bear_int)
            nearest_bear_sw  = latest_ob(all_active_bear_sw)

            # --- Status OB String ---
            smc_bull_int_status = "⚪ Tidak Ada"
            smc_bull_sw_status  = "⚪ Tidak Ada"
            smc_bear_int_status = "⚪ Tidak Ada"
            smc_bear_sw_status  = "⚪ Tidak Ada"

            ob_touch_score = 0
            ob_touch_label = ""

            # Bullish Internal OB
            if bull_int:
                ob, inside = bull_int
                if inside:
                    smc_bull_int_status = f"🎯 DI DALAM [{ob['structure']}]"
                    ob_touch_score += 100
                    ob_touch_label  = f"🎯 Dalam Bullish Internal OB ({ob['structure']})"
                else:
                    smc_bull_int_status = f"🚀 BOUNCE [{ob['structure']}]"
                    ob_touch_score += 60
                    ob_touch_label  = f"🚀 Bounce Bullish Internal OB ({ob['structure']})"
            elif nearest_bull_int:
                ob = nearest_bull_int
                smc_bull_int_status = f"🟡 Ada OB [{ob['structure']}]"

            # Bullish Swing OB
            if bull_sw:
                ob, inside = bull_sw
                if inside:
                    smc_bull_sw_status = f"🎯 DI DALAM [{ob['structure']}]"
                    ob_touch_score += 80
                    if not ob_touch_label:
                        ob_touch_label = f"🎯 Dalam Bullish Swing OB ({ob['structure']})"
                else:
                    smc_bull_sw_status = f"🚀 BOUNCE [{ob['structure']}]"
                    ob_touch_score += 50
                    if not ob_touch_label:
                        ob_touch_label = f"🚀 Bounce Bullish Swing OB ({ob['structure']})"
            elif nearest_bull_sw:
                ob = nearest_bull_sw
                smc_bull_sw_status = f"🟡 Ada OB [{ob['structure']}]"

            # Bearish Internal OB
            if bear_int:
                ob, inside = bear_int
                if inside:
                    smc_bear_int_status = f"⚠️ DI DALAM [{ob['structure']}] (Resistensi)"
                    ob_touch_score -= 50
                else:
                    smc_bear_int_status = f"⚠️ DEKAT [{ob['structure']}] (Resistensi)"
                    ob_touch_score -= 30
            elif nearest_bear_int:
                ob = nearest_bear_int
                smc_bear_int_status = f"🟡 Ada Bearish OB [{ob['structure']}]"

            # Bearish Swing OB
            if bear_sw:
                ob, inside = bear_sw
                if inside:
                    smc_bear_sw_status = f"⚠️ DI DALAM [{ob['structure']}] (Resistensi)"
                    ob_touch_score -= 40
                else:
                    smc_bear_sw_status = f"⚠️ DEKAT [{ob['structure']}] (Resistensi)"
                    ob_touch_score -= 20
            elif nearest_bear_sw:
                ob = nearest_bear_sw
                smc_bear_sw_status = f"🟡 Ada Bearish OB [{ob['structure']}]"

            # --- FVG Status ---
            active_fvg      = [f for f in fvg_list if f['active']]
            bull_fvg_active = [f for f in active_fvg if f['type'] == 'Bullish']
            bear_fvg_active = [f for f in active_fvg if f['type'] == 'Bearish']

            fvg_status = "⚪ Tidak Ada FVG"
            for fvg in bull_fvg_active[-3:]:  # cek 3 FVG bullish terakhir
                if fvg['bottom'] <= price_today <= fvg['top']:
                    fvg_status = "🟢 Di Dalam Bullish FVG"
                    ob_touch_score += 20
                    break
            else:
                for fvg in bear_fvg_active[-3:]:
                    if fvg['bottom'] <= price_today <= fvg['top']:
                        fvg_status = "🔴 Di Dalam Bearish FVG"
                        ob_touch_score -= 20
                        break

            # ============================================
            # 8. SCORING KELTNER + VWAP + RUBBER BAND
            # ============================================
            score      = ob_touch_score
            kc_status  = "⚪ INSIDE KC"
            vwap_status= "⚪ NORMAL"
            action     = "WAIT"

            # Keltner
            for i in range(1, 5):
                try:
                    p_close = float(df["Close"].iloc[-i])
                    p_upper = float(df['KCUe_20_2'].iloc[-i])
                    p_lower = float(df['KCLe_20_2'].iloc[-i])
                    hari_teks = "Hari Ini" if i == 1 else f"{i-1}H Lalu"
                    if p_close > p_upper:
                        kc_status = f"🔥 KC BREAKOUT ATAS ({hari_teks})"
                        score -= 50; break
                    elif p_close < p_lower:
                        kc_status = f"📉 KC BREAKOUT BAWAH ({hari_teks})"
                        score += 30; break
                except Exception:
                    continue

            # VWAP + Rubber Band
            is_deep_oversold = (price_today < lower_kc) and (price_today < vwap_lower_today)

            if price_today > vwap_upper_today:
                vwap_status = "🔥 OVERVALUED"
                score -= 50
            elif is_deep_oversold:
                vwap_status = "🧊 DEEP OVERSOLD"
                if atr_pct >= 3.0 and potensi_tp_pct >= 10.0:
                    score += 100
                else:
                    score += 40
            elif price_today < vwap_lower_today:
                vwap_status = "🧊 UNDERVALUED"
                score += 20

            # ============================================
            # 9. ACTION — Prioritas: Harga Menyentuh Bullish OB
            # ============================================
            if ob_touch_label:
                # Ada sentuhan Bullish OB
                if "BULL" in ha_status:
                    if "BUY" in ut_signal or "Hold BUY" in ut_signal:
                        action = f"🟢 BUY KUAT: {ob_touch_label} + HA✅ + UT✅"
                        score += 50
                    else:
                        action = f"🟡 BUY SIAP: {ob_touch_label} + HA✅ (Tunggu UT)"
                        score += 20
                else:
                    action = f"⏳ WAIT: {ob_touch_label} (Tunggu HA Hijau)"
                    score -= 10
            elif ob_touch_score < 0:
                action = "🛑 HINDARI (Di Area Bearish OB)"
            elif score > 120:
                action = "🔍 PANTAU KETAT (Oversold + OB Dekat)"
            elif vwap_status == "🔥 OVERVALUED":
                action = "🛑 JANGAN BELI (Pucuk)"
            else:
                action = "⏳ WAIT"

            # --- Info OB Terdekat untuk kolom referensi ---
            def fmt_ob(ob):
                if ob is None: return "-"
                return f"{int(ob['ob_low'])}-{int(ob['ob_high'])} [{ob['structure']}]"

            results.append({
                "Ticker"              : ticker,
                "Action"              : action,
                "Score"               : score,
                "Harga Skrg"          : int(price_today),
                "Target TP (EMA20)"   : int(target_tp_price),
                "Potensi TP (%)"      : round(potensi_tp_pct, 2),
                "ATR (%)"             : round(atr_pct, 2),
                # --- SMC OB Internal ---
                "Bull Internal OB"    : smc_bull_int_status,
                "Bull Int OB Range"   : fmt_ob(nearest_bull_int),
                "Bear Internal OB"    : smc_bear_int_status,
                "Bear Int OB Range"   : fmt_ob(nearest_bear_int),
                # --- SMC OB Swing ---
                "Bull Swing OB"       : smc_bull_sw_status,
                "Bull Sw OB Range"    : fmt_ob(nearest_bull_sw),
                "Bear Swing OB"       : smc_bear_sw_status,
                "Bear Sw OB Range"    : fmt_ob(nearest_bear_sw),
                # --- FVG ---
                "FVG Status"          : fvg_status,
                # --- Indikator Lain ---
                "Status Keltner"      : kc_status,
                "Status VWAP"         : vwap_status,
                "Heikin Ashi"         : ha_status,
                "UT Bot"              : ut_signal,
                "Last Update"         : waktu_update
            })

        except Exception as e:
            print(f"  -> ❌ Gagal untuk {ticker}: {e}")

    df_result = pd.DataFrame(results)

    desired_order = [
        "Ticker", "Action", "Score",
        "Harga Skrg", "Target TP (EMA20)", "Potensi TP (%)", "ATR (%)",
        "Bull Internal OB", "Bull Int OB Range",
        "Bear Internal OB", "Bear Int OB Range",
        "Bull Swing OB",    "Bull Sw OB Range",
        "Bear Swing OB",    "Bear Sw OB Range",
        "FVG Status",
        "Status Keltner", "Status VWAP",
        "Heikin Ashi", "UT Bot", "Last Update"
    ]

    if not df_result.empty:
        available_cols = [c for c in desired_order if c in df_result.columns]
        df_result = df_result[available_cols]
        df_result = df_result.sort_values(by="Score", ascending=False)

    return df_result


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
# MAIN
# ==========================================
if __name__ == "__main__":
    print("🤖 START MARKET SCANNER PRO")
    print("   SMC LuxAlgo Style: OB Internal & Swing | BOS/CHoCH | FVG")
    print("   Timeframe: Harian (1d) | Tujuan: Deteksi Bullish OB Touch")
    print("=" * 65)

    for sheet_name, saham_list in SECTOR_CONFIG.items():
        df_final = analyze_sector(sheet_name, saham_list)

        if df_final.empty:
            print(f"⚠️  Tidak ada data valid untuk {sheet_name}")
            continue

        ws = connect_gsheet(sheet_name)
        if ws:
            try:
                ws.clear()
                set_with_dataframe(ws, df_final)
                print(f"✅ {sheet_name} — {len(df_final)} emiten tersimpan.")
            except Exception as e:
                print(f"❌ Upload Error di {sheet_name}: {e}")

        time.sleep(2)   # Hindari rate limit yfinance / Google Sheets

    print("\n🏁 SELESAI 🏁")
