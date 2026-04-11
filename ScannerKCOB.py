# ==========================================
# MARKET SCANNER - PRO
# Keltner + VWAP + Rubber Band
# + SMC LuxAlgo Style (OB Internal & Swing, BOS/CHoCH, FVG)
# Timeframe: Harian (1d)
# Tujuan : Deteksi saham yang menyentuh area Bullish Order Block
# TP     : ob_low Bearish OB aktif terdekat di atas harga
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
            # 3. KONFIRMASI CANDLE
            # Cek apakah 1-2 candle terakhir bullish (close > open)
            # ============================================
            c_close1 = float(df['Close'].iloc[-1])
            c_open1  = float(df['Open'].iloc[-1])
            c_close2 = float(df['Close'].iloc[-2])
            c_open2  = float(df['Open'].iloc[-2])

            candle1_bull = c_close1 > c_open1
            candle2_bull = c_close2 > c_open2

            if candle1_bull and candle2_bull:
                candle_status = "🟢 2 Candle Bullish"
            elif candle1_bull:
                candle_status = "🟡 Candle Hari Ini Bullish"
            elif candle2_bull:
                candle_status = "🟡 Candle Kemarin Bullish"
            else:
                candle_status = "🔴 2 Candle Bearish"

            # ============================================
            # 4. SMC — ATR 200 untuk filter OB
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
            # 5. EKSTRAKSI HARGA
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

            # ============================================
            # 6. STATUS SMC — OB TOUCH DETECTION & TP
            # ============================================
            active_obs       = [o for o in all_obs if o['active']]
            bull_int, bull_sw, bear_int, bear_sw = get_ob_touch_status(price_today, active_obs)

            # Kumpulkan semua OB aktif
            all_active_bull_int = [o for o in ob_internal if o['active'] and o['type']=='Bullish']
            all_active_bull_sw  = [o for o in ob_swing   if o['active'] and o['type']=='Bullish']
            all_active_bear_int = [o for o in ob_internal if o['active'] and o['type']=='Bearish']
            all_active_bear_sw  = [o for o in ob_swing   if o['active'] and o['type']=='Bearish']

            # OB paling baru (latest bar index)
            def latest_ob(obs_list):
                if not obs_list: return None
                return max(obs_list, key=lambda x: x['ob_idx'])

            nearest_bull_int = latest_ob(all_active_bull_int)
            nearest_bull_sw  = latest_ob(all_active_bull_sw)
            nearest_bear_int = latest_ob(all_active_bear_int)
            nearest_bear_sw  = latest_ob(all_active_bear_sw)

            # ============================================
            # TP = ob_low Bearish OB aktif TERDEKAT di atas harga
            # Prioritas: Internal OB dulu, lalu Swing OB
            # Fallback: EMA20 jika tidak ada Bearish OB di atas harga
            # ============================================
            # Kumpulkan semua Bearish OB aktif yang ob_low-nya di atas harga saat ini
            bear_obs_above = [
                o for o in (all_active_bear_int + all_active_bear_sw)
                if o['ob_low'] > price_today
            ]

            if bear_obs_above:
                # Terdekat = ob_low terkecil yang masih di atas harga
                nearest_bear_tp = min(bear_obs_above, key=lambda x: x['ob_low'])
                target_tp_price = nearest_bear_tp['ob_low']
                tp_source       = f"Bear {nearest_bear_tp['label']} OB [{nearest_bear_tp['structure']}]"
            else:
                # Fallback ke EMA20
                nearest_bear_tp = None
                target_tp_price = float(df['KCMa_20_2'].iloc[-1])
                tp_source       = "EMA20 (fallback)"

            potensi_tp_pct = ((target_tp_price - price_today) / price_today) * 100

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
            # 7. SCORING KELTNER + VWAP + RUBBER BAND
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
            # 8. ACTION — Prioritas: Harga Menyentuh Bullish OB
            # Konfirmasi: minimal 1 candle bullish terakhir
            # ============================================
            if ob_touch_label:
                if candle1_bull or candle2_bull:
                    action = f"🟢 BUY: {ob_touch_label}"
                    score += 50
                else:
                    action = f"⏳ WAIT: {ob_touch_label} (Tunggu Candle Bullish)"
                    score -= 10
            elif ob_touch_score < 0:
                action = "🛑 HINDARI (Di Area Bearish OB)"
            elif score > 120:
                action = "🔍 PANTAU KETAT (Oversold)"
            elif vwap_status == "🔥 OVERVALUED":
                action = "🛑 JANGAN BELI (Pucuk)"
            else:
                action = "⏳ WAIT"

            # --- Format OB range untuk kolom ---
            def fmt_ob(ob):
                if ob is None: return "-"
                return f"{int(ob['ob_low'])}-{int(ob['ob_high'])} [{ob['structure']}]"

            results.append({
                "Ticker"            : ticker,
                "Action"            : action,
                "Score"             : score,
                "Harga Skrg"        : int(price_today),
                "Target TP"         : int(target_tp_price),
                "TP Source"         : tp_source,
                "Potensi TP (%)"    : round(potensi_tp_pct, 2),
                "ATR (%)"           : round(atr_pct, 2),
                # --- Konfirmasi Candle ---
                "Candle"            : candle_status,
                # --- SMC OB Internal ---
                "Bull Internal OB"  : smc_bull_int_status,
                "Bull Int OB Range" : fmt_ob(nearest_bull_int),
                "Bear Internal OB"  : smc_bear_int_status,
                "Bear Int OB Range" : fmt_ob(nearest_bear_int),
                # --- SMC OB Swing ---
                "Bull Swing OB"     : smc_bull_sw_status,
                "Bull Sw OB Range"  : fmt_ob(nearest_bull_sw),
                "Bear Swing OB"     : smc_bear_sw_status,
                "Bear Sw OB Range"  : fmt_ob(nearest_bear_sw),
                # --- FVG ---
                "FVG Status"        : fvg_status,
                # --- Keltner & VWAP ---
                "Status Keltner"    : kc_status,
                "Status VWAP"       : vwap_status,
                "Last Update"       : waktu_update
            })

        except Exception as e:
            print(f"  -> ❌ Gagal untuk {ticker}: {e}")

    df_result = pd.DataFrame(results)

    desired_order = [
        "Ticker", "Action", "Score",
        "Harga Skrg", "Target TP", "TP Source", "Potensi TP (%)", "ATR (%)",
        "Candle",
        "Bull Internal OB", "Bull Int OB Range",
        "Bear Internal OB", "Bear Int OB Range",
        "Bull Swing OB",    "Bull Sw OB Range",
        "Bear Swing OB",    "Bear Sw OB Range",
        "FVG Status",
        "Status Keltner", "Status VWAP",
        "Last Update"
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
        "AMFG.JK","AMIN.JK","APII.JK","ARKA.JK","ARNA.JK","ASGR.JK"
    ],
    "IDXNONCYC": [
        "AALI.JK","ADES.JK","AGAR.JK","AISA.JK","ALTO.JK","AMMS.JK"
    ],
    "IDXFINANCE": [
        "ABDA.JK","ADMF.JK","AGRO.JK","AGRS.JK","AHAP.JK","AMAG.JK"
    ],
    "IDXCYCLIC": [
        "ABBA.JK","ACES.JK","ACRO.JK","AEGS.JK","AKKU.JK","ARGO.JK"
    ],
    "IDXTECHNO": [
        "AREA.JK","ATIC.JK","AWAN.JK","AXIO.JK","BELI.JK","BUKA.JK"
    ],
    "IDXBASIC": [
        "ADMG.JK","AGII.JK","AKPI.JK","ALDO.JK","ALKA.JK","ALMI.JK"
    ],
    "IDXENERGY": [
        "AADI.JK","ABMM.JK","ADMR.JK","ADRO.JK","AIMS.JK","AKRA.JK"
    ],
    "IDXHEALTH": [
        "BMHS.JK","CARE.JK","CHEK.JK","DGNS.JK","DKHH.JK","DVLA.JK"
    ],
    "IDXINFRA": [
        "ACST.JK","ADHI.JK","ARKO.JK","ASLI.JK","BALI.JK","BDKR.JK"
    ],
    "IDXPROPERT": [
        "ADCP.JK","AMAN.JK","APLN.JK","ARMY.JK","ASPI.JK","ASRI.JK"
    ],
    "IDXTRANS": [
        "AKSI.JK","ASSA.JK","BIRD.JK","BLOG.JK","BLTA.JK","BPTR.JK"
    ]
}


# ==========================================
# MAIN
# ==========================================
if __name__ == "__main__":
    print("🤖 START MARKET SCANNER PRO")
    print("   SMC LuxAlgo Style: OB Internal & Swing | BOS/CHoCH | FVG")
    print("   Timeframe : Harian (1d)")
    print("   Entry     : Harga menyentuh Bullish OB")
    print("   TP        : ob_low Bearish OB aktif terdekat di atas harga")
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
