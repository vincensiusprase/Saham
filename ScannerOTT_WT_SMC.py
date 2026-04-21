# ==========================================
# MARKET SCANNER - ULTIMATE PRO MAX
# OTT + WaveTrend (Age/Cross Logic) + SMC 
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

SPREADSHEET_ID = "1RppJjEjmwBr3eXh_Bs54Wbs2EAIRWuOlsw8ELD8uUfY"

# ==========================================
# PARAMETER
# ==========================================
# OTT
OTT_PERIOD  = 2
OTT_PERCENT = 1.4

# WaveTrend
WT_N1 = 10
WT_N2 = 21

# SMC (LuxAlgo Style)
INTERNAL_SWING_LENGTH = 5    
SWING_LENGTH          = 50   
OB_FILTER_ATR_PERIOD  = 200  

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
            worksheet = sh.add_worksheet(title=target_sheet_name, rows="300", cols="25")
        return worksheet
    except Exception as e:
        print(f"❌ Error Koneksi GSheet: {e}")
        return None

# ==========================================
# INDICATOR FUNCTIONS (OTT & WT)
# ==========================================
def calculate_ott(df, length=2, percent=1.4):
    src = df['Close'].values
    n = len(src)
    
    valpha = 2 / (length + 1)
    change = np.diff(src, prepend=src[0])
    vud1 = np.where(change > 0, change, 0)
    vdd1 = np.where(change < 0, -change, 0)
    
    vUD = pd.Series(vud1).rolling(window=9, min_periods=1).sum().values
    vDD = pd.Series(vdd1).rolling(window=9, min_periods=1).sum().values
    
    denominator = vUD + vDD
    vCMO = np.where(denominator != 0, (vUD - vDD) / denominator, 0)
    
    VAR = np.zeros(n)
    VAR[0] = src[0]
    for i in range(1, n):
        VAR[i] = (valpha * abs(vCMO[i]) * src[i]) + (1 - valpha * abs(vCMO[i])) * VAR[i-1]
        
    longStop = np.zeros(n)
    shortStop = np.zeros(n)
    direction = np.ones(n)
    MT = np.zeros(n)
    
    for i in range(1, n):
        fark = VAR[i] * percent * 0.01
        ls = VAR[i] - fark
        ss = VAR[i] + fark
        
        longStop[i]  = max(ls, longStop[i-1]) if VAR[i] > longStop[i-1] else ls
        shortStop[i] = min(ss, shortStop[i-1]) if VAR[i] < shortStop[i-1] else ss
        
        prev_dir = direction[i-1]
        if prev_dir == -1 and VAR[i] > shortStop[i-1]:
            curr_dir = 1
        elif prev_dir == 1 and VAR[i] < longStop[i-1]:
            curr_dir = -1
        else:
            curr_dir = prev_dir
            
        direction[i] = curr_dir
        MT[i] = longStop[i] if curr_dir == 1 else shortStop[i]
        
    OTT_base = np.where(VAR > MT, MT * (200 + percent) / 200, MT * (200 - percent) / 200)
    
    df['VAR'] = VAR
    df['OTT'] = pd.Series(OTT_base).shift(2).values 
    
    cross_signal = np.zeros(n)
    for i in range(1, n):
        if VAR[i-1] <= df['OTT'].iloc[i-1] and VAR[i] > df['OTT'].iloc[i]:
            cross_signal[i] = 1 
        elif VAR[i-1] >= df['OTT'].iloc[i-1] and VAR[i] < df['OTT'].iloc[i]:
            cross_signal[i] = -1 
            
    df['OTT_Cross'] = cross_signal
    return df

def calculate_wavetrend(df, n1=10, n2=21):
    ap = (df['High'] + df['Low'] + df['Close']) / 3
    esa = ap.ewm(span=n1, adjust=False).mean()
    d = (ap - esa).abs().ewm(span=n1, adjust=False).mean()
    ci = np.where(d != 0, (ap - esa) / (0.015 * d), 0)
    ci_series = pd.Series(ci, index=df.index)
    
    wt1 = ci_series.ewm(span=n2, adjust=False).mean()
    wt2 = wt1.rolling(window=4).mean()
    
    df['WT1'] = wt1
    df['WT2'] = wt2
    
    # Deteksi WT Cross Historis (1 untuk Bullish, -1 untuk Bearish)
    n = len(df)
    wt_cross_signal = np.zeros(n)
    
    wt1_arr = wt1.values
    wt2_arr = wt2.values
    
    for i in range(1, n):
        if wt1_arr[i-1] <= wt2_arr[i-1] and wt1_arr[i] > wt2_arr[i]:
            wt_cross_signal[i] = 1 # Golden Cross
        elif wt1_arr[i-1] >= wt2_arr[i-1] and wt1_arr[i] < wt2_arr[i]:
            wt_cross_signal[i] = -1 # Dead Cross
            
    df['WT_Cross_Signal'] = wt_cross_signal
    
    return df

# ==========================================
# SMC FUNCTIONS (LuxAlgo Logic)
# ==========================================
def calc_atr(df, period):
    hl = df['High'] - df['Low']
    hc = (df['High'] - df['Close'].shift(1)).abs()
    lc = (df['Low'] - df['Close'].shift(1)).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    return tr.rolling(window=period, min_periods=1).mean()

def get_parsed_hl(df, atr_200):
    high_vol = (df['High'] - df['Low']) >= (2.0 * atr_200)
    parsed_high = np.where(high_vol, df['Low'], df['High'])
    parsed_low  = np.where(high_vol, df['High'], df['Low'])
    return pd.Series(parsed_high, index=df.index), pd.Series(parsed_low, index=df.index)

def get_swing_points(df, length):
    win = 2 * length + 1
    roll_max = df['High'].rolling(window=win, center=True).max()
    roll_min = df['Low'].rolling(window=win, center=True).min()
    return (df['High'] == roll_max), (df['Low'] == roll_min)

def detect_structure_and_ob(df, parsed_high, parsed_low, swing_high, swing_low):
    closes, highs, lows = df['Close'].values, df['High'].values, df['Low'].values
    ph_arr, pl_arr = parsed_high.values, parsed_low.values
    sh_arr, sl_arr = swing_high.values, swing_low.values
    n = len(df)

    last_sh_price, last_sh_idx = np.nan, -1
    last_sl_price, last_sl_idx = np.nan, -1
    trend_bias = 0
    sh_crossed, sl_crossed = False, False
    order_blocks = []

    for i in range(n):
        if sh_arr[i]:
            last_sh_price, last_sh_idx, sh_crossed = highs[i], i, False
        if sl_arr[i]:
            last_sl_price, last_sl_idx, sl_crossed = lows[i], i, False

        # Bullish OB
        if not np.isnan(last_sh_price) and closes[i] > last_sh_price and not sh_crossed and last_sh_idx >= 0:
            sh_crossed = True
            trend_bias = 1
            if i > last_sh_idx:
                segment = pl_arr[last_sh_idx:i]
                local_idx = int(np.argmin(segment))
                ob_idx = last_sh_idx + local_idx
                order_blocks.append({
                    'type': 'Bullish', 'ob_high': ph_arr[ob_idx], 'ob_low': pl_arr[ob_idx], 
                    'ob_idx': ob_idx, 'active': True
                })

        # Bearish OB
        if not np.isnan(last_sl_price) and closes[i] < last_sl_price and not sl_crossed and last_sl_idx >= 0:
            sl_crossed = True
            trend_bias = -1
            if i > last_sl_idx:
                segment = ph_arr[last_sl_idx:i]
                local_idx = int(np.argmax(segment))
                ob_idx = last_sl_idx + local_idx
                order_blocks.append({
                    'type': 'Bearish', 'ob_high': ph_arr[ob_idx], 'ob_low': pl_arr[ob_idx], 
                    'ob_idx': ob_idx, 'active': True
                })

    for ob in order_blocks:
        start = ob['ob_idx'] + 1
        for j in range(start, n):
            if ob['type'] == 'Bullish' and lows[j] < ob['ob_low']:
                ob['active'] = False; break
            if ob['type'] == 'Bearish' and highs[j] > ob['ob_high']:
                ob['active'] = False; break

    return order_blocks

def detect_fvg(df):
    closes, highs, lows, opens = df['Close'].values, df['High'].values, df['Low'].values, df['Open'].values
    n = len(df)
    fvg_list = []

    for i in range(2, n):
        if lows[i] > highs[i-2] and closes[i-1] > highs[i-2]:
            fvg_list.append({'type': 'Bullish', 'top': lows[i], 'bottom': highs[i-2], 'idx': i, 'active': True})
        if highs[i] < lows[i-2] and closes[i-1] < lows[i-2]:
            fvg_list.append({'type': 'Bearish', 'top': lows[i-2], 'bottom': highs[i], 'idx': i, 'active': True})

    for fvg in fvg_list:
        start = fvg['idx'] + 1
        for j in range(start, n):
            if fvg['type'] == 'Bullish' and lows[j] < fvg['bottom']:
                fvg['active'] = False; break
            if fvg['type'] == 'Bearish' and highs[j] > fvg['top']:
                fvg['active'] = False; break

    return fvg_list

# ==========================================
# MAIN ANALYZER
# ==========================================
def analyze_sector(sector_name, ticker_list):
    tz_jkt = pytz.timezone("Asia/Jakarta")
    waktu_update = datetime.now(tz_jkt).strftime("%Y-%m-%d %H:%M:%S")
    results = []
    print(f"\n🚀 Scan {sector_name} | Total: {len(ticker_list)} saham (TRIAL)")

    for ticker in ticker_list:
        try:
            df = yf.download(ticker, period="1y", interval="1d", progress=False, auto_adjust=True, threads=False)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            if df.empty or len(df) < 100:
                continue

            df.reset_index(inplace=True)

            # 1. Kalkulasi OTT & WT
            df = calculate_ott(df, length=OTT_PERIOD, percent=OTT_PERCENT)
            df = calculate_wavetrend(df, n1=WT_N1, n2=WT_N2)
            
            # 2. Kalkulasi SMC
            atr_200 = calc_atr(df, OB_FILTER_ATR_PERIOD)
            parsed_high, parsed_low = get_parsed_hl(df, atr_200)
            
            sh_int, sl_int = get_swing_points(df, INTERNAL_SWING_LENGTH)
            sh_sw, sl_sw = get_swing_points(df, SWING_LENGTH)
            
            obs_int = detect_structure_and_ob(df, parsed_high, parsed_low, sh_int, sl_int)
            obs_sw  = detect_structure_and_ob(df, parsed_high, parsed_low, sh_sw, sl_sw)
            all_obs = obs_int + obs_sw
            active_obs = [o for o in all_obs if o['active']]
            
            fvg_list = detect_fvg(df)
            active_fvg = [f for f in fvg_list if f['active']]

            # ============================================
            # EKSTRAKSI HARGA & INDIKATOR HARI INI
            # ============================================
            price_today = float(df["Close"].iloc[-1])
            var_today   = float(df['VAR'].iloc[-1])
            ott_today   = float(df['OTT'].iloc[-1])
            wt1_today, wt2_today = float(df['WT1'].iloc[-1]), float(df['WT2'].iloc[-1])
            wt1_prev, wt2_prev   = float(df['WT1'].iloc[-2]), float(df['WT2'].iloc[-2])

            # --- Cari Umur & Jenis Sinyal OTT ---
            lookback = 30
            df_recent = df.iloc[-lookback:]
            
            days_since_ott_cross, ott_cross_type = "Belum Ada", "Tidak Ada"
            for i in range(len(df_recent)-1, -1, -1):
                if df_recent['OTT_Cross'].iloc[i] == 1:
                    jarak = len(df_recent) - 1 - i
                    days_since_ott_cross = f"{jarak} Hari" if jarak > 0 else "HARI INI"
                    ott_cross_type = "VAR Cross Up OTT"
                    break
                elif df_recent['OTT_Cross'].iloc[i] == -1:
                    jarak = len(df_recent) - 1 - i
                    days_since_ott_cross = f"{jarak} Hari" if jarak > 0 else "HARI INI"
                    ott_cross_type = "VAR Cross Down OTT"
                    break

            # --- Cari Umur & Jenis Sinyal WT Cross ---
            days_since_wt_cross, wt_cross_type = "Belum Ada", "Tidak Ada"
            for i in range(len(df_recent)-1, -1, -1):
                if df_recent['WT_Cross_Signal'].iloc[i] == 1:
                    jarak = len(df_recent) - 1 - i
                    days_since_wt_cross = f"{jarak} Hari" if jarak > 0 else "HARI INI"
                    wt_cross_type = "WT Cross UP (Bullish)"
                    break
                elif df_recent['WT_Cross_Signal'].iloc[i] == -1:
                    jarak = len(df_recent) - 1 - i
                    days_since_wt_cross = f"{jarak} Hari" if jarak > 0 else "HARI INI"
                    wt_cross_type = "WT Cross DOWN (Bearish)"
                    break

            # ============================================
            # CEK POSISI HARGA TERHADAP SMC (OB & FVG)
            # ============================================
            smc_status = "⚪ Di Luar Zona"
            smc_score = 0
            
            for ob in active_obs:
                if ob['type'] == 'Bullish' and ob['ob_low'] <= price_today <= ob['ob_high']:
                    smc_status = "🟢 Di Dalam Bullish OB"
                    smc_score += 40
                    break
            
            if smc_score == 0:
                for fvg in active_fvg:
                    if fvg['type'] == 'Bullish' and fvg['bottom'] <= price_today <= fvg['top']:
                        smc_status = "🟢 Di Dalam Bullish FVG"
                        smc_score += 30
                        break
            
            for ob in active_obs:
                if ob['type'] == 'Bearish' and ob['ob_low'] <= price_today <= ob['ob_high']:
                    smc_status = "🔴 Di Dalam Bearish OB (Resistensi)"
                    smc_score -= 40
                    break

            # ============================================
            # SET TARGET TP
            # ============================================
            bear_obs_above = [o for o in active_obs if o['type'] == 'Bearish' and o['ob_low'] > price_today]
            if bear_obs_above:
                nearest_bear_ob = min(bear_obs_above, key=lambda x: x['ob_low'])
                target_tp = nearest_bear_ob['ob_low']
                potensi_tp_pct = ((target_tp - price_today) / price_today) * 100
            else:
                target_tp = 0 
                potensi_tp_pct = 0

            # ============================================
            # SCORING & ACTION (Logika Confluence)
            # ============================================
            score  = smc_score
            action = "WAIT"
            trend  = "UPTREND" if var_today > ott_today else "DOWNTREND"
            wt_status = "⚪ Netral"

            # Deteksi WT Cross Hari Ini (Realtime)
            is_wt_bull_cross = (wt1_prev <= wt2_prev) and (wt1_today > wt2_today)
            is_wt_bear_cross = (wt1_prev >= wt2_prev) and (wt1_today < wt2_today)
            
            if wt1_today < -53:
                wt_status = "🚀 WT GOLDEN CROSS (Oversold)" if is_wt_bull_cross else "🟢 Oversold"
            elif wt1_today > 53:
                wt_status = "⚠️ WT DEAD CROSS (Overbought)" if is_wt_bear_cross else "🔴 Overbought"
            else:
                if is_wt_bull_cross: wt_status = "🟢 WT Cross UP"
                elif is_wt_bear_cross: wt_status = "🔴 WT Cross DOWN"

            # Scoring
            if trend == "UPTREND": score += 50
            elif trend == "DOWNTREND": score -= 50

            if days_since_ott_cross == "HARI INI":
                if ott_cross_type == "VAR Cross Up OTT": score += 50
                elif ott_cross_type == "VAR Cross Down OTT": score -= 50

            if "WT GOLDEN CROSS" in wt_status: score += 40
            elif "WT DEAD CROSS" in wt_status: score -= 40

            # Action Logic (SMC + Trigger)
            if smc_score > 0 and (days_since_ott_cross == "HARI INI" or is_wt_bull_cross):
                action = "🔥 SNIPER BUY (SMC + Trigger)"
            elif days_since_ott_cross == "HARI INI" and ott_cross_type == "VAR Cross Up OTT":
                action = "🟢 BUY (OTT Breakout)"
            elif smc_score > 0 and trend == "UPTREND":
                action = "🟡 AKUMULASI (Di Area Diskon)"
            elif trend == "DOWNTREND" and smc_score < 0:
                action = "🔴 HINDARI (Downtrend & Resistensi)"
            
            tp_text = str(int(target_tp)) if target_tp > 0 else "High Baru"
            potensi_text = f"{round(potensi_tp_pct, 2)}%" if target_tp > 0 else "-"

            results.append({
                "Ticker"             : ticker,
                "Action"             : action,
                "Score"              : score,
                "Trend (OTT)"        : trend,
                "Harga Skrg"         : int(price_today),
                "Status SMC"         : smc_status,
                "Target TP"          : tp_text,
                "Potensi TP"         : potensi_text,
                "Umur OTT Cross"     : days_since_ott_cross,
                "WT Cross Terakhir"  : wt_cross_type,
                "Umur WT Cross"      : days_since_wt_cross,
                "WT Status (Now)"    : wt_status,
                "VAR (MAvg)"         : round(var_today, 2),
                "OTT Line"           : round(ott_today, 2),
                "Last Update"        : waktu_update
            })

        except Exception as e:
            print(f"  -> ❌ Gagal untuk {ticker}: {e}")

    df_result = pd.DataFrame(results)

    desired_order = [
        "Ticker", "Action", "Score", "Trend (OTT)",
        "Harga Skrg", "Status SMC", "Target TP", "Potensi TP", 
        "Umur OTT Cross", "WT Cross Terakhir", "Umur WT Cross", "WT Status (Now)", 
        "VAR (MAvg)", "OTT Line", "Last Update"
    ]

    if not df_result.empty:
        available_cols = [c for c in desired_order if c in df_result.columns]
        df_result = df_result[available_cols]
        df_result = df_result.sort_values(by="Score", ascending=False)

    return df_result

# ==========================================
# SECTOR CONFIG (5 TICKER PER SECTOR FOR TRIAL)
# ==========================================
SECTOR_CONFIG = {    
    "IDXINDUST": ["AMFG.JK", "AMIN.JK", "APII.JK", "ARKA.JK", "ARNA.JK"],
    "IDXNONCYC": ["AALI.JK", "ADES.JK", "AGAR.JK", "AISA.JK", "ALTO.JK"],
    "IDXFINANCE": ["ABDA.JK", "ADMF.JK", "AGRO.JK", "AGRS.JK", "AHAP.JK"],
    "IDXCYCLIC": ["ABBA.JK", "ACES.JK", "ACRO.JK", "AEGS.JK", "AKKU.JK"],
    "IDXTECHNO": ["AREA.JK", "ATIC.JK", "AWAN.JK", "AXIO.JK", "BELI.JK"],
    "IDXBASIC": ["ADMG.JK", "AGII.JK", "AKPI.JK", "ALDO.JK", "ALKA.JK"],
    "IDXENERGY": ["AADI.JK", "ABMM.JK", "ADMR.JK", "ADRO.JK", "AIMS.JK"],
    "IDXHEALTH": ["BMHS.JK", "CARE.JK", "CHEK.JK", "DGNS.JK", "DKHH.JK"],
    "IDXINFRA": ["ACST.JK", "ADHI.JK", "ARKO.JK", "ASLI.JK", "BALI.JK"],
    "IDXPROPERT": ["ADCP.JK", "AMAN.JK", "APLN.JK", "ARMY.JK", "ASPI.JK"],
    "IDXTRANS": ["AKSI.JK", "ASSA.JK", "BIRD.JK", "BLOG.JK", "BLTA.JK"]
}

# ==========================================
# MAIN
# ==========================================
if __name__ == "__main__":
    print("🤖 START MARKET SCANNER ULTIMATE PRO MAX (TRIAL)")
    print("   1. OTT (Trend Follower)")
    print("   2. WaveTrend (Momentum & Age Validation)")
    print("   3. SMC: Filter Area OB/FVG & Dynamic TP")
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

        time.sleep(2)

    print("\n🏁 SELESAI 🏁")
