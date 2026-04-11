# ==========================================
# MARKET SCANNER - PRO (KELTNER + VWAP + HA + UT BOT + RUBBER BAND + SMC ORDER BLOCK)
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

# Ganti dengan Spreadsheet ID Anda
SPREADSHEET_ID = "1QbdNwITMBF0MZXh3ousJ8WwHFYIaAxNxzNPwHOtSXlo"
SWING_LENGTH = 5  # Parameter panjang swing untuk SMC LuxAlgo

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
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SPREADSHEET_ID)

        try:
            worksheet = sh.worksheet(target_sheet_name)
        except:
            worksheet = sh.add_worksheet(title=target_sheet_name, rows="300", cols="15")
        return worksheet
    except Exception as e:
        print(f"❌ Error Koneksi GSheet: {e}")
        return None

# ==========================================
# SMART MONEY CONCEPTS (SMC) FUNCTIONS
# ==========================================
def get_swing_points(df, length):
    """Mencari Swing High dan Swing Low"""
    df['Swing_High'] = df['High'] == df['High'].rolling(window=length*2+1, center=True).max()
    df['Swing_Low'] = df['Low'] == df['Low'].rolling(window=length*2+1, center=True).min()
    
    # Isi forward (ffill) level swing untuk referensi harga
    df['Last_Swing_High_Price'] = np.where(df['Swing_High'], df['High'], np.nan)
    df['Last_Swing_High_Price'] = df['Last_Swing_High_Price'].ffill()
    
    df['Last_Swing_Low_Price'] = np.where(df['Swing_Low'], df['Low'], np.nan)
    df['Last_Swing_Low_Price'] = df['Last_Swing_Low_Price'].ffill()
    
    return df

def find_bullish_order_blocks(df):
    """Mendeteksi Bullish Order Blocks setelah Break of Structure (BOS)"""
    df['Bullish_OB_Top'] = np.nan
    df['Bullish_OB_Bottom'] = np.nan
    
    ob_top = np.nan
    ob_bottom = np.nan
    trend_bullish = False

    for i in range(SWING_LENGTH * 2, len(df)):
        current_close = df['Close'].iloc[i]
        last_swing_high = df['Last_Swing_High_Price'].iloc[i-1]
        last_swing_low = df['Last_Swing_Low_Price'].iloc[i-1]
        
        # 1. Deteksi Bullish BOS
        if pd.notna(last_swing_high) and current_close > last_swing_high and not trend_bullish:
            trend_bullish = True
            
            # 2. Cari Order Block (Candle merah terakhir sebelum pergerakan naik)
            for j in range(i-1, max(0, i-20), -1):
                if df['Close'].iloc[j] < df['Open'].iloc[j]: # Candle Bearish
                    ob_top = df['High'].iloc[j]
                    ob_bottom = df['Low'].iloc[j]
                    break
        
        # 3. Mitigasi / Pembatalan OB (REVISI PENTING)
        # Jika harga turun dan closing menembus batas bawah OB, OB tersebut hangus (invalid)
        if pd.notna(ob_bottom) and current_close < ob_bottom:
            ob_top = np.nan
            ob_bottom = np.nan
            
        # Jika harga turun menembus swing low utama, tren batal
        if pd.notna(last_swing_low) and current_close < last_swing_low:
            trend_bullish = False
            
        # Simpan nilai OB yang aktif
        df.loc[df.index[i], 'Bullish_OB_Top'] = ob_top
        df.loc[df.index[i], 'Bullish_OB_Bottom'] = ob_bottom

    return df

# ==========================================
# ANALYZE FUNCTION
# ==========================================
def analyze_sector(sector_name, ticker_list):

    tz_jkt = pytz.timezone("Asia/Jakarta")
    waktu_update = datetime.now(tz_jkt).strftime("%Y-%m-%d %H:%M:%S")
    
    results = []
    print(f"\n🚀 Scan {sector_name} | Total: {len(ticker_list)} saham")

    for ticker in ticker_list:
        try:
            # Download data (Diperpanjang ke 120d untuk SMC)
            df = yf.download(ticker, period="120d", interval="1d", progress=False, auto_adjust=True, threads=False)

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            # Minimal data 50 hari untuk SMC berjalan lancar
            if df.empty or len(df) < 50: 
                continue

            # ==============================
            # 1. KELTNER CHANNEL
            # ==============================
            df['EMA_20'] = df['Close'].ewm(span=20, adjust=False).mean()
            
            high_low = df['High'] - df['Low']
            high_close = np.abs(df['High'] - df['Close'].shift(1))
            low_close = np.abs(df['Low'] - df['Close'].shift(1))
            true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)

            df['ATR_10'] = true_range.ewm(alpha=1/10, adjust=False).mean()

            df['KCUe_20_2'] = df['EMA_20'] + (2.0 * df['ATR_10'])
            df['KCLe_20_2'] = df['EMA_20'] - (2.0 * df['ATR_10'])
            df['KCMa_20_2'] = df['EMA_20'] 

            # ==============================
            # 2. VWAP BANDS (WEEKLY)
            # ==============================
            if df.index.tz is not None:
                df.index = df.index.tz_localize(None)

            df['Week'] = df.index.to_period('W')
            df['TP'] = (df['High'] + df['Low'] + df['Close']) / 3
            df['TPV'] = df['TP'] * df['Volume']

            df['Cum_TPV'] = df.groupby('Week')['TPV'].cumsum()
            df['Cum_Vol'] = df.groupby('Week')['Volume'].cumsum()
            df['VWAP'] = df['Cum_TPV'] / df['Cum_Vol']

            df['Dev'] = df['TP'] - df['VWAP']
            df['Dev_Sq_Vol'] = (df['Dev'] ** 2) * df['Volume']
            df['Cum_Dev_Sq_Vol'] = df.groupby('Week')['Dev_Sq_Vol'].cumsum()
            df['VWAP_Stdev'] = np.sqrt(df['Cum_Dev_Sq_Vol'] / df['Cum_Vol'])

            df['VWAP_Upper'] = df['VWAP'] + (2.0 * df['VWAP_Stdev'])
            df['VWAP_Lower'] = df['VWAP'] - (2.0 * df['VWAP_Stdev'])

            # ==============================
            # 3. HEIKIN ASHI CANDLES
            # ==============================
            df['HA_Close'] = (df['Open'] + df['High'] + df['Low'] + df['Close']) / 4
            ha_open = np.zeros(len(df))
            ha_open[0] = (df['Open'].iloc[0] + df['Close'].iloc[0]) / 2
            for i in range(1, len(df)):
                ha_open[i] = (ha_open[i-1] + df['HA_Close'].iloc[i-1]) / 2
            df['HA_Open'] = ha_open
            ha_status = "🟢 BULL (Hijau)" if df['HA_Close'].iloc[-1] > df['HA_Open'].iloc[-1] else "🔴 BEAR (Merah)"

            # ==============================
            # 4. UT BOT ALGORITHM
            # ==============================
            df['nLoss'] = 1.0 * df['ATR_10'] 
            trail_stop = np.zeros(len(df))
            trend = np.zeros(len(df))
            closes = df['Close'].values
            nLosses = df['nLoss'].values
            trail_stop[0] = closes[0]
            trend[0] = 1
            
            for i in range(1, len(df)):
                if np.isnan(nLosses[i]):
                    trail_stop[i] = closes[i]
                    trend[i] = 1
                    continue
                prev_trail = trail_stop[i-1]
                prev_trend = trend[i-1]
                curr_close = closes[i]
                curr_nloss = nLosses[i]
                
                if prev_trend == 1:
                    if curr_close > prev_trail:
                        trail_stop[i] = max(prev_trail, curr_close - curr_nloss)
                        trend[i] = 1
                    else:
                        trail_stop[i] = curr_close + curr_nloss
                        trend[i] = -1
                else:
                    if curr_close < prev_trail:
                        trail_stop[i] = min(prev_trail, curr_close + curr_nloss)
                        trend[i] = -1
                    else:
                        trail_stop[i] = curr_close - curr_nloss
                        trend[i] = 1
                        
            df['UT_Trend'] = trend
            trend_now = trend[-1]
            trend_prev = trend[-2]
            
            if trend_now == 1 and trend_prev == -1: ut_signal = "🟢 BUY"
            elif trend_now == -1 and trend_prev == 1: ut_signal = "🔴 SELL"
            elif trend_now == 1: ut_signal = "🔼 Hold BUY"
            else: ut_signal = "🔽 Hold SELL"

            # ==============================
            # 5. SMART MONEY CONCEPTS (SMC) EXECUTION
            # ==============================
            df = get_swing_points(df, SWING_LENGTH)
            df = find_bullish_order_blocks(df)

            # ==============================
            # 6. EKSTRAKSI DATA & LOGIKA SCORING
            # ==============================
            price_today = float(df["Close"].iloc[-1])
            upper_kc = float(df['KCUe_20_2'].iloc[-1])
            middle_kc = float(df['KCMa_20_2'].iloc[-1]) 
            lower_kc = float(df['KCLe_20_2'].iloc[-1])
            
            vwap_today = float(df['VWAP'].iloc[-1])
            vwap_upper_today = float(df['VWAP_Upper'].iloc[-1])
            vwap_lower_today = float(df['VWAP_Lower'].iloc[-1])
            
            atr_today = float(df['ATR_10'].iloc[-1])
            atr_pct = (atr_today / price_today) * 100
            potensi_tp_pct = ((middle_kc - price_today) / price_today) * 100
            target_tp_price = middle_kc

            # SMC Order Block Status
            ob_top = df['Bullish_OB_Top'].iloc[-1]
            ob_bottom = df['Bullish_OB_Bottom'].iloc[-1]
            smc_status = "⚪ Tidak Ada OB Aktif"

            score = 0
            kc_status = "⚪ INSIDE KC"
            vwap_status = "⚪ DALAM BATAS WAJAR"
            action = "WAIT"

            # SMC Scoring Bonus (REVISI LOGIKA STATUS)
            if pd.notna(ob_top) and pd.notna(ob_bottom):
                if ob_bottom <= price_today <= ob_top:
                    smc_status = "🟢 DI DALAM OB (Area Beli)"
                    score += 50 
                elif price_today > ob_top:
                    jarak_ke_ob = (price_today - ob_top) / ob_top
                    if jarak_ke_ob <= 0.03: # Toleransi maksimal 3% di atas OB
                        smc_status = "🚀 DEKAT/MANTUL DARI OB"
                        score += 30
                    else:
                        smc_status = "🟡 DI ATAS OB (Tunggu Retest)"
                        score += 10 # Bonus kecil karena struktur masih uptrend
                elif price_today < ob_bottom:
                    # Safety fallback (jika fitur mitigasi terlewat)
                    smc_status = "🔴 OB JEBOL (Mitigated)"

            # 1. Deteksi Keltner Channel 
            for i in range(1, 5):
                try:
                    p_close = float(df["Close"].iloc[-i])
                    p_upper = float(df['KCUe_20_2'].iloc[-i])
                    p_lower = float(df['KCLe_20_2'].iloc[-i])
                    hari_teks = "Hari Ini" if i == 1 else f"{i-1} Hari Lalu"
                    
                    if p_close > p_upper:
                        kc_status = f"🔥 KC BREAKOUT ATAS ({hari_teks})"
                        action = "⚠️ RAWAN KOREKSI"
                        score -= 50  
                        break 
                    elif p_close < p_lower:
                        kc_status = f"📉 KC BREAKOUT BAWAH ({hari_teks})"
                        action = "🔍 PANTAU (Oversold)"
                        score += 30  
                        break
                except: continue

            # 2. Deteksi VWAP & Rubber Band
            is_deep_oversold = (price_today < lower_kc) and (price_today < vwap_lower_today)

            if price_today > vwap_upper_today:
                vwap_status = "🔥 OVERVALUED"
                action = "🛑 JANGAN DIBELI (Pucuk)"
                score -= 50 
            elif is_deep_oversold:
                vwap_status = "🧊 DEEP OVERSOLD"
                if atr_pct >= 3.0 and potensi_tp_pct >= 10.0:
                    action = "🎯 TARGET UTAMA: RUBBER BAND"
                    score += 100
                else:
                    action = "🧊 OVERSOLD (Potensi <10%)"
                    score += 40
            elif price_today < vwap_lower_today:
                vwap_status = "🧊 UNDERVALUED"
                if "PANTAU" in action: action = "💎 OVERSOLD ENTRY"
                score += 20
                
            # 3. Filter Konfirmasi Heikin Ashi
            if "TARGET UTAMA" in action or "OVERSOLD" in action:
                if "BULL" in ha_status:
                    score += 80 
                    action = "🟢 BUY: " + action + " (HA CONFIRMED)"
                else:
                    action = "⏳ WAIT: " + action + " (Tunggu HA Hijau)"
                    score -= 20 

            results.append({
                "Ticker": ticker,
                "Action": action,
                "Score": score,
                "Harga Skrg": int(price_today),
                "Target TP": int(target_tp_price),
                "Potensi TP (%)": round(potensi_tp_pct, 2),
                "ATR (%)": round(atr_pct, 2),
                "SMC Status": smc_status,
                "OB Top": int(ob_top) if pd.notna(ob_top) else "-",
                "OB Bottom": int(ob_bottom) if pd.notna(ob_bottom) else "-",
                "Status Keltner": kc_status,
                "Status VWAP": vwap_status,
                "Heikin Ashi": ha_status,
                "UT Bot (1,10)": ut_signal,
                "Last Update": waktu_update
            })
            
        except Exception as e:
            print(f"  -> Kalkulasi gagal untuk {ticker}: {e}")

    df_result = pd.DataFrame(results)

    desired_order = [
        "Ticker", "Action", "Score", "Harga Skrg", "Target TP", "Potensi TP (%)", "ATR (%)",
        "SMC Status", "OB Top", "OB Bottom", "Status Keltner", "Status VWAP", 
        "Heikin Ashi", "UT Bot (1,10)", "Last Update"
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
        "AMFG.JK", "AMIN.JK", "APII.JK", "ARKA.JK", "ARNA.JK", "ASGR.JK"
    ],
    "IDXNONCYC": [
        "AALI.JK", "ADES.JK", "AGAR.JK", "AISA.JK", "ALTO.JK", "AMMS.JK"
    ],
    "IDXFINANCE": [
        "ABDA.JK", "ADMF.JK", "AGRO.JK", "AGRS.JK", "AHAP.JK", "AMAG.JK"
    ],
    "IDXCYCLIC": [
        "ABBA.JK", "ACES.JK", "ACRO.JK", "AEGS.JK", "AKKU.JK", "ARGO.JK"
    ],
    "IDXTECHNO": [
        "AREA.JK", "ATIC.JK", "AWAN.JK", "AXIO.JK", "BELI.JK", "BUKA.JK"
    ],
    "IDXBASIC": [
        "ADMG.JK", "AGII.JK", "AKPI.JK", "ALDO.JK", "ALKA.JK", "ALMI.JK"
    ],
    "IDXENERGY": [
        "AADI.JK", "ABMM.JK", "ADMR.JK", "ADRO.JK", "AIMS.JK", "AKRA.JK"
    ],
    "IDXHEALTH": [
        "BMHS.JK", "CARE.JK", "CHEK.JK", "DGNS.JK", "DKHH.JK", "DVLA.JK"
    ],
    "IDXINFRA": [
        "ACST.JK", "ADHI.JK", "ARKO.JK", "ASLI.JK", "BALI.JK", "BDKR.JK"
    ],
    "IDXPROPERT": [
        "ADCP.JK", "AMAN.JK", "APLN.JK", "ARMY.JK", "ASPI.JK", "ASRI.JK"
    ],
    "IDXTRANS": [
        "AKSI.JK", "ASSA.JK", "BIRD.JK", "BLOG.JK", "BLTA.JK", "BPTR.JK"
    ]
}

# ==========================================
# MAIN
# ==========================================
if __name__ == "__main__":

    print("🤖 START MARKET SCANNER PRO (KELTNER + VWAP + SMC) 🤖")

    for sheet_name, saham_list in SECTOR_CONFIG.items():
        df_final = analyze_sector(sheet_name, saham_list)
        
        if df_final.empty:
            print(f"⚠️ Tidak ada data valid untuk {sheet_name}")
            continue

        ws = connect_gsheet(sheet_name)
        if ws:
            try:
                ws.clear()
                set_with_dataframe(ws, df_final)
                print(f"✅ {sheet_name} Updated! Tersimpan {len(df_final)} emiten.")
            except Exception as e:
                print(f"❌ Upload Error di {sheet_name}: {e}")
        
        # Delay singkat agar tidak terkena limit API yfinance atau Google Sheets
        time.sleep(1) 

    print("\n🏁 SELESAI 🏁")
