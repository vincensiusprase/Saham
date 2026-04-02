# ==========================================
# MARKET SCANNER - PRO (KELTNER + VWAP + HA + UT BOT + RUBBER BAND)
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
SPREADSHEET_ID = "1CVHTapflLP1Lypr_Q1KXC0I9qPHCnpDYNKHdYx31kh0"

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
# ANALYZE FUNCTION (NATIVE MATH CALCULATIONS)
# ==========================================
def analyze_sector(sector_name, ticker_list):

    tz_jkt = pytz.timezone("Asia/Jakarta")
    waktu_update = datetime.now(tz_jkt).strftime("%Y-%m-%d %H:%M:%S")
    
    results = []
    print(f"\n🚀 Scan {sector_name} | Total: {len(ticker_list)} saham")

    for ticker in ticker_list:
        try:
            # Download data
            df = yf.download(ticker, period="60d", interval="1d", progress=False, auto_adjust=True, threads=False)

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            if df.empty or len(df) < 30: 
                continue

            # ==============================
            # 1. KELTNER CHANNEL (TradingView Exact Match)
            # ==============================
            df['EMA_20'] = df['Close'].ewm(span=20, adjust=False).mean()
            
            high_low = df['High'] - df['Low']
            high_close = np.abs(df['High'] - df['Close'].shift(1))
            low_close = np.abs(df['Low'] - df['Close'].shift(1))
            true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)

            # ATR 10 menggunakan RMA Smoothing
            df['ATR_10'] = true_range.ewm(alpha=1/10, adjust=False).mean()

            df['KCUe_20_2'] = df['EMA_20'] + (2.0 * df['ATR_10'])
            df['KCLe_20_2'] = df['EMA_20'] - (2.0 * df['ATR_10'])
            
            # KCMa
            df['KCMa_20_2'] = df['EMA_20'] 

            # ==============================
            # 2. VWAP BANDS (ANCHOR: WEEKLY)
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
            # 4. UT BOT ALGORITHM (Key: 1, ATR: 10)
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
            
            if trend_now == 1 and trend_prev == -1:
                ut_signal = "🟢 BUY"
            elif trend_now == -1 and trend_prev == 1:
                ut_signal = "🔴 SELL"
            elif trend_now == 1:
                ut_signal = "🔼 Hold BUY"
            else:
                ut_signal = "🔽 Hold SELL"

            # ==============================
            # 5. EKSTRAKSI DATA & LOGIKA SCORING (THE CONTRARIAN / RUBBER BAND)
            # ==============================
            price_today = float(df["Close"].iloc[-1])
            upper_kc = float(df['KCUe_20_2'].iloc[-1])
            middle_kc = float(df['KCMa_20_2'].iloc[-1]) 
            lower_kc = float(df['KCLe_20_2'].iloc[-1])
            
            vwap_today = float(df['VWAP'].iloc[-1])
            vwap_upper_today = float(df['VWAP_Upper'].iloc[-1])
            vwap_lower_today = float(df['VWAP_Lower'].iloc[-1])
            
            atr_today = float(df['ATR_10'].iloc[-1])
            
            # --- KALKULASI TARGET 10% ---
            atr_pct = (atr_today / price_today) * 100
            potensi_tp_pct = ((middle_kc - price_today) / price_today) * 100
            target_tp_price = middle_kc

            # --- LOGIKA SCORING CONTRARIAN ---
            score = 0
            kc_status = "⚪ INSIDE KC"
            vwap_status = "⚪ DALAM BATAS WAJAR"
            action = "WAIT"

            # 1. Deteksi Keltner Channel (Breakout Atas = Bahaya, Breakout Bawah = Pantau)
            for i in range(1, 5):
                try:
                    p_close = float(df["Close"].iloc[-i])
                    p_upper = float(df['KCUe_20_2'].iloc[-i])
                    p_lower = float(df['KCLe_20_2'].iloc[-i])
                    hari_teks = "Hari Ini" if i == 1 else f"{i-1} Hari Lalu"
                    
                    if p_close > p_upper:
                        kc_status = f"🔥 KC BREAKOUT ATAS ({hari_teks})"
                        action = "⚠️ RAWAN KOREKSI (Take Profit)"
                        # Penalti skor besar karena kita tidak mau beli saham pucuk
                        score -= 50  
                        break 
                    elif p_close < p_lower:
                        kc_status = f"📉 KC BREAKOUT BAWAH ({hari_teks})"
                        action = "🔍 PANTAU (Oversold Zone)"
                        # Tambah skor karena masuk ke zona target kita
                        score += 30  
                        break
                except: continue

            # 2. Deteksi VWAP & Setup Target Utama Anda
            is_deep_oversold = (price_today < lower_kc) and (price_today < vwap_lower_today)

            if price_today > vwap_upper_today:
                vwap_status = "🔥 OVERVALUED (Tembus VWAP Atas)"
                action = "🛑 JANGAN DIBELI (Pucuk VWAP)"
                score -= 50 # Buang ke peringkat bawah
                
            elif is_deep_oversold:
                vwap_status = "🧊 DEEP OVERSOLD (Bawah KC & VWAP)"
                
                # SETUP TARGET UTAMA ANDA: ATR liar dan jarak TP jauh
                if atr_pct >= 3.0 and potensi_tp_pct >= 10.0:
                    action = "🎯 TARGET UTAMA: RUBBER BAND (>10%)"
                    score += 100 # Skor Maksimal
                else:
                    action = "🧊 OVERSOLD (Potensi <10%)"
                    score += 40
                    
            elif price_today < vwap_lower_today:
                vwap_status = "🧊 UNDERVALUED (Tembus VWAP Bawah)"
                if "PANTAU" in action: action = "💎 OVERSOLD ENTRY"
                score += 20
                
            # 3. Filter Konfirmasi "Pisau Jatuh" menggunakan Heikin Ashi
            # Kita hanya mau entry jika saham yang hancur sudah mulai mantul (HA Hijau)
            if "TARGET UTAMA" in action or "OVERSOLD" in action:
                if "BULL" in ha_status:
                    score += 80 # SUPER JACKPOT: Harga hancur + Udah mantul hijau
                    action = "🟢 BUY: " + action + " (HA CONFIRMED)"
                else:
                    action = "⏳ WAIT: " + action + " (Tunggu HA Hijau)"
                    # Tahan skor agar tidak terlalu di atas, karena pisau masih jatuh (Merah)
                    score -= 20 

            results.append({
                "Ticker": ticker,
                "Action": action,
                "Score": score,
                "Harga Skrg": int(price_today),
                "Target TP": int(target_tp_price),
                "Potensi TP (%)": round(potensi_tp_pct, 2),
                "ATR (%)": round(atr_pct, 2),
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
        "Status Keltner", "Status VWAP", "Heikin Ashi", "UT Bot (1,10)", "Last Update"
    ]
    
    if not df_result.empty:
        available_cols = [c for c in desired_order if c in df_result.columns]
        df_result = df_result[available_cols]
        df_result = df_result.sort_values(by="Score", ascending=False)

    return df_result


# ==========================================
# SECTOR CONFIG (RATA KIRI / NO INDENT)
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

    print("🤖 START MARKET SCANNER PRO (KELTNER CHANNEL NATIVE) 🤖")

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
