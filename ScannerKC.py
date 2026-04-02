# ==========================================
# MARKET SCANNER - PRO (KELTNER CHANNEL + VWAP)
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
# ANALYZE FUNCTION (KELTNER + VWAP BANDS FIXED)
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

            # Fix struktur kolom yfinance terbaru
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            # Jika saham delisted/tidak ada data (seperti KLJA.JK), lewati
            if df.empty or len(df) < 30: 
                continue

# ==============================
            # 1. KELTNER CHANNEL (TradingView Exact Match)
            # EMA 20, ATR 10 (RMA Smoothing), Multiplier 2.0
            # ==============================
            # Hitung Middle Line (EMA 20)
            df['EMA_20'] = df['Close'].ewm(span=20, adjust=False).mean()

            # Hitung True Range (TR)
            high_low = df['High'] - df['Low']
            high_close = np.abs(df['High'] - df['Close'].shift(1))
            low_close = np.abs(df['Low'] - df['Close'].shift(1))
            true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)

            # Hitung ATR menggunakan RMA (Persis seperti TradingView 'ta.atr')
            # alpha = 1 / ATR_Length
            df['ATR_10'] = true_range.ewm(alpha=1/10, adjust=False).mean()

            # Hitung Upper dan Lower Bands
            df['KCUe_20_2'] = df['EMA_20'] + (2.0 * df['ATR_10'])
            df['KCLe_20_2'] = df['EMA_20'] - (2.0 * df['ATR_10'])
            df['KCMa_20_2'] = df['EMA_20']
            
            # ==============================
            # 2. VWAP BANDS CALCULATION (ANCHOR: WEEKLY)
            # ==============================
            # FIX CRITICAL: Hapus zona waktu (timezone) dari index sebelum diconvert ke Period
            if df.index.tz is not None:
                df.index = df.index.tz_localize(None)

            # Buat penanda MINGGU (Week) untuk Anchoring
            df['Week'] = df.index.to_period('W')
            
            # Typical Price & Volume Berbobot
            df['TP'] = (df['High'] + df['Low'] + df['Close']) / 3
            df['TPV'] = df['TP'] * df['Volume']

            # Cumulative TPV & Volume per MINGGU (Reset setiap Senin)
            df['Cum_TPV'] = df.groupby('Week')['TPV'].cumsum()
            df['Cum_Vol'] = df.groupby('Week')['Volume'].cumsum()
            
            # Garis Tengah VWAP
            df['VWAP'] = df['Cum_TPV'] / df['Cum_Vol']

            # Perhitungan Standar Deviasi untuk Bands
            df['Dev'] = df['TP'] - df['VWAP']
            df['Dev_Sq_Vol'] = (df['Dev'] ** 2) * df['Volume']
            df['Cum_Dev_Sq_Vol'] = df.groupby('Week')['Dev_Sq_Vol'].cumsum()
            df['VWAP_Variance'] = df['Cum_Dev_Sq_Vol'] / df['Cum_Vol']
            df['VWAP_Stdev'] = np.sqrt(df['VWAP_Variance'])

            # Upper dan Lower VWAP Band (Multiplier = 2.0)
            vwap_mult = 2.0
            df['VWAP_Upper'] = df['VWAP'] + (vwap_mult * df['VWAP_Stdev'])
            df['VWAP_Lower'] = df['VWAP'] - (vwap_mult * df['VWAP_Stdev'])

            # ==============================
            # 3. EKSTRAKSI DATA HARI INI
            # ==============================
            price_today = float(df["Close"].iloc[-1])
            upper_kc = float(df['KCUe_20_2'].iloc[-1])
            lower_kc = float(df['KCLe_20_2'].iloc[-1])
            
            vwap_today = float(df['VWAP'].iloc[-1])
            vwap_upper_today = float(df['VWAP_Upper'].iloc[-1])
            vwap_lower_today = float(df['VWAP_Lower'].iloc[-1])
            # ==============================
            # 4. LOGIKA SCORING (KC + VWAP)
            # ==============================
            score = 0
            kc_status = "⚪ INSIDE KC"
            vwap_status = "⚪ DALAM BATAS WAJAR VWAP"
            action = "WAIT"

            # Logika Keltner Channel
            for i in range(1, 5):
                try:
                    p_close = float(df["Close"].iloc[-i])
                    p_upper = float(df['KCUe_20_2'].iloc[-i])
                    p_lower = float(df['KCLe_20_2'].iloc[-i])
                    
                    hari_teks = "Hari Ini" if i == 1 else f"{i-1} Hari Lalu"
                    
                    if p_close > p_upper:
                        kc_status = f"🚀 KC BREAKOUT ATAS ({hari_teks})"
                        action = "🟢 BUY MOMENTUM" if i == 1 else "🟡 PULLBACK / RETEST"
                        score += 100 - (i * 2) 
                        break 
                    elif p_close < p_lower:
                        kc_status = f"📉 KC BREAKOUT BAWAH ({hari_teks})"
                        action = "🔴 SELL / AVOID"
                        score -= 100 - (i * 2)
                        break
                except:
                    continue

            # Logika VWAP Bands
            if price_today > vwap_upper_today:
                vwap_status = "🔥 OVERVALUED (Tembus VWAP Atas)"
                if "BUY" in action: action = "⚠️ RAWAN KOREKSI (Take Profit)"
                score += 20
            elif price_today < vwap_lower_today:
                vwap_status = "🧊 UNDERVALUED (Tembus VWAP Bawah)"
                if "PULLBACK" in action: action = "💎 SNIPER ENTRY"
                score -= 20

            # Masukkan ke dalam array hasil
            results.append({
                "Ticker": ticker,
                "Action": action,
                "Score": score,
                "Harga Skrg": int(price_today),
                "Status Keltner": kc_status,
                "Status VWAP": vwap_status,
                "VWAP Upper": int(vwap_upper_today),
                "VWAP": int(vwap_today),
                "VWAP Lower": int(vwap_lower_today),
                "Upper KC": int(upper_kc),
                "Lower KC": int(lower_kc),
                "Last Update": waktu_update
            })

        except Exception as e:
            # FIX: Jangan telan error secara diam-diam. Print errornya agar kita tahu!
            print(f"  -> Kalkulasi gagal untuk {ticker}: {e}")

    df_result = pd.DataFrame(results)

    # Sesuaikan urutan kolom untuk Google Sheets
    desired_order = [
        "Ticker", "Action", "Score", "Harga Skrg", "Status Keltner", "Status VWAP", 
        "VWAP Upper", "VWAP", "VWAP Lower", "Upper KC", "Lower KC", "Last Update"
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
        "ASII.JK", "UNTR.JK", "PIPA.JK", "BNBR.JK", "HEXA.JK", "IMPC.JK", "MHKI.JK", "LABA.JK"
    ],
    "IDXNONCYC": [
        "UNVR.JK", "INDF.JK", "ICBP.JK", "GGRM.JK", "AALI.JK", "JPFA.JK", "MYOR.JK", "CPIN.JK"
    ],
    "IDXFINANCE": [
        "BBCA.JK", "BBRI.JK", "BMRI.JK", "BBNI.JK", "BRIS.JK", "SUPA.JK", "COIN.JK", "BBTN.JK"
    ],
    "IDXCYCLIC": [
        "MNCN.JK", "SCMA.JK", "LPPF.JK", "MINA.JK", "BUVA.JK", "ACES.JK", "ERAA.JK", "HRTA.JK"
    ],
    "IDXTECHNO": [
        "GOTO.JK", "WIFI.JK", "EMTK.JK", "BUKA.JK", "WIRG.JK", "DCII.JK", "IOTF.JK", "MTDL.JK"
    ],
    "IDXBASIC": [
        "ANTM.JK", "BRMS.JK", "SMGR.JK", "BRPT.JK", "INTP.JK", "EMAS.JK", "MDKA.JK", "INCO.JK"
    ],
    "IDXENERGY": [
        "ADRO.JK", "BUMI.JK", "PGAS.JK", "PTBA.JK", "ITMG.JK", "DEWA.JK", "CUAN.JK", "HRUM.JK"
    ],
    "IDXHEALTH": [
        "KLBF.JK", "SIDO.JK", "KAEF.JK", "PYFA.JK", "MIKA.JK", "DKHH.JK", "SILO.JK", "HEAL.JK"
    ],
    "IDXINFRA": [
        "TLKM.JK", "CDIA.JK", "ADHI.JK", "JSMR.JK", "WIKA.JK", "PTPP.JK", "INET.JK", "WSKT.JK"
    ],
    "IDXPROPERT": [
        "CTRA.JK", "BSDE.JK", "PWON.JK", "SMRA.JK", "KLJA.JK", "PANI.JK", "BKSL.JK", "DADA.JK"
    ],
    "IDXTRANS": [
        "PJHB.JK", "GIAA.JK", "SMDR.JK", "BIRD.JK", "BLOG.JK", "IMJS.JK", "ASSA.JK", "TMAS.JK"
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
