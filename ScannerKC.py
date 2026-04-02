# ==========================================
# MARKET SCANNER - PRO (KELTNER CHANNEL BREAKOUT)
# ==========================================

import yfinance as yf
import pandas as pd
import pandas_ta as ta
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
SPREADSHEET_ID = "1I_SJ3InMZPiSS1XibF-w000lwjc1PIsRaJ_kXzQ3LxE"

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
# ANALYZE FUNCTION (KELTNER CHANNEL)
# ==========================================
def analyze_sector(sector_name, ticker_list):

    tz_jkt = pytz.timezone("Asia/Jakarta")
    waktu_update = datetime.now(tz_jkt).strftime("%Y-%m-%d %H:%M:%S")
    
    results = []
    print(f"\n🚀 Scan {sector_name} | Total: {len(ticker_list)} saham")

    for ticker in ticker_list:
        try:
            # Download data 60 hari ke belakang
            df = yf.download(ticker, period="60d", interval="1d", progress=False, auto_adjust=True, threads=False)

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            if df.empty or len(df) < 30: 
                continue

            # Hitung Keltner Channel
            kc = df.ta.kc(length=20, scalar=2, mamode="ema")
            df = pd.concat([df, kc], axis=1)

            # Data Hari Ini (Untuk perhitungan jarak real-time)
            price_today = float(df["Close"].iloc[-1])
            upper_today = float(df['KCUe_20_2'].iloc[-1])
            lower_today = float(df['KCLe_20_2'].iloc[-1])
            middle_today = float(df['KCMa_20_2'].iloc[-1])

            jarak_ke_upper = ((upper_today - price_today) / price_today) * 100
            jarak_ke_lower = ((price_today - lower_today) / price_today) * 100

            # ==============================
            # LOOKBACK WINDOW LOGIC (0 - 3 Hari Lalu)
            # ==============================
            score = 0
            status = "⚪ INSIDE CHANNEL"
            action = "WAIT"

            # Looping dari indeks -1 (Hari Ini) sampai -4 (3 Hari Lalu)
            for i in range(1, 5):
                try:
                    p_close = float(df["Close"].iloc[-i])
                    p_upper = float(df['KCUe_20_2'].iloc[-i])
                    p_lower = float(df['KCLe_20_2'].iloc[-i])
                    
                    # Penamaan label hari
                    hari_teks = "Hari Ini" if i == 1 else f"{i-1} Hari Lalu"
                    
                    if p_close > p_upper:
                        status = f"🚀 BREAKOUT ATAS ({hari_teks})"
                        # Jika breakout terjadi kemarin/lusa, itu bisa jadi peluang 'Retest/Pullback'
                        action = "🟢 BUY MOMENTUM" if i == 1 else "🟡 PULLBACK / RETEST"
                        # Skor berkurang 2 poin per hari agar yg paling baru tetap di urutan atas
                        score = 100 - (i * 2) 
                        break # Hentikan loop ke belakang jika sudah menemukan sinyal terdekat
                        
                    elif p_close < p_lower:
                        status = f"📉 BREAKOUT BAWAH ({hari_teks})"
                        action = "🔴 SELL / AVOID"
                        score = -100 + (i * 2)
                        break
                except:
                    continue

            # Jika tidak ada riwayat breakout di 3 hari terakhir, cek posisinya hari ini
            if status == "⚪ INSIDE CHANNEL":
                if jarak_ke_upper <= 2.0:
                    status = "⚠️ MENGUJI UPPER BAND"
                    action = "WATCHLIST (Potensi Breakout)"
                    score = 50
                elif jarak_ke_lower <= 2.0:
                    status = "⚠️ MENGUJI LOWER BAND"
                    action = "WATCHLIST (Potensi Breakdown)"
                    score = -50

            # Masukkan ke dalam array hasil
            results.append({
                "Ticker": ticker,
                "Status": status,
                "Action": action,
                "Score": score,
                "Harga Skrg": int(price_today),
                "Upper KC": int(upper_today),
                "Middle KC (EMA)": int(middle_today),
                "Lower KC": int(lower_today),
                "Jarak ke Upper (%)": round(jarak_ke_upper, 2),
                "Jarak ke Lower (%)": round(jarak_ke_lower, 2),
                "Last Update": waktu_update
            })

        except Exception as e:
            pass

    df_result = pd.DataFrame(results)

    desired_order = [
        "Ticker", "Status", "Action", "Score", "Harga Skrg", 
        "Upper KC", "Middle KC (EMA)", "Lower KC", 
        "Jarak ke Upper (%)", "Jarak ke Lower (%)", "Last Update"
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

    print("🤖 START MARKET SCANNER PRO (KELTNER CHANNEL) 🤖")

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
