# ==========================================
# MARKET SCANNER - GITHUB ACTIONS VERSION
# (USING ta LIBRARY)
# ==========================================

import yfinance as yf
import pandas as pd
import numpy as np
import ta
import gspread
from gspread_dataframe import set_with_dataframe
from tqdm import tqdm
from datetime import datetime
import pytz
import warnings
import json
import os
import time
from google.oauth2.service_account import Credentials

warnings.filterwarnings('ignore')

SPREADSHEET_ID = "1bUzWbd1pqTZO37cZ1rQzTelqUcykz_oOwULOCmK-HNc"

# ==========================================
# GOOGLE SHEET CONNECTION
# ==========================================
def connect_gsheet(target_sheet_name):
    try:
        creds_json = os.environ.get("GCP_SA_KEY")

        if not creds_json:
            print("❌ GCP_SA_KEY tidak ditemukan")
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
            print(f"📝 Membuat Sheet baru: {target_sheet_name}")
            worksheet = sh.add_worksheet(title=target_sheet_name, rows="300", cols="20")

        return worksheet

    except Exception as e:
        print(f"❌ Error Koneksi GSheet: {e}")
        return None


# ==========================================
# ANALISA SEKTOR
# ==========================================
def analyze_sector(sector_name, ticker_list):

    tz_jkt = pytz.timezone("Asia/Jakarta")
    waktu_skrg = datetime.now(tz_jkt).strftime("%H:%M")
    tgl_skrg = datetime.now(tz_jkt).strftime("%Y-%m-%d")

    results = []

    print(f"\n🚀 Scan {sector_name} | Total: {len(ticker_list)} saham")

    for ticker in ticker_list:
        try:
            df = yf.download(
                ticker,
                period="1y",
                progress=False,
                auto_adjust=False,
                threads=False
            )

            if df.empty or len(df) < 50:
                continue

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            price = float(df["Close"].iloc[-1])
            ma20 = float(df["Close"].rolling(20).mean().iloc[-1])

            # ======================
            # TECHNICAL INDICATORS
            # ======================

            # RSI
            rsi_indicator = ta.momentum.RSIIndicator(close=df["Close"], window=14)
            df["RSI"] = rsi_indicator.rsi()
            rsi_val = df["RSI"].iloc[-1]

            # MACD
            macd_indicator = ta.trend.MACD(close=df["Close"])
            df["MACD"] = macd_indicator.macd()
            df["MACD_SIGNAL"] = macd_indicator.macd_signal()

            macd_line = df["MACD"].iloc[-1]
            macd_signal = df["MACD_SIGNAL"].iloc[-1]

            if pd.isna(macd_line) or pd.isna(macd_signal) or pd.isna(rsi_val):
                continue

            # ======================
            # FIBO
            # ======================
            lookback = 120
            recent = df.iloc[-lookback:]

            high_swing = float(recent["High"].max())
            low_swing = float(recent["Low"].min())

            range_price = high_swing - low_swing

            target_aman = int(high_swing)
            target_jp = int(high_swing + (range_price * 0.618))

            upside_jp = (target_jp - price) / price
            potensi_max = round(upside_jp * 100, 1)

            # ======================
            # LOGIC
            # ======================
            is_uptrend = price > ma20
            is_macd = macd_line > macd_signal
            is_rsi = rsi_val < 70

            stop_loss = int(df["Low"].iloc[-10:].min() * 0.97)

            if is_uptrend and is_macd and is_rsi:
                action = "BELI (Strong)"
            elif is_uptrend:
                action = "WAIT"
            else:
                action = "PANTAU"

            results.append({
                "Ticker": ticker,
                "Tanggal": tgl_skrg,
                "Jam": waktu_skrg,
                "Harga": int(price),
                "MA20": int(ma20),
                "RSI": round(rsi_val, 1),
                "MACD>Signal": is_macd,
                "Action": action,
                "Stop Loss": stop_loss,
                "Target Aman": target_aman,
                "Target Jackpot": target_jp,
                "Potensi MAX (%)": potensi_max
            })

        except Exception as e:
            print(f"Error {ticker}: {e}")
            continue

    return pd.DataFrame(results)


# ==========================================
# TEST SECTOR
# ==========================================
SECTOR_CONFIG = {
    "TEST": ["BBCA.JK", "BBRI.JK", "TLKM.JK"]
}

# ==========================================
# MAIN
# ==========================================
if __name__ == "__main__":

    print("🤖 START MARKET SCANNER 🤖")

    for sheet_name, saham_list in SECTOR_CONFIG.items():

        df_final = analyze_sector(sheet_name, saham_list)

        if df_final.empty:
            print(f"⚠️ Tidak ada data untuk {sheet_name}")
            continue

        df_final["Score"] = df_final["Action"].apply(
            lambda x: 2 if "BELI" in x else (1 if "WAIT" in x else 0)
        )

        df_final = df_final.sort_values(
            by=["Score", "Potensi MAX (%)"],
            ascending=[False, False]
        ).drop(columns=["Score"])

        ws = connect_gsheet(sheet_name)

        if ws:
            try:
                ws.clear()
                set_with_dataframe(ws, df_final)
                print(f"✅ {sheet_name} Updated!")
            except Exception as e:
                print(f"❌ Upload Error: {e}")

        time.sleep(1)

    print("🏁 SELESAI 🏁")
