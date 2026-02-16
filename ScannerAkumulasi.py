# ==========================================
# MARKET SCANNER - PRO VERSION
# ==========================================

import yfinance as yf
import pandas as pd
import numpy as np
import ta
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

SPREADSHEET_ID = "1I_SJ3InMZPiSS1XibF-w000lwjc1PIsRaJ_kXzQ3LxE"

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
            worksheet = sh.add_worksheet(title=target_sheet_name, rows="300", cols="25")

        return worksheet

    except Exception as e:
        print(f"❌ Error Koneksi GSheet: {e}")
        return None


# ==========================================
# ANALYZE FUNCTION
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
                auto_adjust=True,
                threads=False)

            # 🔥 FIX MultiIndex column
        if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            price = float(df["Close"].iloc[-1])

            # ==============================
            # BASE CONDITION
            # ==============================
            high_120 = df["High"].rolling(120).max().iloc[-1]
            low_120 = df["Low"].rolling(120).min().iloc[-1]
            range_pct = (high_120 - low_120) / price
            base_condition = range_pct < 0.35

            # ==============================
            # VOLUME SHIFT
            # ==============================
            vol_ma50 = df["Volume"].rolling(50).mean().iloc[-1]
            vol_10 = df["Volume"].iloc[-10:].mean()
            is_volume_shift = vol_10 > vol_ma50

            # ==============================
            # ATR
            # ==============================
            high_low = df["High"] - df["Low"]
            high_close = np.abs(df["High"] - df["Close"].shift())
            low_close = np.abs(df["Low"] - df["Close"].shift())

            ranges = pd.concat([high_low, high_close, low_close], axis=1)
            true_range = ranges.max(axis=1)

            atr_now = true_range.rolling(14).mean().iloc[-1]
            atr_prev = true_range.rolling(14).mean().iloc[-30]
            volatility_contracting = atr_now < atr_prev

            # ==============================
            # SPRING
            # ==============================
            support = low_120
            recent_low = df["Low"].iloc[-5:].min()
            spring = recent_low < support * 0.98 and price > support

            # ==============================
            # RSI
            # ==============================
            delta = df["Close"].diff()
            gain = delta.clip(lower=0)
            loss = -delta.clip(upper=0)

            avg_gain = gain.rolling(14).mean()
            avg_loss = loss.rolling(14).mean()

            rs = avg_gain / avg_loss
            rsi = float((100 - (100 / (1 + rs))).iloc[-1])

            # ==============================
            # OBV
            # ==============================
            obv = (np.sign(df["Close"].diff()) * df["Volume"]).fillna(0).cumsum()
            obv_trend = obv.iloc[-1] > obv.iloc[-20]

            # ==============================
            # TARGET & RISK
            # ==============================
            target_aman = low_120 + (high_120 - low_120) * 0.5
            target_jp = high_120
            stop_loss = low_120 - atr_now

            risk = price - stop_loss
            reward = target_aman - price

            if risk <= 0:
                continue

            rr = reward / risk

            potensi_aman = ((target_aman - price) / price) * 100
            potensi_max = ((target_jp - price) / price) * 100

            # ==============================
            # SCORE
            # ==============================
            acc_score = 0
            if base_condition: acc_score += 20
            if is_volume_shift: acc_score += 20
            if volatility_contracting: acc_score += 15
            if spring: acc_score += 20
            if obv_trend: acc_score += 15
            if 40 < rsi < 60: acc_score += 10

            if spring:
                tipe = "Spring Wyckoff"
            elif base_condition and is_volume_shift:
                tipe = "Base Accumulation"
            else:
                tipe = "Early Base"

            if acc_score >= 75 and rr >= 2:
                action = "💎 STRONG ACCUMULATION"
            elif acc_score >= 60:
                action = "🟢 EARLY ACCUMULATION"
            else:
                action = "⚪ WATCHLIST"

            results.append({
                "Ticker": ticker,
                "Harga Skrg": int(price),
                "RSI": round(rsi,2),
                "Risk/Reward": round(rr,2),
                "Target Aman": int(target_aman),
                "Target Jackpot": int(target_jp),
                "Potensi Aman (%)": round(potensi_aman,2),
                "Potensi MAX (%)": round(potensi_max,2),
                "Tipe Akumulasi": tipe,
                "Action": action,
                "Acc Score": acc_score
            })

        except Exception as e:
            print(f"Error {ticker}: {e}")

    df_result = pd.DataFrame(results)

    if not df_result.empty:
        df_result = df_result.sort_values(by="Acc Score", ascending=False)

    return df_result

# ==========================================
# TEST SECTOR
# ==========================================
SECTOR_CONFIG = {
    "TEST": [
        "TOWR.JK", "FUTR.JK", "PIPA.JK" ]
}

# ==========================================
# MAIN
# ==========================================
if __name__ == "__main__":

    print("🤖 START MARKET SCANNER PRO 🤖")

    for sheet_name, saham_list in SECTOR_CONFIG.items():

        df_final = analyze_sector(sheet_name, saham_list)

        if df_final.empty:
            print(f"⚠️ Tidak ada data untuk {sheet_name}")
            continue

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
