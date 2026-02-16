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
                auto_adjust=False,
                threads=False
            )

            if df.empty or len(df) < 50:
                continue

        if len(df) < 200:
            continue

        price = float(df["Close"].iloc[-1])

        # ==============================
        # BASE CONDITION (120 HARI)
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
        # ATR & VOLATILITY CONTRACTING
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
        # SPRING DETECTION (FALSE BREAK)
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
        rsi = 100 - (100 / (1 + rs))
        rsi = float(rsi.iloc[-1])

        # ==============================
        # OBV
        # ==============================
        obv = (np.sign(df["Close"].diff()) * df["Volume"]).fillna(0).cumsum()
        obv_trend = obv.iloc[-1] > obv.iloc[-20]

        # ==============================
        # TARGET & STOP LOSS
        # ==============================
        target_aman = low_120 + (high_120 - low_120) * 0.5
        target_jp = high_120

        stop_loss = low_120 - atr_now

        potensi_aman = ((target_aman - price) / price) * 100
        potensi_max = ((target_jp - price) / price) * 100

        risk = price - stop_loss
        reward = target_aman - price

        if risk <= 0:
            continue

        rr = reward / risk

        # ==============================
        # ACCUMULATION SCORE
        # ==============================
        acc_score = 0
        if base_condition: acc_score += 20
        if is_volume_shift: acc_score += 20
        if volatility_contracting: acc_score += 15
        if spring: acc_score += 20
        if obv_trend: acc_score += 15
        if 40 < rsi < 60: acc_score += 10

        # ==============================
        # TIPE AKUMULASI
        # ==============================
        if spring:
            tipe = "Spring Wyckoff"
        elif base_condition and is_volume_shift:
            tipe = "Base Accumulation"
        else:
            tipe = "Early Base"

        # ==============================
        # ACTION
        # ==============================
        if acc_score >= 75 and rr >= 2:
            action = "💎 STRONG ACCUMULATION"
        elif acc_score >= 60:
            action = "🟢 EARLY ACCUMULATION"
        else:
            action = "⚪ WATCHLIST"

        # ==============================
        # ESTIMASI WAKTU (Swing)
        # ==============================
        est_aman = "2-4 Minggu"
        est_jp = "4-8 Minggu"

        alasan = []
        if base_condition: alasan.append("Sideways 4-6 bulan")
        if is_volume_shift: alasan.append("Volume meningkat")
        if volatility_contracting: alasan.append("ATR mengecil")
        if spring: alasan.append("Spring terdeteksi")
        if obv_trend: alasan.append("OBV naik")

        alasan_text = ", ".join(alasan)

        hasil.append({
            "Ticker": ticker,
            "Harga Skrg": int(price),
            "Base Condition": base_condition,
            "Vol MA 50": int(vol_ma50),
            "is_volume_shift": is_volume_shift,
            "volatility_contracting": volatility_contracting,
            "spring": spring,
            "RSI": round(rsi,2),
            "obv_trend": obv_trend,
            "Risk/Reward": round(rr,2),
            "Action": action,
            "Stop Loss": int(stop_loss),
            "Target Aman": int(target_aman),
            "Est. Waktu Aman": est_aman,
            "Target Jackpot": int(target_jp),
            "Est. Waktu JP": est_jp,
            "Potensi Aman (%)": round(potensi_aman,2),
            "Potensi MAX (%)": round(potensi_max,2),
            "Tipe Akumulasi": tipe,
            "Alasan Rekomendasi": alasan_text,
            "Acc Score": acc_score
        })

    except Exception as e:
        print(f"Error {ticker}: {e}")

# ==============================
# OUTPUT
# ==============================
df_hasil = pd.DataFrame(hasil)

if not df_hasil.empty:
    df_hasil = df_hasil.sort_values(by="Acc Score", ascending=False)
    print(df_hasil)
else:
    print("Tidak ada saham dalam fase akumulasi bawah saat ini.")

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
