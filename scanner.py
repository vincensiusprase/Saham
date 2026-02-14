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

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            price = float(df["Close"].iloc[-1])
            ma20 = float(df["Close"].rolling(20).mean().iloc[-1])

            vol_today = df["Volume"].iloc[-1]
            vol_ma20 = df["Volume"].rolling(20).mean().iloc[-1]

            # ===== INDICATORS =====
            rsi = ta.momentum.RSIIndicator(df["Close"], window=14).rsi().iloc[-1]

            macd_ind = ta.trend.MACD(df["Close"])
            macd_line = macd_ind.macd().iloc[-1]
            macd_signal = macd_ind.macd_signal().iloc[-1]

            atr = ta.volatility.AverageTrueRange(
                df["High"], df["Low"], df["Close"], window=14
            ).average_true_range().iloc[-1]

            if pd.isna(macd_line) or pd.isna(macd_signal) or pd.isna(rsi):
                continue

            # ===== FIBO =====
            lookback = 120
            recent = df.iloc[-lookback:]
            high_swing = float(recent["High"].max())
            low_swing = float(recent["Low"].min())
            range_price = high_swing - low_swing

            target_aman = int(high_swing)
            target_jp = int(high_swing + (range_price * 0.618))

            stop_loss = int(df["Low"].iloc[-10:].min() * 0.97)

            # ===== LOGIC =====
            is_uptrend = price > ma20
            is_macd = macd_line > macd_signal
            is_rsi = rsi < 70
            is_vol_break = vol_today > (1.5 * vol_ma20)

            posisi_ma = "DI ATAS MA20" if is_uptrend else "DI BAWAH MA20"

            # ===== Risk Reward =====
            if price > stop_loss:
                rr = round((target_aman - price) / (price - stop_loss), 2)
            else:
                rr = 0

            # ===== Estimasi Waktu =====
            if atr > 0:
                est_aman = round((target_aman - price) / atr)
                est_jp = round((target_jp - price) / atr)
            else:
                est_aman = "-"
                est_jp = "-"

            # ===== ATR Percentage =====
            atr_pct = (atr / price) * 100 if price > 0 else 0

            if atr_pct > 3:
                tipe_swing = "Swing Harian"
            elif 1.5 < atr_pct <= 3:
                tipe_swing = "Swing Mingguan"
            else:
                tipe_swing = "Swing Bulanan"

            # ===== SMART SCORE =====
            score = 0
            if is_uptrend: score += 2
            if is_macd: score += 2
            if is_rsi: score += 1
            if is_vol_break: score += 2
            if rr >= 2: score += 2

            if score >= 7:
                action = "🔥 STRONG BUY"
            elif score >= 4:
                action = "🟢 BUY"
            elif score >= 2:
                action = "🟡 WAIT"
            else:
                action = "⚪ PANTAU"

            # ===== Alasan =====
            alasan = []
            if is_uptrend: alasan.append("Trend naik")
            if is_macd: alasan.append("MACD bullish")
            if is_vol_break: alasan.append("Volume breakout")
            if rr >= 2: alasan.append("RR bagus")

            if not alasan:
                alasan.append("Belum cukup konfirmasi")

            alasan_text = ", ".join(alasan)

            potensi_max = round(((target_jp - price) / price) * 100, 1)
            potensi_aman = round(((target_aman - price) / price) * 100, 1)
            
            results.append({
                "Ticker": ticker,
                "Tanggal": tgl_skrg,
                "Jam Update": waktu_skrg,
                "Harga Skrg": int(price),
                "Posisi vs MA20": posisi_ma,
                "Volume MA20": int(vol_ma20),
                "Volume Breakout": is_vol_break,
                "RSI": round(rsi,1),
                "Risk/Reward": rr,
                "Action": action,
                "Stop Loss": stop_loss,
                "Target Aman": target_aman,
                "Est. Waktu Aman (hari)": est_aman,
                "Target Jackpot": target_jp,
                "Est. Waktu JP (hari)": est_jp,
                "Potensi Aman (%)": potensi_aman,
                "Potensi MAX (%)": potensi_max,
                "Tipe Swing Disarankan": tipe_swing,
                "Alasan Rekomendasi": alasan_text,
                "Score": score
            })

        except Exception as e:
            print(f"Error {ticker}: {e}")
            continue

    df_result = pd.DataFrame(results)

    if not df_result.empty:
        df_result = df_result.sort_values(
            by=["Score","Risk/Reward"],
            ascending=False
        )

    return df_result


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
