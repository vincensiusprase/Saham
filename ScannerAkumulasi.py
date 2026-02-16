# ==========================================
# MARKET SCANNER - PRO (MINERVINI + FIBO + NEWS SENTIMENT)
# ==========================================

import yfinance as yf
import pandas as pd
import numpy as np
import ta
import gspread
from gspread_dataframe import set_with_dataframe
from datetime import datetime, timedelta
import pytz
import warnings
import json
import os
import time
from google.oauth2.service_account import Credentials
from GoogleNews import GoogleNews # Library Baru

warnings.filterwarnings('ignore')

SPREADSHEET_ID = "1I_SJ3InMZPiSS1XibF-w000lwjc1PIsRaJ_kXzQ3LxE"

# ==========================================
# FUNGSI SENTIMENT ANALISIS (BARU)
# ==========================================
def check_news_sentiment(ticker_symbol):
    """
    Mencari berita via Google News dan memberikan skor sentimen sederhana
    berdasarkan keyword positif/negatif.
    """
    try:
        # Bersihkan Ticker (BBRI.JK -> BBRI)
        clean_ticker = ticker_symbol.replace(".JK", "")
        
        # Setup Google News (Bahasa Indonesia, Periode 7 Hari Terakhir)
        googlenews = GoogleNews(lang='id', region='ID', period='7d')
        googlenews.search(clean_ticker)
        results = googlenews.result()
        
        if not results:
            return "No News", 0

        # Keyword Database (Sederhana)
        positive_keywords = [
            "laba naik", "dividen", "akuisisi", "merger", "buyback", 
            "proyek baru", "kerjasama", "investasi", "untung", "lonjakan",
            "ekspansi", "tertinggi", "positif", "disetujui", "bonus"
        ]
        
        negative_keywords = [
            "rugi", "turun", "anjlok", "pkpu", "pailit", "gugat", 
            "suspensi", "utang", "beban", "negatif", "korupsi", 
            "diperiksa", "sanksi", "denda", "phk"
        ]

        sentiment_score = 0
        news_titles = []

        # Analisa 3 Berita Teratas
        for i, item in enumerate(results[:3]):
            title = item['title'].lower()
            news_titles.append(item['title'])
            
            # Scoring
            for word in positive_keywords:
                if word in title: sentiment_score += 1
            
            for word in negative_keywords:
                if word in title: sentiment_score -= 1

        # Kesimpulan Narasi
        if sentiment_score > 0:
            narrative = "🟢 POSITIVE NEWS"
        elif sentiment_score < 0:
            narrative = "🔴 NEGATIVE NEWS"
        else:
            narrative = "⚪ NEUTRAL/NO SIGNAL"

        # Gabungkan judul berita untuk info (maks 1 judul teratas)
        headline = news_titles[0] if news_titles else "-"
        
        return f"{narrative} | {headline}", sentiment_score

    except Exception as e:
        print(f"News Error: {e}")
        return "Error", 0

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
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
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
    waktu_update = datetime.now(tz_jkt).strftime("%Y-%m-%d %H:%M:%S")
    
    results = []
    print(f"\n🚀 Scan {sector_name} | Total: {len(ticker_list)} saham")

    for ticker in ticker_list:
        try:
            # Download data (Sedikit lebih cepat dengan threads=True jika scrape news dimatikan sementara)
            df = yf.download(ticker, period="2y", progress=False, auto_adjust=True, threads=False)

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            if df.empty or len(df) < 200: continue

            price = float(df["Close"].iloc[-1])

            # ==============================
            # 1. MINERVINI TREND TEMPLATE
            # ==============================
            ma_50 = df["Close"].rolling(50).mean().iloc[-1]
            ma_150 = df["Close"].rolling(150).mean().iloc[-1]
            ma_200 = df["Close"].rolling(200).mean().iloc[-1]
            ma_200_prev = df["Close"].rolling(200).mean().iloc[-22]
            
            c1 = price > ma_150 and price > ma_200
            c2 = ma_150 > ma_200
            c3 = ma_200 > ma_200_prev
            c4 = price > ma_50
            
            is_super_uptrend = c1 and c2 and c3 and c4
            is_moderate_uptrend = c1 and c3

            # ==============================
            # 2. WYCKOFF & PATTERNS
            # ==============================
            high_60 = df["High"].rolling(60).max().iloc[-1]
            low_60 = df["Low"].rolling(60).min().iloc[-1]
            range_span = high_60 - low_60
            range_pct = range_span / price
            is_consolidating = range_pct < 0.40

            atr = ta.volatility.AverageTrueRange(df["High"], df["Low"], df["Close"], window=14).average_true_range()
            atr_now = atr.iloc[-1]
            atr_prev = atr.iloc[-25]
            is_vcp = atr_now < atr_prev

            low_5 = df["Low"].iloc[-5:].min()
            is_spring_ma50 = (low_5 < ma_50) and (price > ma_50)

            # ==============================
            # 3. VOLUME & MOMENTUM
            # ==============================
            vol_ma50 = df["Volume"].rolling(50).mean().iloc[-1]
            vol_now = df["Volume"].iloc[-1]
            is_vol_spike = vol_now > (vol_ma50 * 1.5)
            rsi = ta.momentum.RSIIndicator(df["Close"]).rsi().iloc[-1]

            # ==============================
            # 4. TARGET & RISK (FIBO LADDER)
            # ==============================
            if price > (low_60 + range_span * 0.5): 
                stop_loss = ma_50 
            else:
                stop_loss = low_60 - atr_now 

            fib_levels = [0.236, 0.382, 0.5, 0.618, 0.786, 1.0, 1.272, 1.618]
            target_aman = 0; target_moon = 0; fib_note = ""
            found_target = False
            
            for level in fib_levels:
                calc_price = low_60 + (range_span * level)
                if calc_price > price * 1.02: 
                    target_aman = calc_price
                    fib_note = f"Fib {level}"
                    try:
                        next_idx = fib_levels.index(level) + 1
                        if next_idx < len(fib_levels):
                             target_moon = low_60 + (range_span * fib_levels[next_idx])
                        else:
                             target_moon = low_60 + (range_span * 2.0)
                    except:
                        target_moon = target_aman * 1.1
                    found_target = True
                    break
            
            if not found_target:
                target_aman = price * 1.05; target_moon = price * 1.15; fib_note = "Blue Sky"

            risk = price - stop_loss
            reward = target_aman - price
            if risk <= 0: risk = 0.1
            rr = reward / risk

            potensi_aman = ((target_aman - price) / price) * 100
            potensi_moon = ((target_moon - price) / price) * 100

            # ==============================
            # SCORING AWAL (TEKNIKAL SAJA)
            # ==============================
            score = 0
            if is_super_uptrend: score += 40
            elif is_moderate_uptrend: score += 20
            else: score -= 20
            if is_consolidating: score += 15
            if is_vcp: score += 15
            if is_spring_ma50: score += 15
            if is_vol_spike: score += 10
            if 40 < rsi < 70: score += 5

            # ==============================
            # 🔥 5. INTELLIGENT NEWS CHECK
            # ==============================
            # Hanya cek berita jika saham sudah lolos kriteria teknikal (Score >= 70)
            # Ini untuk menghemat waktu dan menghindari blocking IP
            
            news_info = "-"
            news_score = 0
            
            if score >= 70:
                # print(f"🔍 Checking News for {ticker}...") # Uncomment untuk debug
                news_info, news_score = check_news_sentiment(ticker)
                
                # Update Score berdasarkan Berita
                if news_score > 0: score += 10 # Berita Bagus = Tambah Poin
                elif news_score < 0: score -= 15 # Berita Buruk = Kurangi Poin Drastis (Safety)

            # ==============================
            # CLASSIFICATION
            # ==============================
            trend_desc = "🚀 SUPER UPTREND" if is_super_uptrend else ("📈 Uptrend" if is_moderate_uptrend else "⚠️ Sideways/Down")

            action = "⚪ WATCHLIST"
            if score >= 85 and rr >= 2: action = "💎 STRONG BUY"
            elif score >= 70 and rr >= 1.5: action = "🟢 BUY"

            reasons = []
            if is_super_uptrend: reasons.append("Strong Trend")
            if is_vcp: reasons.append("VCP")
            if is_spring_ma50: reasons.append("Pantul MA50")
            if is_vol_spike: reasons.append("Vol Spike")
            if "Breakout" in fib_note: reasons.append("Breakout")
            if news_score > 0: reasons.append("Positive News") # Tambah alasan berita
            
            alasan_text = ", ".join(reasons) if reasons else "-"

            results.append({
                "Ticker": ticker,
                "Harga Skrg": int(price),
                "Trend Status": trend_desc,
                "Action": action,
                "Score": score,
                "Narasi Berita": news_info, # Kolom Baru
                "Risk/Reward": round(rr, 2),
                "Target Aman": int(target_aman),
                "Target Moon": int(target_moon),
                "Potensi Aman (%)": round(potensi_aman, 2),
                "Potensi Moon (%)": round(potensi_moon, 2),
                "Stop Loss": int(stop_loss),
                "Alasan": alasan_text,
                "Last Update": waktu_update
            })

        except Exception as e:
            pass

    df_result = pd.DataFrame(results)

    # Tambahkan kolom Narasi Berita ke urutan
    desired_order = [
        "Ticker", "Harga Skrg", "Trend Status", "Action", "Score", "Narasi Berita", # Updated
        "Risk/Reward", "Target Aman", "Target Moon", 
        "Potensi Aman (%)", "Potensi Moon (%)", "Stop Loss", "Alasan", "Last Update"
    ]
    
    if not df_result.empty:
        available_cols = [c for c in desired_order if c in df_result.columns]
        df_result = df_result[available_cols]
        df_result = df_result.sort_values(by="Score", ascending=False)

    return df_result

# ==========================================
# TEST SECTOR
# ==========================================
SECTOR_CONFIG = {    
    # ... GUNAKAN LIST SECTOR LAMA ANDA DI SINI ...
    # Saya sarankan test dulu dengan 1 sektor kecil untuk memastikan news scraping berjalan
    "TEST_NEWS": ["BBRI.JK", "GOTO.JK", "BREN.JK"] 
}

# ==========================================
# MAIN
# ==========================================
if __name__ == "__main__":
    print("🤖 START MARKET SCANNER PRO (WITH NEWS NARRATION) 🤖")
    print("⚠️ Note: Proses akan lebih lambat karena download berita...")

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
        time.sleep(2) # Sleep lebih lama agar tidak dianggap spam oleh Google News

    print("🏁 SELESAI 🏁")
