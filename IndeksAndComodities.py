import os
import json
import warnings
from datetime import datetime
import pandas as pd
import yfinance as yf
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
import ta

warnings.filterwarnings("ignore")

# ── CONFIG ─────────────────────────────────────────────────────────────────
SPREADSHEET_ID = "11CTr-xGnEPdhozhBPsjXRyqqP_VgmCKt8cUCry2EdSc"
SHEET_1_NAME = "Harga"
SHEET_2_NAME = "Indikator"

# Mapping Ticker Yahoo Finance
TICKERS = {
    "Indices": {
        "IHSG": "^JKSE",
        "HANGSENG": "^HSI",
        "NIKKEI": "^N225",
        "SHANGHAI": "000001.SS",
        "STI_Singapore": "^STI",
        "NIFTY 50": "^NSEI",
        "S&P/ASX 200": "^AXJO",
        "CSI 300": "000300.SS",
        "KOSPI": "^KS11"
    },
    "Commodities": {
        "Crude Oil": "CL=F",
        "Brent Oil": "BZ=F",
        "CPO": "FCPO.KL",
        "Newcastle Coal": "KOL", # Proxy ETF Coal karena YF tidak punya ticker Newcastle ideal
        "XAU Gold": "GC=F",
        "Silver": "SI=F",
        "Nickel": "NI=F",
        "Gas": "NG=F",
        "Alumunium": "ALI=F",
        "Copper": "HG=F",
        "Rubber": "TSR20.SI", # SICOM Rubber
        "Tin": "JJT",
        "Zinc": "ZNC=F"
    }
}

def get_google_sheet(sheet_name):
    # Mengambil credential dari GitHub Secrets
    sa_key = os.environ.get("GCP_SA_KEY")
    if not sa_key:
        print("❌ GCP_SA_KEY tidak ditemukan di environment.")
        return None
        
    creds = Credentials.from_service_account_info(
        json.loads(sa_key),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)
    try:
        return sh.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        return sh.add_worksheet(title=sheet_name, rows="1000", cols="20")

def fetch_and_process_data():
    flat_tickers = {}
    for category, items in TICKERS.items():
        for name, ticker in items.items():
            flat_tickers[name] = {"ticker": ticker, "category": category}

    all_data = []
    
    print("⏳ Mendownload data 5 tahun terakhir...")
    for name, info in flat_tickers.items():
        ticker = info["ticker"]
        try:
            df = yf.download(ticker, period="5y", interval="1d", progress=False, auto_adjust=False)
            if df.empty:
                print(f"⚠️ Data kosong untuk {name} ({ticker})")
                continue
                
            # Jika menggunakan yfinance versi baru, MultiIndex column perlu di-flatten
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [c[0] for c in df.columns]
                
            df.index = pd.to_datetime(df.index).tz_localize(None)
            
            # Memastikan kolom ada
            for col in ['High', 'Low', 'Close']:
                if col not in df.columns:
                    if 'Adj Close' in df.columns and col == 'Close':
                        df['Close'] = df['Adj Close']
                    else:
                        continue
            
            # Kalkulasi Indikator
            c = df['Close']
            h = df['High']
            l = df['Low']
            
            df['EMA9'] = c.ewm(span=9, adjust=False).mean()
            df['EMA20'] = c.ewm(span=20, adjust=False).mean()
            df['EMA50'] = c.ewm(span=50, adjust=False).mean()
            df['EMA200'] = c.ewm(span=200, adjust=False).mean()
            
            # Ichimoku Cloud
            tenkan = (h.rolling(9).max() + l.rolling(9).min()) / 2
            kijun = (h.rolling(26).max() + l.rolling(26).min()) / 2
            df['Tenkan_Sen'] = tenkan
            df['Kijun_Sen'] = kijun
            df['Senkou_Span_A'] = ((tenkan + kijun) / 2).shift(26)
            df['Senkou_Span_B'] = ((h.rolling(52).max() + l.rolling(52).min()) / 2).shift(26)
            
            # Tambahkan metadata
            df['Asset_Name'] = name
            df['Category'] = info["category"]
            df['Date'] = df.index
            
            all_data.append(df)
            print(f"✅ Berhasil memproses {name}")
            
        except Exception as e:
            print(f"❌ Error memproses {name} ({ticker}): {e}")

    # Gabungkan semua data
    full_df = pd.concat(all_data, ignore_index=True)
    
    # ── PERSIAPAN SHEET 1: Harga Penutupan (Wide Format) ──
    # Kolom: Date, IHSG, HANGSENG, NIKKEI, dll...
    sheet1_df = full_df.pivot_table(index='Date', columns='Asset_Name', values='Close').reset_index()
    sheet1_df['Date'] = sheet1_df['Date'].dt.strftime('%Y-%m-%d')
    sheet1_df = sheet1_df.sort_values('Date')

    # ── PERSIAPAN SHEET 2: Indikator (Long Format untuk Looker) ──
    # Kolom: Date, Category, Asset_Name, Close, EMA9... Senkou_Span_B
    sheet2_cols = ['Date', 'Category', 'Asset_Name', 'Close', 'EMA9', 'EMA20', 'EMA50', 'EMA200', 
                   'Tenkan_Sen', 'Kijun_Sen', 'Senkou_Span_A', 'Senkou_Span_B']
    sheet2_df = full_df[sheet2_cols].copy()
    sheet2_df['Date'] = sheet2_df['Date'].dt.strftime('%Y-%m-%d')
    sheet2_df = sheet2_df.sort_values(['Date', 'Category', 'Asset_Name'])
    # Bulatkan ke 2 desimal agar GSheets tidak berat
    sheet2_df = sheet2_df.round(2)
    # Hapus baris yang semua indikatornya NaN (awal-awal periode)
    sheet2_df = sheet2_df.dropna(subset=['EMA9', 'EMA20'], how='all')

    return sheet1_df, sheet2_df

def upload_to_sheets():
    sheet1_df, sheet2_df = fetch_and_process_data()
    
    # Upload Sheet 1
    ws1 = get_google_sheet(SHEET_1_NAME)
    if ws1:
        print(f"📤 Uploading ke {SHEET_1_NAME}...")
        ws1.clear()
        set_with_dataframe(ws1, sheet1_df, include_index=False)
        print("✅ Sheet 1 Selesai!")

    # Upload Sheet 2
    ws2 = get_google_sheet(SHEET_2_NAME)
    if ws2:
        print(f"📤 Uploading ke {SHEET_2_NAME} ({len(sheet2_df)} baris)...")
        ws2.clear()
        set_with_dataframe(ws2, sheet2_df, include_index=False)
        print("✅ Sheet 2 Selesai!")

if __name__ == "__main__":
    upload_to_sheets()
