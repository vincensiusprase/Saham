import os
import json
import warnings
from datetime import datetime
import pandas as pd
import yfinance as yf
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials

warnings.filterwarnings("ignore")

# ── CONFIG ─────────────────────────────────────────────────────────────────
SPREADSHEET_ID = "11CTr-xGnEPdhozhBPsjXRyqqP_VgmCKt8cUCry2EdSc"
SHEET_1_NAME = "Harga"
SHEET_2_NAME = "Indikator"
SHEET_3_NAME = "MataUang"

# Mapping Ticker Yahoo Finance & Mata Uang
TICKERS = {
    "Indices": {
        "IHSG": {"ticker": "^JKSE", "currency": "IDR", "currency_name": "Indonesian Rupiah"},
        "HANGSENG": {"ticker": "^HSI", "currency": "HKD", "currency_name": "Hong Kong Dollar"},
        "NIKKEI": {"ticker": "^N225", "currency": "JPY", "currency_name": "Japanese Yen"},
        "SHANGHAI": {"ticker": "000001.SS", "currency": "CNY", "currency_name": "Chinese Yuan"},
        "STI_Singapore": {"ticker": "^STI", "currency": "SGD", "currency_name": "Singapore Dollar"},
        "NIFTY 50": {"ticker": "^NSEI", "currency": "INR", "currency_name": "Indian Rupee"},
        "S&P/ASX 200": {"ticker": "^AXJO", "currency": "AUD", "currency_name": "Australian Dollar"},
        "CSI 300": {"ticker": "000300.SS", "currency": "CNY", "currency_name": "Chinese Yuan"},
        "KOSPI": {"ticker": "^KS11", "currency": "KRW", "currency_name": "South Korean Won"}
    },
    "Commodities": {
        "Crude Oil": {"ticker": "CL=F", "currency": "USD", "currency_name": "US Dollar"},
        "Brent Oil": {"ticker": "BZ=F", "currency": "USD", "currency_name": "US Dollar"},
        "CPO": {"ticker": "FCPO.KL", "currency": "MYR", "currency_name": "Malaysian Ringgit"},
        "Newcastle Coal (Proxy)": {"ticker": "WHC.AX", "currency": "AUD", "currency_name": "Australian Dollar"}, 
        "Gold": {"ticker": "GC=F", "currency": "USD", "currency_name": "US Dollar"},    
        "Silver": {"ticker": "SI=F", "currency": "USD", "currency_name": "US Dollar"},      
        "Nickel (Proxy)": {"ticker": "VALE", "currency": "USD", "currency_name": "US Dollar"},          
        "Gas": {"ticker": "NG=F", "currency": "USD", "currency_name": "US Dollar"},             
        "Alumunium": {"ticker": "ALI=F", "currency": "USD", "currency_name": "US Dollar"},      
        "Copper (ETF)": {"ticker": "CPER", "currency": "USD", "currency_name": "US Dollar"},          
        "Rubber": {"ticker": "TSR20.SI", "currency": "USD", "currency_name": "US Dollar"},      
        "Tin (Proxy)": {"ticker": "MSC.KL", "currency": "MYR", "currency_name": "Malaysian Ringgit"},              
        "Zinc (Proxy)": {"ticker": "TECK", "currency": "USD", "currency_name": "US Dollar"}            
    }
}

def get_google_sheet(sheet_name):
    sa_key = os.environ.get("GCP_SA_KEY")
    if not sa_key:
        print("❌ GCP_SA_KEY tidak ditemukan di environment variables.")
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
    currency_ref_list = []
    
    for category, items in TICKERS.items():
        for name, info in items.items():
            flat_tickers[name] = {"ticker": info["ticker"], "category": category}
            currency_ref_list.append({
                "Category": category,
                "Asset_Name": name,
                "Ticker": info["ticker"],
                "Currency_Code": info["currency"],
                "Currency_Name": info["currency_name"]
            })

    all_data = []
    
    print("⏳ Mendownload data 5 tahun terakhir (Daily)...")
    for name, info in flat_tickers.items():
        ticker = info["ticker"]
        try:
            df = yf.download(ticker, period="5y", interval="1d", progress=False, auto_adjust=False)
            if df.empty:
                print(f"⚠️ Data kosong untuk {name} ({ticker})")
                continue
                
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [c[0] for c in df.columns]
                
            df.index = pd.to_datetime(df.index).tz_localize(None)
            
            # Normalisasi kolom harga
            if 'Close' not in df.columns and 'Adj Close' in df.columns:
                df['Close'] = df['Adj Close']
            
            # Indikator Teknikal
            c = df['Close']
            h = df['High'] if 'High' in df.columns else c
            l = df['Low'] if 'Low' in df.columns else c
            
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
            
            # Metadata
            df['Asset_Name'] = name
            df['Category'] = info["category"]
            df['Date'] = df.index
            
            all_data.append(df)
            print(f"✅ Berhasil memproses: {name}")
            
        except Exception as e:
            print(f"❌ Error memproses {name} ({ticker}): {e}")

    if not all_data:
        print("❌ Tidak ada data yang berhasil didownload.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    full_df = pd.concat(all_data, ignore_index=True)
    
    # ── PERBAIKAN LOGIKA PERCENTAGE CHANGE ──
    price_pivot = full_df.pivot(index='Date', columns='Asset_Name', values='Close')
    price_pivot = price_pivot.ffill()
    pct_pivot = price_pivot.pct_change() * 100
    
    # ── SHEET 1: WIDE FORMAT ──
    pct_pivot_renamed = pct_pivot.copy()
    pct_pivot_renamed.columns = [f"{col} %Chg" for col in pct_pivot_renamed.columns]
    
    sheet1_df = pd.concat([price_pivot, pct_pivot_renamed], axis=1)
    sheet1_df = sheet1_df.reindex(sorted(sheet1_df.columns), axis=1).reset_index()
    sheet1_df = sheet1_df.dropna(subset=[pct_pivot_renamed.columns[0]]) 
    sheet1_df = sheet1_df.round(2)
    sheet1_df['Date'] = sheet1_df['Date'].dt.strftime('%Y-%m-%d')

    # ── SHEET 2: LONG FORMAT ──
    pct_long = pct_pivot.reset_index().melt(id_vars='Date', var_name='Asset_Name', value_name='Pct_Change')
    full_df = pd.merge(full_df, pct_long, on=['Date', 'Asset_Name'], how='left')

    sheet2_cols = ['Date', 'Category', 'Asset_Name', 'Close', 'Pct_Change', 'EMA9', 'EMA20', 'EMA50', 'EMA200', 
                   'Tenkan_Sen', 'Kijun_Sen', 'Senkou_Span_A', 'Senkou_Span_B']
    sheet2_df = full_df[sheet2_cols].copy()
    sheet2_df['Date'] = sheet2_df['Date'].dt.strftime('%Y-%m-%d')
    sheet2_df = sheet2_df.sort_values(['Date', 'Category', 'Asset_Name'])
    sheet2_df = sheet2_df.round(2)
    sheet2_df = sheet2_df.dropna(subset=['EMA50']) 

    # ── SHEET 3: REFERENSI MATA UANG ──
    sheet3_df = pd.DataFrame(currency_ref_list)

    return sheet1_df, sheet2_df, sheet3_df

def upload_to_sheets():
    sheet1_df, sheet2_df, sheet3_df = fetch_and_process_data()
    if sheet1_df.empty: return
    
    # Upload Sheet 1
    ws1 = get_google_sheet(SHEET_1_NAME)
    if ws1:
        print(f"\n📤 Uploading ke {SHEET_1_NAME} ({len(sheet1_df)} baris)...")
        ws1.clear()
        set_with_dataframe(ws1, sheet1_df, include_index=False)
        print("✅ Sheet 1 (Harga) Selesai!")

    # Upload Sheet 2
    ws2 = get_google_sheet(SHEET_2_NAME)
    if ws2:
        print(f"📤 Uploading ke {SHEET_2_NAME} ({len(sheet2_df)} baris)...")
        ws2.clear()
        set_with_dataframe(ws2, sheet2_df, include_index=False)
        print("✅ Sheet 2 (Indikator) Selesai!")
        
    # Upload Sheet 3
    ws3 = get_google_sheet(SHEET_3_NAME)
    if ws3:
        print(f"📤 Uploading ke {SHEET_3_NAME} ({len(sheet3_df)} baris)...")
        ws3.clear()
        set_with_dataframe(ws3, sheet3_df, include_index=False)
        ws3.format("A1:E1", {"textFormat": {"bold": True}, "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}})
        print("✅ Sheet 3 (Mata Uang) Selesai!")

if __name__ == "__main__":
    upload_to_sheets()
