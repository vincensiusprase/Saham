# ==========================================
# MARKET SCANNER - PRO VERSION
# ==========================================

import yfinance as yf
import pandas as pd
import numpy as np
import pandas_ta as ta  # PERBAIKAN: Menggunakan pandas_ta, bukan ta
import gspread
from gspread_dataframe import set_with_dataframe
from datetime import datetime
import warnings
import json
import os
import time
from google.oauth2.service_account import Credentials

warnings.filterwarnings('ignore')

SPREADSHEET_ID = "1U094Atkf-3EAq5jHQceAbqPYezvJxz-L-aWx4SAiFNE"

# ==========================================
# GOOGLE SHEET CONNECTION
# ==========================================
def connect_gsheet(target_sheet_name):
    try:
        creds_json = os.environ.get("GCP_CREDENTIALS") # Menyesuaikan dengan nama secret di Github Action sebelumnya
        if not creds_json:
            print("❌ GCP_CREDENTIALS tidak ditemukan di environment.")
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
            print(f"Membuat sheet baru: {target_sheet_name}")
            worksheet = sh.add_worksheet(title=target_sheet_name, rows="300", cols="25")

        return worksheet

    except Exception as e:
        print(f"❌ Error Koneksi GSheet: {e}")
        return None


# ==========================================
# ANALYZE SINGLE STOCK
# ==========================================
def analyze_stock(ticker):
    try:
        df = yf.download(ticker, period="6mo", interval="1d", progress=False)
        if df.empty: return None
        if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.droplevel(1)
        df.dropna(inplace=True)
        if len(df) < 60: return None

        ma_periods = [10, 20, 50, 100, 200]
        for p in ma_periods:
            df[f'SMA_{p}'] = ta.sma(df['Close'], length=p)
            df[f'EMA_{p}'] = ta.ema(df['Close'], length=p)

        ichi, _ = ta.ichimoku(df['High'], df['Low'], df['Close'])
        df['ISA'], df['ISB'] = ichi['ISA_9'], ichi['ISB_26']
        df['ITS'], df['IKS'] = ichi['ITS_9'], ichi['IKS_26']

        df['RSI'] = ta.rsi(df['Close'], length=14)
        
        stoch = ta.stoch(df['High'], df['Low'], df['Close'])
        df['STOCH_K'], df['STOCH_D'] = stoch['STOCHk_14_3_3'], stoch['STOCHd_14_3_3']

        df['CCI'] = ta.cci(df['High'], df['Low'], df['Close'], length=20)
        
        adx = ta.adx(df['High'], df['Low'], df['Close'], length=14)
        df['ADX'], df['+DI'], df['-DI'] = adx['ADX_14'], adx['DMP_14'], adx['DMN_14']

        df['AO'] = ta.ao(df['High'], df['Low'])
        df['MOM'] = ta.mom(df['Close'], length=10)

        macd = ta.macd(df['Close'])
        df['MACD'], df['MACD_SIGNAL'] = macd['MACD_12_26_9'], macd['MACDs_12_26_9']

        stochrsi = ta.stochrsi(df['Close'])
        df['SRSI_K'], df['SRSI_D'] = stochrsi['STOCHRSIk_14_14_3_3'], stochrsi['STOCHRSId_14_14_3_3']

        df['WILLR'] = ta.willr(df['High'], df['Low'], df['Close'], length=14)

        df['EMA_13'] = ta.ema(df['Close'], length=13)
        eri = ta.eri(df['High'], df['Low'], df['Close'], length=13)
        df['BULLP'], df['BEARP'] = eri['BULLP_13'], eri['BEARP_13']

        df['UO'] = ta.uo(df['High'], df['Low'], df['Close'])
        df['VOL_SMA_20'] = df['Volume'].rolling(window=20).mean()

        day1, day2, day3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
        
        # --- KALKULASI SKOR TRADINGVIEW ---
        score, counted = 0, 0
        def add_score(val):
            nonlocal score, counted
            score += val
            counted += 1

        for p in ma_periods:
            if pd.notna(day3[f'SMA_{p}']): add_score(1 if day3[f'SMA_{p}'] < day3['Close'] else -1 if day3[f'SMA_{p}'] > day3['Close'] else 0)
            if pd.notna(day3[f'EMA_{p}']): add_score(1 if day3[f'EMA_{p}'] < day3['Close'] else -1 if day3[f'EMA_{p}'] > day3['Close'] else 0)

        if (day3['ISA'] > day3['ISB']) and (day3['IKS'] > day3['ISA']) and (day3['ITS'] > day3['IKS']) and (day3['Close'] > day3['ITS']): add_score(1)
        elif (day3['ISA'] < day3['ISB']) and (day3['IKS'] < day3['ISA']) and (day3['ITS'] < day3['IKS']) and (day3['Close'] < day3['ITS']): add_score(-1)
        else: add_score(0)

        if day3['RSI'] < 30 and (day3['RSI'] > day2['RSI']): add_score(1)
        elif day3['RSI'] > 70 and (day3['RSI'] < day2['RSI']): add_score(-1)
        else: add_score(0)

        if day3['STOCH_K'] < 20 and day3['STOCH_D'] < 20 and (day3['STOCH_K'] > day3['STOCH_D']): add_score(1)
        elif day3['STOCH_K'] > 80 and day3['STOCH_D'] > 80 and (day3['STOCH_K'] < day3['STOCH_D']): add_score(-1)
        else: add_score(0)

        if day3['CCI'] < -100 and (day3['CCI'] > day2['CCI']): add_score(1)
        elif day3['CCI'] > 100 and (day3['CCI'] < day2['CCI']): add_score(-1)
        else: add_score(0)

        if (day3['+DI'] > day3['-DI']) and day3['ADX'] > 20 and (day3['ADX'] > day2['ADX']): add_score(1)
        elif (day3['+DI'] < day3['-DI']) and day3['ADX'] > 20 and (day3['ADX'] > day2['ADX']): add_score(-1)
        else: add_score(0)

        ao_saucer_buy = (day3['AO'] > 0) and (day1['AO'] > day2['AO']) and (day3['AO'] > day2['AO'])
        ao_cross_buy = (day2['AO'] < 0) and (day3['AO'] > 0)
        ao_saucer_sell = (day3['AO'] < 0) and (day1['AO'] < day2['AO']) and (day3['AO'] < day2['AO'])
        ao_cross_sell = (day2['AO'] > 0) and (day3['AO'] < 0)
        
        if ao_saucer_buy or ao_cross_buy: add_score(1)
        elif ao_saucer_sell or ao_cross_sell: add_score(-1)
        else: add_score(0)

        if day3['MOM'] > day2['MOM']: add_score(1)
        elif day3['MOM'] < day2['MOM']: add_score(-1)
        else: add_score(0)

        if day3['MACD'] > day3['MACD_SIGNAL']: add_score(1)
        elif day3['MACD'] < day3['MACD_SIGNAL']: add_score(-1)
        else: add_score(0)

        tren_naik = day3['EMA_13'] > day2['EMA_13']
        if not tren_naik and day3['SRSI_K'] < 20 and day3['SRSI_D'] < 20 and (day3['SRSI_K'] > day3['SRSI_D']): add_score(1)
        elif tren_naik and day3['SRSI_K'] > 80 and day3['SRSI_D'] > 80 and (day3['SRSI_K'] < day3['SRSI_D']): add_score(-1)
        else: add_score(0)

        if day3['WILLR'] < -80 and (day3['WILLR'] > day2['WILLR']): add_score(1)
        elif day3['WILLR'] > -20 and (day3['WILLR'] < day2['WILLR']): add_score(-1)
        else: add_score(0)

        if tren_naik and day3['BEARP'] < 0 and (day3['BEARP'] > day2['BEARP']): add_score(1)
        elif tren_naik and day3['BULLP'] > 0 and (day3['BULLP'] < day2['BULLP']): add_score(-1)
        else: add_score(0)

        if day3['UO'] > 70: add_score(1)
        elif day3['UO'] < 30: add_score(-1)
        else: add_score(0)

        final_value = score / counted if counted > 0 else 0
        if -1.0 <= final_value < -0.5: rec = "Penjualan Kuat"
        elif -0.5 <= final_value < -0.1: rec = "Penjualan"
        elif -0.1 <= final_value <= 0.1: rec = "Netral"
        elif 0.1 < final_value <= 0.5: rec = "Pembelian"
        elif 0.5 < final_value <= 1.0: rec = "Pembelian Kuat"
        else: rec = "Netral"

        # --- DETEKSI POLA CANDLESTICK ---
        body_day1 = abs(day1['Close'] - day1['Open'])
        body_day2 = abs(day2['Close'] - day2['Open'])
        body_day3 = abs(day3['Close'] - day3['Open'])
        
        bull_1, bear_1 = day1['Close'] > day1['Open'], day1['Open'] > day1['Close']
        bull_2, bear_2 = day2['Close'] > day2['Open'], day2['Open'] > day2['Close']
        bull_3, bear_3 = day3['Close'] > day3['Open'], day3['Open'] > day3['Close']

        def close_near_high(row):
            length = row['High'] - row['Low']
            return (row['High'] - max(row['Open'], row['Close'])) <= (0.2 * length) if length > 0 else True

        vol_conf = (day3['Volume'] > day3['VOL_SMA_20']) or (day3['Volume'] > day2['Volume'])
        
        is_3_white_soldiers = (bull_1 and bull_2 and bull_3 and (day2['Close'] > day1['Close']) and (day3['Close'] > day2['Close']) and 
                               (day2['Open'] > day1['Open']) and (day2['Open'] <= day1['Close']) and 
                               (day3['Open'] > day2['Open']) and (day3['Open'] <= day2['Close']) and 
                               close_near_high(day1) and close_near_high(day2) and close_near_high(day3) and vol_conf)

        sm_body_2 = body_day2 <= (0.3 * max(body_day1, 0.0001))
        mid_1 = day1['Close'] + (body_day1/2) if bear_1 else day1['Open'] + (body_day1/2)
        
        is_morning_star = bear_1 and sm_body_2 and bull_3 and (day3['Close'] >= mid_1)
        is_evening_star = bull_1 and sm_body_2 and bear_3 and (day3['Close'] <= mid_1)

        is_bull_engulfing = bear_2 and bull_3 and (day3['Close'] > day2['Open']) and (day3['Open'] <= day2['Close'])
        is_bear_engulfing = bull_2 and bear_3 and (day3['Open'] >= day2['Close']) and (day3['Close'] <= day2['Open'])

        is_piercing = bear_2 and (day3['Open'] < day2['Close']) and bull_3 and (day3['Close'] >= (day2['Close'] + body_day2/2))

        low_shad3 = min(day3['Open'], day3['Close']) - day3['Low']
        up_shad3 = day3['High'] - max(day3['Open'], day3['Close'])
        
        is_hammer = (low_shad3 >= (2 * max(body_day3, 0.0001))) and (up_shad3 <= (0.5 * max(body_day3, 0.0001)))
        is_shooting_star = (up_shad3 >= (2 * max(body_day3, 0.0001))) and (low_shad3 <= (0.5 * max(body_day3, 0.0001)))

        pola = "-"
        if is_3_white_soldiers: pola = "Bullish: 3 White Soldiers"
        elif is_evening_star: pola = "Bearish: Evening Star"
        elif is_morning_star: pola = "Bullish: Morning Star"
        elif is_bear_engulfing: pola = "Bearish: Engulfing"
        elif is_bull_engulfing: pola = "Bullish: Engulfing"
        elif is_piercing: pola = "Bullish: Piercing Line"
        elif is_shooting_star: pola = "Bearish: Shooting Star"
        elif is_hammer: pola = "Bullish: Hammer"

        return {
            "Ticker": ticker.replace('.JK', ''),
            "Harga": round(day3['Close'], 0),
            "Skor TV": round(final_value, 2),
            "Rekomendasi TV": rec,
            "Pola Terdeteksi": pola,
            "Waktu": datetime.now().strftime("%Y-%m-%d %H:%M")
        }
    except Exception as e:
        print(f"Error pada {ticker}: {e}")
        return None

# ==========================================
# ANALYZE SECTOR (FUNGSI BARU YANG DITAMBAHKAN)
# ==========================================
def analyze_sector(sheet_name, saham_list):
    print(f"\nMemulai *scan* untuk sektor: {sheet_name} ({len(saham_list)} emiten)")
    results = []
    
    for ticker in saham_list:
        res = analyze_stock(ticker)
        if res:
            results.append(res)
            
    if results:
        return pd.DataFrame(results)
    else:
        return pd.DataFrame() # Return dataframe kosong jika tidak ada hasil

# ==========================================
# TEST SECTOR
# ==========================================
SECTOR_CONFIG = {
    "IDXINDUST": [
        "ASII.JK",
        "UNTR.JK",
        "IMPC.JK",
        "ARNA.JK",
        "SCCO.JK"
    ],

    "IDXNONCYC": [
        "UNVR.JK",
        "ICBP.JK",
        "INDF.JK",
        "CPIN.JK",
        "MYOR.JK"
    ],

    "IDXFINANCE": [
        "BBCA.JK",
        "BBRI.JK",
        "BMRI.JK",
        "BBNI.JK",
        "BRIS.JK"
    ],

    "IDXCYCLIC": [
        "ACES.JK",
        "ERAA.JK",
        "MAPI.JK",
        "AUTO.JK",
        "SMSM.JK"
    ],

    "IDXTECHNO": [
        "GOTO.JK",
        "BUKA.JK",
        "EMTK.JK",
        "DCII.JK",
        "MTDL.JK"
    ],

    "IDXBASIC": [
        "ANTM.JK",
        "MDKA.JK",
        "TPIA.JK",
        "BRPT.JK",
        "INKP.JK"
    ],

    "IDXENERGY": [
        "ADRO.JK",
        "PTBA.JK",
        "ITMG.JK",
        "MEDC.JK",
        "PGAS.JK"
    ],

    "IDXHEALTH": [
        "KLBF.JK",
        "SIDO.JK",
        "MIKA.JK",
        "SILO.JK",
        "KAEF.JK"
    ],

    "IDXINFRA": [
        "TLKM.JK",
        "JSMR.JK",
        "PGEO.JK",
        "TOWR.JK",
        "ISAT.JK"
    ],

    "IDXPROPERT": [
        "BSDE.JK",
        "CTRA.JK",
        "PWON.JK",
        "SMRA.JK",
        "DMAS.JK"
    ],

    "IDXTRANS": [
        "SMDR.JK",
        "ASSA.JK",
        "TMAS.JK",
        "BIRD.JK",
        "GIAA.JK"
    ]
}

# ==========================================
# MAIN EXECUTION
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

        # Jeda 2 detik antar sheet untuk mencegah limit API Google
        time.sleep(2)

    print("🏁 SEMUA SEKTOR SELESAI DIPROSES 🏁")
