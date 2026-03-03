# ==========================================
# MARKET SCANNER - PRO VERSION
# ==========================================

import yfinance as yf
import pandas as pd
import numpy as np
import ta
import gspread
from gspread_dataframe import set_with_dataframe
from datetime import datetime, timezone, timedelta
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
        creds_json = os.environ.get("GCP_SA_KEY") # Menyesuaikan dengan nama secret di Github Action sebelumnya
        if not creds_json:
            print("❌ GCP_SA_KEY tidak ditemukan di environment.")
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
# ANALYZE SINGLE STOCK (VERSI LIBRARY 'ta')
# ==========================================
def analyze_stock(ticker):
    try:
        df = yf.download(ticker, period="6mo", interval="1d", progress=False)
        if df.empty: return None
        if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.droplevel(1)
        df.dropna(inplace=True)
        if len(df) < 60: return None

        # --- KALKULASI INDIKATOR MENGGUNAKAN LIBRARY 'ta' ---
        
        # 1. Moving Averages
        ma_periods = [10, 20, 50, 100, 200]
        for p in ma_periods:
            df[f'SMA_{p}'] = ta.trend.sma_indicator(df['Close'], window=p)
            df[f'EMA_{p}'] = ta.trend.ema_indicator(df['Close'], window=p)

        # 2. Ichimoku Cloud
        ichi = ta.trend.IchimokuIndicator(high=df['High'], low=df['Low'], window1=9, window2=26, window3=52)
        df['ISA'] = ichi.ichimoku_a()
        df['ISB'] = ichi.ichimoku_b()
        df['ITS'] = ichi.ichimoku_conversion_line()
        df['IKS'] = ichi.ichimoku_base_line()

        # 3. RSI
        df['RSI'] = ta.momentum.rsi(df['Close'], window=14)
        
        # 4. Stochastic
        stoch = ta.momentum.StochasticOscillator(high=df['High'], low=df['Low'], close=df['Close'], window=14, smooth_window=3)
        df['STOCH_K'] = stoch.stoch()
        df['STOCH_D'] = stoch.stoch_signal()

        # 5. CCI
        df['CCI'] = ta.trend.cci(high=df['High'], low=df['Low'], close=df['Close'], window=20)
        
        # 6. ADX (+DI, -DI)
        adx_ind = ta.trend.ADXIndicator(high=df['High'], low=df['Low'], close=df['Close'], window=14)
        df['ADX'] = adx_ind.adx()
        df['+DI'] = adx_ind.adx_pos()
        df['-DI'] = adx_ind.adx_neg()

        # 7. Awesome Oscillator
        df['AO'] = ta.momentum.awesome_oscillator(high=df['High'], low=df['Low'], window1=5, window2=34)
        
        # 8. Momentum (TradingView menggunakan Close hari ini dikurangi Close 10 hari lalu)
        df['MOM'] = df['Close'].diff(10)

        # 9. MACD
        macd_ind = ta.trend.MACD(close=df['Close'], window_slow=26, window_fast=12, window_sign=9)
        df['MACD'] = macd_ind.macd()
        df['MACD_SIGNAL'] = macd_ind.macd_signal()

        # 10. Stochastic RSI (Library 'ta' menghasilkan skala 0-1, kita kali 100 agar cocok dengan skor TV)
        stochrsi_ind = ta.momentum.StochRSIIndicator(close=df['Close'], window=14, smooth1=3, smooth2=3)
        df['SRSI_K'] = stochrsi_ind.stochrsi_k() * 100
        df['SRSI_D'] = stochrsi_ind.stochrsi_d() * 100

        # 11. Williams %R
        df['WILLR'] = ta.momentum.williams_r(high=df['High'], low=df['Low'], close=df['Close'], lbp=14)

        # 12. Bull & Bear Power (Elder Ray) - Dihitung manual menggunakan EMA 13
        df['EMA_13'] = ta.trend.ema_indicator(df['Close'], window=13)
        df['BULLP'] = df['High'] - df['EMA_13']
        df['BEARP'] = df['Low'] - df['EMA_13']

        # 13. Ultimate Oscillator
        df['UO'] = ta.momentum.ultimate_oscillator(high=df['High'], low=df['Low'], close=df['Close'], window1=7, window2=14, window3=28)
        
        # Volume MA
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
        
        # Syarat Morning Star
        is_morning_star = (
        bear_1 and                               # Candle 1: Merah (Bearish)
        sm_body_2 and                            # Candle 2: Kecil (Bintang)
        (day2['Low'] < day1['Low']) and          # Syarat: Bintang harus membuat Low lebih rendah dari kemarin
        bull_3 and                               # Candle 3: Hijau (Bullish)
        (day3['Close'] >= mid_1) and             # Candle 3: Menembus setengah body Candle 1
        (day3['Close'] < day3['SMA_50'])         # KONTEKS: Biasanya lebih akurat jika muncul di area bawah (di bawah SMA 50)
        )

        # Syarat Evening Star
        is_evening_star = (
        bull_1 and                               # Candle 1: Hijau (Bullish)
        sm_body_2 and                            # Candle 2: Kecil (Bintang)
        (day2['High'] > day1['High']) and        # Syarat: Bintang harus melompat lebih tinggi (Euphoria)
        bear_3 and                               # Candle 3: Merah (Bearish)
        (day3['Close'] <= mid_1) and             # Candle 3: Menembus ke bawah setengah body Candle 1
        (day3['Close'] > day3['SMA_50'])         # KONTEKS: Hanya valid jika harga masih di area atas/puncak
        )

        is_bull_engulfing = bear_2 and bull_3 and (day3['Close'] > day2['Open']) and (day3['Open'] <= day2['Close'])
        is_bear_engulfing = bull_2 and bear_3 and (day3['Open'] >= day2['Close']) and (day3['Close'] <= day2['Open'])

        # Menghitung titik tengah body merah secara lebih akurat
        mid_point = (day2['Open'] + day2['Close']) / 2

        # Syarat Piercing Line yang Diperketat:
        is_piercing = (
        bear_2 and bull_3 and 
        (day3['Open'] < day2['Low']) and         # Syarat tambahan: Open harus gap down di bawah Low kemarin
        (day3['Close'] > mid_point) and          # Harus tutup DI ATAS titik tengah
        (day3['Close'] < day2['Open'])           # Tapi tidak boleh menelan seluruh body (supaya tidak jadi Engulfing)
        )

        low_shad3 = min(day3['Open'], day3['Close']) - day3['Low']
        up_shad3 = day3['High'] - max(day3['Open'], day3['Close'])
        
        is_hammer = (low_shad3 >= (2 * max(body_day3, 0.0001))) and (up_shad3 <= (0.5 * max(body_day3, 0.0001)))
        is_shooting_star = (up_shad3 >= (2 * max(body_day3, 0.0001))) and (low_shad3 <= (0.5 * max(body_day3, 0.0001)))

        mid_2 = day2['Open'] + (body_day2/2) if bull_2 else day2['Close'] + (body_day2/2)
        is_dark_cloud = bull_2 and bear_3 and (day3['Open'] > day2['Close']) and (day3['Close'] <= mid_2) and (day3['Close'] >= day2['Open'])

        is_bull_harami = bear_2 and bull_3 and (day3['Open'] >= day2['Close']) and (day3['Close'] <= day2['Open']) and (body_day3 <= (0.5 * body_day2))

        is_bear_harami = bull_2 and bear_3 and (day3['Open'] <= day2['Close']) and (day3['Close'] >= day2['Open']) and (body_day3 <= (0.5 * body_day2))

        def close_near_low(row):
            length = row['High'] - row['Low']
            return (min(row['Open'], row['Close']) - row['Low']) <= (0.2 * length) if length > 0 else True

        is_3_black_crows = (bear_1 and bear_2 and bear_3 and 
                            (day2['Close'] < day1['Close']) and (day3['Close'] < day2['Close']) and 
                            (day2['Open'] < day1['Open']) and (day2['Open'] >= day1['Close']) and 
                            (day3['Open'] < day2['Open']) and (day3['Open'] >= day2['Close']) and 
                            close_near_low(day1) and close_near_low(day2) and close_near_low(day3))
                
        pola = "-"
        # Pola 3 Candle
        if is_3_white_soldiers: pola = "Bullish: 3 White Soldiers"
        elif is_3_black_crows: pola = "Bearish: 3 Black Crows"
        elif is_evening_star: pola = "Bearish: Evening Star"
        elif is_morning_star: pola = "Bullish: Morning Star"
        
        # Pola 2 Candle
        elif is_bear_engulfing: pola = "Bearish: Engulfing"
        elif is_bull_engulfing: pola = "Bullish: Engulfing"
        elif is_dark_cloud: pola = "Bearish: Dark Cloud Cover"
        elif is_piercing: pola = "Bullish: Piercing Line"
        elif is_bear_harami: pola = "Bearish: Harami"
        elif is_bull_harami: pola = "Bullish: Harami"
        
        # Pola 1 Candle
        elif is_shooting_star: pola = "Bearish: Shooting Star"
        elif is_hammer: pola = "Bullish: Hammer"

        return {
            "Ticker": ticker,
            "Harga": round(day3['Close'], 0),
            "Skor TV": round(final_value, 2),
            "Rekomendasi TV": rec,
            "Pola Terdeteksi": pola,
            "Waktu": datetime.now(timezone(timedelta(hours=7))).strftime("%Y-%m-%d %H:%M")
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
        "ASII.JK", "UNTR.JK", "PIPA.JK", "BNBR.JK", "HEXA.JK", "IMPC.JK", "MHKI.JK", "LABA.JK", "SMIL.JK", "NTBK.JK", 
        "PADA.JK", "NAIK.JK", "TOTO.JK", "BHIT.JK", "MARK.JK", "INDX.JK", "PTMP.JK", "ARNA.JK", "KOBX.JK", "ASGR.JK", 
        "GPSO.JK", "SINI.JK", "HOPE.JK", "FOLK.JK", "KUAS.JK", "KBLI.JK", "SPTO.JK", "IKAI.JK", "SKRN.JK", "MUTU.JK", 
        "CTTH.JK", "CAKK.JK", "CCSI.JK", "MLIA.JK", "JTPE.JK", "BLUE.JK", "CRSN.JK", "DYAN.JK", "LION.JK", "MDRN.JK", 
        "VOKS.JK", "SCCO.JK", "KBLM.JK", "JECC.JK", "INTA.JK", "BINO.JK", "VISI.JK", "ARKA.JK", "SOSS.JK", "IKBI.JK", 
        "AMIN.JK", "ICON.JK", "KONI.JK", "AMFG.JK", "IBFN.JK", "ZBRA.JK", "TIRA.JK", "KIAS.JK", "APII.JK", "HYGN.JK", 
        "KOIN.JK", "MFMI.JK", "TRIL.JK"
    ],
    "IDXNONCYC": [
        "UNVR.JK", "INDF.JK", "ICBP.JK", "GGRM.JK", "AALI.JK", "JPFA.JK", "MYOR.JK", "CPIN.JK", "RLCO.JK", "LSIP.JK", 
        "HMSD.JK", "AMRT.JK", "GZCO.JK", "MLPL.JK", "JARR.JK", "BWPT.JK", "ULTJ.JK", "FORE.JK", "TAPG.JK", "CPRO.JK", 
        "SIMP.JK", "CLEO.JK", "WIIM.JK", "MPPA.JK", "HOKI.JK", "COCO.JK", "DSNG.JK", "IKAN.JK", "SMAR.JK", "BRRC.JK", 
        "PTPS.JK", "SSMS.JK", "GOOD.JK", "PGUN.JK", "SGRO.JK", "BEEF.JK", "BTEK.JK", "MAIN.JK", "MIDI.JK", "AYAM.JK", 
        "WMUU.JK", "DEWI.JK", "ASHA.JK", "CMRY.JK", "AISA.JK", "CAMP.JK", "OILS.JK", "ANJT.JK", "TBLA.JK", "ROTI.JK", 
        "NASI.JK", "ITIC.JK", "DSFI.JK", "ISEA.JK", "JAWA.JK", "NSSS.JK", "CSRA.JK", "STAA.JK", "KEJU.JK", "UNSP.JK", 
        "MRAT.JK", "DPUM.JK", "PSDN.JK", "STRK.JK", "WAPO.JK", "MBTO.JK", "GULA.JK", "LAPD.JK", "RANC.JK", "NEST.JK", 
        "TAYS.JK", "CEKA.JK", "ADES.JK", "KINO.JK", "BISI.JK", "ANDI.JK", "PMMP.JK", "DMND.JK", "UCID.JK", "YUPI.JK", 
        "BEER.JK", "PSGO.JK", "BUDI.JK", "SKBM.JK", "MLBI.JK", "TLDN.JK", "FOOD.JK", "MKTR.JK", "SKLT.JK", "MAXI.JK", 
        "STTP.JK", "PNGO.JK", "PCAR.JK", "GUNA.JK", "EPMT.JK", "DLTA.JK", "HERO.JK", "TGUK.JK", "VICI.JK", "MSJA.JK", 
        "SDPC.JK", "MGRO.JK", "FISH.JK", "BOBA.JK", "KMDS.JK", "TCID.JK", "BUAH.JK", "TGKA.JK", "WINE.JK", "ENZO.JK",
        "CBUT.JK", "WMPP.JK", "IPPE.JK", "DAYA.JK", "SIPD.JK", "CRAB.JK", "FAPA.JK", "AGAR.JK", "TRGU.JK", "ALTO.JK", 
        "MAGP.JK", "GOLL.JK", "WICO.JK"
    ],
   "IDXFINANCE": [
    "BBCA.JK", "BBRI.JK", "BMRI.JK", "BBNI.JK", "BRIS.JK", "SUPA.JK", "COIN.JK", "BBTN.JK", "ARTO.JK", "BBYB.JK", "BNGA.JK", "BBKP.JK",
    "BTPS.JK", "BJTM.JK", "SRTG.JK", "PNLF.JK", "PADI.JK", "AGRO.JK", "NISP.JK", "INPC.JK", "BJBR.JK", "BBHI.JK", "BFIN.JK", "BDMN.JK",
    "BABP.JK", "PNBS.JK", "BGTG.JK", "AHAP.JK", "BANK.JK", "BACA.JK", "BNLI.JK", "BNII.JK", "BCAP.JK", "PNBN.JK", "MEGA.JK", "BVIC.JK",
    "ADMF.JK", "DNAR.JK", "MAYA.JK", "CFIN.JK", "BTPN.JK", "BSIM.JK", "BEKS.JK", "TUGU.JK", "PEGE.JK", "NOBU.JK", "PALM.JK", "BNBA.JK",
    "LPPS.JK", "AGRS.JK", "DNET.JK", "AMAR.JK", "GSMF.JK", "JMAS.JK", "TRIM.JK", "MCOR.JK", "PNIN.JK", "SMMA.JK", "PANS.JK", "BKSW.JK",
    "VINS.JK", "BCIC.JK", "BINA.JK", "WOMF.JK", "LPGI.JK", "LIFE.JK", "VTNY.JK", "VICO.JK", "STAR.JK", "YOII.JK", "FUJI.JK", "MTWI.JK",
    "POLA.JK", "BBSI.JK", "ASJT.JK", "SDRA.JK", "BMAS.JK", "AMAG.JK", "ASMI.JK", "HDFA.JK", "VRNA.JK", "AMOR.JK", "APIC.JK", "MREI.JK",
    "ASDM.JK", "TIFA.JK", "BHIT.JK", "ASRM.JK", "RELI.JK", "NICK.JK", "TRUS.JK", "ASBI.JK", "DEFI.JK", "BBLD.JK", "BBMD.JK", "MASB.JK",
    "BPFI.JK", "YULE.JK", "BPII.JK", "POOL.JK", "BSWD.JK", "SFAN.JK", "ABDA.JK", "OCAP.JK", "PLAS.JK"
    ],
    "IDXCYCLIC": [
    "MNCN.JK", "SCMA.JK", "LPPF.JK", "MINA.JK", "BUVA.JK", "ACES.JK", "ERAA.JK", "HRTA.JK", "FUTR.JK", "MAPI.JK", "AUTO.JK", "GJTL.JK",
    "FAST.JK", "VKTR.JK", "DOOH.JK", "BMTR.JK", "MPMX.JK", "FILM.JK", "RALS.JK", "KPIG.JK", "MAPA.JK", "SLIS.JK", "ZATA.JK", "SMSM.JK",
    "JGLE.JK", "ASLC.JK", "IMAS.JK", "MERI.JK", "NETV.JK", "KAQI.JK", "CNMA.JK", "MSIN.JK", "WOOD.JK", "BELL.JK", "PSKT.JK", "VIVA.JK",
    "MSKY.JK", "BABY.JK", "YELO.JK", "IPTV.JK", "TMPO.JK", "JIHD.JK", "DOSS.JK", "PMUI.JK", "SRIL.JK", "ERAL.JK", "DRMA.JK", "GOLF.JK",
    "ESTA.JK", "DFAM.JK", "PBRX.JK", "PZZA.JK", "BAIK.JK", "MDIA.JK", "CARS.JK", "ABBA.JK", "GEMA.JK", "PART.JK", "SWID.JK", "EAST.JK",
    "MARI.JK", "UNTD.JK", "KDTN.JK", "ACRO.JK", "ERTX.JK", "VERN.JK", "BOLA.JK", "KOTA.JK", "MDIY.JK", "FITT.JK", "TOOL.JK", "INDR.JK",
    "LIVE.JK", "PJAA.JK", "RAAM.JK", "INOV.JK", "CINT.JK", "KICI.JK", "FORU.JK", "ECII.JK", "GRPH.JK", "PANR.JK", "NATO.JK", "LPIN.JK",
    "CSMI.JK", "TRIS.JK", "UFOE.JK", "BOGA.JK", "SSTM.JK", "MGNA.JK", "DEPO.JK", "ESTI.JK", "POLU.JK", "SOTS.JK", "INDS.JK", "RAFI.JK",
    "BAYU.JK", "TOYS.JK", "GDYR.JK", "SONA.JK", "MAPB.JK", "PGLI.JK", "BAUT.JK", "GWSA.JK", "HRME.JK", "BIKE.JK", "DIGI.JK", "JSPT.JK",
    "MICE.JK", "LMPI.JK", "CSAP.JK", "BIMA.JK", "POLY.JK", "SHID.JK", "PTSP.JK", "SBAT.JK", "SCNP.JK", "RICY.JK", "BRAM.JK", "ENAK.JK",
    "PMJS.JK", "SNLK.JK", "TELE.JK", "BATA.JK", "ARGO.JK", "ZONE.JK", "BOLT.JK", "PNSE.JK", "DUCK.JK", "TYRE.JK", "CLAY.JK", "ARTA.JK",
    "IIKP.JK", "PDES.JK", "CBMF.JK", "BLTZ.JK", "HOME.JK", "TFCO.JK", "GLOB.JK", "AKKU.JK", "MYTX.JK", "CNTX.JK", "UNIT.JK", "TRIO.JK",
    "NUSA.JK", "HOTL.JK", "MABA.JK"
    ],
    "IDXTECHNO": [
    "GOTO.JK", "WIFI.JK", "EMTK.JK", "BUKA.JK", "WIRG.JK", "DCII.JK", "IOTF.JK", "MTDL.JK", "ELIT.JK", "MLPT.JK", "DMMX.JK",
    "TOSK.JK", "JATI.JK", "KIOS.JK", "IRSX.JK", "UVCR.JK", "TRON.JK", "KREN.JK", "CYBR.JK", "LUCK.JK", "PTSN.JK", "HDIT.JK",
    "EDGE.JK", "DIVA.JK", "TFAS.JK", "ZYRX.JK", "MSTI.JK", "MCAS.JK", "MPIX.JK", "BELI.JK", "AXIO.JK", "AWAN.JK", "AREA.JK",
    "NFCX.JK", "ATIC.JK", "TECH.JK", "GLVA.JK", "ENVY.JK", "LMAS.JK", "SKYB.JK"
    ],
    "IDXBASIC": [
    "ANTM.JK", "BRMS.JK", "SMGR.JK", "BRPT.JK", "INTP.JK", "EMAS.JK", "MDKA.JK", "INCO.JK", "TINS.JK", "ARCI.JK", "TPIA.JK",
    "MBMA.JK", "INKP.JK", "PSAB.JK", "NCKL.JK", "AMMN.JK", "ESSA.JK", "TKIM.JK", "KRAS.JK", "DKFT.JK", "NICL.JK", "FPNI.JK",
    "WSBP.JK", "SMBR.JK", "WTON.JK", "SMGA.JK", "AGII.JK", "AVIA.JK", "NIKL.JK", "SOLA.JK", "ISSP.JK", "MINE.JK", "DAAZ.JK",
    "OKAS.JK", "OPMS.JK", "BAJA.JK", "NICE.JK", "CHEM.JK", "ZINC.JK", "PPRI.JK", "AYLS.JK", "SRSN.JK", "EKAD.JK", "PBID.JK",
    "PICO.JK", "ESIP.JK", "CITA.JK", "MOLI.JK", "GDST.JK", "SULI.JK", "TIRT.JK", "MDKI.JK", "ADMG.JK", "SPMA.JK", "SMLE.JK",
    "CLPI.JK", "ASPR.JK", "NPGF.JK", "BLES.JK", "BATR.JK", "DGWG.JK", "GGRP.JK", "FWCT.JK", "TBMS.JK", "PDPP.JK", "LTLS.JK",
    "SAMF.JK", "BMSR.JK", "BEBS.JK", "SBMA.JK", "PTMR.JK", "IPOL.JK", "UNIC.JK", "OBMD.JK", "KAYU.JK", "SMCB.JK", "IGAR.JK",
    "INCI.JK", "INCF.JK", "EPAC.JK", "INAI.JK", "ALDO.JK", "HKMU.JK", "SQMI.JK", "SMKL.JK", "IFII.JK", "IFSH.JK", "PURE.JK",
    "SWAT.JK", "BTON.JK", "TALF.JK", "KDSI.JK", "INRU.JK", "CMNT.JK", "INTD.JK", "ALKA.JK", "KMTR.JK", "CTBN.JK", "YPAS.JK",
    "KKES.JK", "AKPI.JK", "DPNS.JK", "APLI.JK", "TRST.JK", "BRNA.JK", "LMSH.JK","ALMI.JK","FASW.JK","ETWA.JK","TDPM.JK","SIMA.JK","KBRI.JK"
    ],
    "IDXENERGY": [
    "ADRO.JK", "BUMI.JK", "PGAS.JK", "PTBA.JK", "ITMG.JK", "DEWA.JK", "CUAN.JK", "HRUM.JK", "PTRO.JK", "RAJA.JK", "MEDC.JK", "ADMR.JK",
    "HUMI.JK", "ENRG.JK", "BULL.JK", "TOBA.JK", "AADI.JK", "RATU.JK", "CBRE.JK", "INDY.JK", "AKRA.JK", "ELSA.JK", "GTSI.JK", "BIPI.JK",
    "COAL.JK", "BSSR.JK", "LEAD.JK", "APEX.JK", "TEBE.JK", "ATLA.JK", "SOCI.JK", "FIRE.JK", "PSAT.JK", "GEMS.JK", "DOID.JK", "DSSA.JK",
    "SGER.JK", "IATA.JK", "BBRM.JK", "BYAN.JK", "ABMM.JK", "TPMA.JK", "MAHA.JK", "BOAT.JK", "KKGI.JK", "MBSS.JK", "WOWS.JK", "CGAS.JK",
    "RMKE.JK", "WINS.JK", "MTFN.JK", "MBAP.JK", "UNIQ.JK", "RMKO.JK", "SMMT.JK", "SICO.JK", "BSML.JK", "PSSI.JK", "DWGL.JK", "TAMU.JK",
    "ALII.JK", "ITMA.JK", "RUIS.JK", "CNKO.JK", "TCPI.JK", "HILL.JK", "BOSS.JK", "PKPK.JK", "MYOH.JK", "SEMA.JK", "ARII.JK", "GTBO.JK",
    "MCOL.JK", "RGAS.JK", "SHIP.JK", "BESS.JK", "RIGS.JK", "JSKY.JK", "KOPI.JK", "PTIS.JK", "CANI.JK", "ARTI.JK", "INPS.JK", "MKAP.JK",
    "AIMS.JK", "HITS.JK", "SUNI.JK", "TRAM.JK", "SURE.JK", "SMRU.JK", "SUGI.JK"
    ],
    "IDXHEALTH": [
    "KLBF.JK", "SIDO.JK", "KAEF.JK", "PYFA.JK", "MIKA.JK", "DKHH.JK", "SILO.JK", "HEAL.JK", "TSPC.JK", "INAF.JK", "CHEK.JK", "IRRA.JK", "SAME.JK", "MEDS.JK", "PRDA.JK", "MDLA.JK", "SURI.JK", "PRIM.JK", "HALO.JK", "OBAT.JK", "CARE.JK",
    "MERK.JK", "DGNS.JK", "SOHO.JK", "BMHS.JK", "PEHA.JK", "SRAJ.JK", "MMIX.JK", "DVLA.JK", "OMED.JK", "PEVE.JK", "LABS.JK", "RSCH.JK", "MTMH.JK", "IKPM.JK", "PRAY.JK", "SCPI.JK", "RSGK.JK"
    ],
    "IDXINFRA": [
    "TLKM.JK", "CDIA.JK", "ADHI.JK", "JSMR.JK", "WIKA.JK", "PTPP.JK", "INET.JK", "WSKT.JK", "BREN.JK", "PGEO.JK", "EXCL.JK", "ISAT.JK", "TOWR.JK", "SSIA.JK", "DATA.JK", "OASA.JK", "PPRE.JK", "TBIG.JK", "POWR.JK", "NRCA.JK", "WEGE.JK", "TOTL.JK",
    "KETR.JK", "IPCC.JK", "KOKA.JK", "KBLV.JK", "MTEL.JK", "CENT.JK", "KRYA.JK", "GMFI.JK", "JAST.JK", "KEEN.JK", "JKON.JK", "ACST.JK", "ASLI.JK", "PBSA.JK", "IPCM.JK", "MORA.JK", "ARKO.JK", "MPOW.JK", "CMNP.JK", "LINK.JK", "HGII.JK", "DGIK.JK", "BDKR.JK",
    "META.JK", "KARW.JK", "CASS.JK", "BUKK.JK", "TGRA.JK", "GOLD.JK", "BALI.JK", "PTDU.JK", "IDPR.JK", "PORT.JK", "TOPS.JK", "HADE.JK", "TAMA.JK", "BTEL.JK", "GHON.JK", "SUPR.JK", "MTPS.JK", "RONY.JK", "IBST.JK", "LCKM.JK", "PTPW.JK", "MTRA.JK"
    ],
    "IDXPROPERT": [
    "CTRA.JK", "BSDE.JK", "PWON.JK", "SMRA.JK", "KLJA.JK", "PANI.JK", "BKSL.JK", "DADA.JK", "CBDK.JK", "DMAS.JK", "ASRI.JK", "LPKR.JK", "BSBK.JK", "REAL.JK", "ELTY.JK", "APLN.JK", "TRUE.JK", "TRIN.JK", "UANG.JK", "CSIS.JK", "DILD.JK", "KOCI.JK", "BEST.JK",
    "LAND.JK", "DUTI.JK", "EMDE.JK", "LPLI.JK", "GRIA.JK", "VAST.JK", "BAPI.JK", "MTLA.JK", "SAGE.JK", "BBSS.JK", "HOMI.JK", "PUDP.JK", "RBMS.JK", "URBN.JK", "TARA.JK", "CBPE.JK", "MPRO.JK", "RODA.JK", "SATU.JK", "NASA.JK", "FMII.JK", "BKDP.JK", "GMTD.JK",
    "PPRO.JK", "BAPA.JK", "PAMG.JK", "MMLP.JK", "PURI.JK", "GPRA.JK", "LPCK.JK", "MDLN.JK", "BCIP.JK", "ADCP.JK", "CITY.JK", "RISE.JK", "WINR.JK", "JRPT.JK", "AMAN.JK", "SMDM.JK", "INDO.JK", "ATAP.JK", "ASPI.JK", "KSIX.JK", "KBAG.JK", "NZIA.JK",
    "NIRO.JK", "DART.JK", "BIPP.JK", "PLIN.JK", "RDTX.JK", "ROCK.JK", "MKPI.JK", "INPP.JK", "MTSM.JK", "POLL.JK", "POLI.JK", "OMRE.JK", "GAMA.JK", "POSA.JK", "BIKA.JK", "CPRI.JK", "ARMY.JK", "COWL.JK", "RIMO.JK", "LCGP.JK"
    ],
    "IDXTRANS": [
    "PJHB.JK", "GIAA.JK", "SMDR.JK", "BIRD.JK", "BLOG.JK", "IMJS.JK", "ASSA.JK", "TMAS.JK", "LAJU.JK", "HAIS.JK", "KLAS.JK", "MITI.JK", "JAYA.JK", "NELY.JK", "WEHA.JK", "TNCA.JK", "CMPP.JK", "MPXL.JK", "KJEN.JK", "SDMU.JK", "TRUK.JK", "PURA.JK", "HATM.JK",
    "TAXI.JK", "ELPI.JK", "AKSI.JK", "GTRA.JK", "TRJA.JK", "MIRA.JK", "BLTA.JK", "SAPX.JK", "SAFE.JK", "LRNA.JK", "DEAL.JK", "BPTR.JK", "HELI.JK"
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
