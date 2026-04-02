# ==========================================
# MARKET SCANNER - PRO (KELTNER + VWAP + HA + UT BOT + RUBBER BAND)
# ==========================================

import numpy as np
import yfinance as yf
import pandas as pd
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
SPREADSHEET_ID = "1CVHTapflLP1Lypr_Q1KXC0I9qPHCnpDYNKHdYx31kh0"

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
# ANALYZE FUNCTION (NATIVE MATH CALCULATIONS)
# ==========================================
def analyze_sector(sector_name, ticker_list):

    tz_jkt = pytz.timezone("Asia/Jakarta")
    waktu_update = datetime.now(tz_jkt).strftime("%Y-%m-%d %H:%M:%S")
    
    results = []
    print(f"\n🚀 Scan {sector_name} | Total: {len(ticker_list)} saham")

    for ticker in ticker_list:
        try:
            # Download data
            df = yf.download(ticker, period="60d", interval="1d", progress=False, auto_adjust=True, threads=False)

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            if df.empty or len(df) < 30: 
                continue

            # ==============================
            # 1. KELTNER CHANNEL (TradingView Exact Match)
            # ==============================
            df['EMA_20'] = df['Close'].ewm(span=20, adjust=False).mean()
            
            high_low = df['High'] - df['Low']
            high_close = np.abs(df['High'] - df['Close'].shift(1))
            low_close = np.abs(df['Low'] - df['Close'].shift(1))
            true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)

            # ATR 10 menggunakan RMA Smoothing
            df['ATR_10'] = true_range.ewm(alpha=1/10, adjust=False).mean()

            df['KCUe_20_2'] = df['EMA_20'] + (2.0 * df['ATR_10'])
            df['KCLe_20_2'] = df['EMA_20'] - (2.0 * df['ATR_10'])
            
            # INI BARIS YANG TERTINGGAL SEBELUMNYA:
            df['KCMa_20_2'] = df['EMA_20'] 

            # ==============================
            # 2. VWAP BANDS (ANCHOR: WEEKLY)
            # ==============================
            if df.index.tz is not None:
                df.index = df.index.tz_localize(None)

            df['Week'] = df.index.to_period('W')
            df['TP'] = (df['High'] + df['Low'] + df['Close']) / 3
            df['TPV'] = df['TP'] * df['Volume']

            df['Cum_TPV'] = df.groupby('Week')['TPV'].cumsum()
            df['Cum_Vol'] = df.groupby('Week')['Volume'].cumsum()
            df['VWAP'] = df['Cum_TPV'] / df['Cum_Vol']

            df['Dev'] = df['TP'] - df['VWAP']
            df['Dev_Sq_Vol'] = (df['Dev'] ** 2) * df['Volume']
            df['Cum_Dev_Sq_Vol'] = df.groupby('Week')['Dev_Sq_Vol'].cumsum()
            df['VWAP_Stdev'] = np.sqrt(df['Cum_Dev_Sq_Vol'] / df['Cum_Vol'])

            df['VWAP_Upper'] = df['VWAP'] + (2.0 * df['VWAP_Stdev'])
            df['VWAP_Lower'] = df['VWAP'] - (2.0 * df['VWAP_Stdev'])

            # ==============================
            # 3. HEIKIN ASHI CANDLES
            # ==============================
            df['HA_Close'] = (df['Open'] + df['High'] + df['Low'] + df['Close']) / 4
            
            ha_open = np.zeros(len(df))
            ha_open[0] = (df['Open'].iloc[0] + df['Close'].iloc[0]) / 2
            for i in range(1, len(df)):
                ha_open[i] = (ha_open[i-1] + df['HA_Close'].iloc[i-1]) / 2
            df['HA_Open'] = ha_open
            
            ha_status = "🟢 BULL (Hijau)" if df['HA_Close'].iloc[-1] > df['HA_Open'].iloc[-1] else "🔴 BEAR (Merah)"

            # ==============================
            # 4. UT BOT ALGORITHM (Key: 1, ATR: 10)
            # ==============================
            df['nLoss'] = 1.0 * df['ATR_10'] 
            
            trail_stop = np.zeros(len(df))
            trend = np.zeros(len(df))
            
            closes = df['Close'].values
            nLosses = df['nLoss'].values
            
            trail_stop[0] = closes[0]
            trend[0] = 1
            
            for i in range(1, len(df)):
                if np.isnan(nLosses[i]):
                    trail_stop[i] = closes[i]
                    trend[i] = 1
                    continue
                    
                prev_trail = trail_stop[i-1]
                prev_trend = trend[i-1]
                curr_close = closes[i]
                curr_nloss = nLosses[i]
                
                if prev_trend == 1:
                    if curr_close > prev_trail:
                        trail_stop[i] = max(prev_trail, curr_close - curr_nloss)
                        trend[i] = 1
                    else:
                        trail_stop[i] = curr_close + curr_nloss
                        trend[i] = -1
                else:
                    if curr_close < prev_trail:
                        trail_stop[i] = min(prev_trail, curr_close + curr_nloss)
                        trend[i] = -1
                    else:
                        trail_stop[i] = curr_close - curr_nloss
                        trend[i] = 1
                        
            df['UT_Trend'] = trend
            
            trend_now = trend[-1]
            trend_prev = trend[-2]
            
            if trend_now == 1 and trend_prev == -1:
                ut_signal = "🟢 BUY"
            elif trend_now == -1 and trend_prev == 1:
                ut_signal = "🔴 SELL"
            elif trend_now == 1:
                ut_signal = "🔼 Hold BUY"
            else:
                ut_signal = "🔽 Hold SELL"

            # ==============================
            # 5. EKSTRAKSI DATA & LOGIKA SCORING (RUBBER BAND)
            # ==============================
            price_today = float(df["Close"].iloc[-1])
            upper_kc = float(df['KCUe_20_2'].iloc[-1])
            middle_kc = float(df['KCMa_20_2'].iloc[-1]) 
            lower_kc = float(df['KCLe_20_2'].iloc[-1])
            
            vwap_today = float(df['VWAP'].iloc[-1])
            vwap_upper_today = float(df['VWAP_Upper'].iloc[-1])
            vwap_lower_today = float(df['VWAP_Lower'].iloc[-1])
            
            atr_today = float(df['ATR_10'].iloc[-1])
            
            atr_pct = (atr_today / price_today) * 100
            potensi_tp_pct = ((middle_kc - price_today) / price_today) * 100
            target_tp_price = middle_kc

            score = 0
            kc_status = "⚪ INSIDE KC"
            vwap_status = "⚪ DALAM BATAS WAJAR"
            action = "WAIT"

            for i in range(1, 5):
                try:
                    p_close = float(df["Close"].iloc[-i])
                    p_upper = float(df['KCUe_20_2'].iloc[-i])
                    p_lower = float(df['KCLe_20_2'].iloc[-i])
                    hari_teks = "Hari Ini" if i == 1 else f"{i-1} Hari Lalu"
                    
                    if p_close > p_upper:
                        kc_status = f"🚀 KC BREAKOUT ATAS ({hari_teks})"
                        action = "🟢 BUY MOMENTUM" if i == 1 else "🟡 PULLBACK / RETEST"
                        score += 100 - (i * 2) 
                        break 
                    elif p_close < p_lower:
                        kc_status = f"📉 KC BREAKOUT BAWAH ({hari_teks})"
                        action = "🔴 SELL / AVOID"
                        score -= 100 - (i * 2)
                        break
                except: continue

            is_deep_oversold = (price_today < lower_kc) and (price_today < vwap_lower_today)

            if price_today > vwap_upper_today:
                vwap_status = "🔥 OVERVALUED (Tembus VWAP Atas)"
                if "BUY" in action: action = "⚠️ RAWAN KOREKSI (Take Profit)"
                score += 20
                
            elif is_deep_oversold:
                vwap_status = "🧊 DEEP OVERSOLD (Bawah KC & VWAP)"
                if atr_pct >= 3.0 and potensi_tp_pct >= 10.0:
                    action = "🎯 RUBBER BAND SETUP (Target >10%)"
                    score += 80 
                else:
                    action = "🧊 OVERSOLD (Pantulan Kecil)"
                    score += 20
            elif price_today < vwap_lower_today:
                vwap_status = "🧊 UNDERVALUED (Tembus VWAP Bawah)"
                if "PULLBACK" in action: action = "💎 SNIPER ENTRY"
                score -= 20
                
            if "RUBBER BAND" in action or "SNIPER" in action:
                if "BULL" in ha_status:
                    score += 50 
                    action = "🟢 " + action + " + HA CONFIRMED"
                else:
                    action = "⏳ WAIT " + action + " (Tunggu HA Hijau)"
                    score -= 40 

            results.append({
                "Ticker": ticker,
                "Action": action,
                "Score": score,
                "Harga Skrg": int(price_today),
                "Target TP": int(target_tp_price),
                "Potensi TP (%)": round(potensi_tp_pct, 2),
                "ATR (%)": round(atr_pct, 2),
                "Status Keltner": kc_status,
                "Status VWAP": vwap_status,
                "Heikin Ashi": ha_status,
                "UT Bot (1,10)": ut_signal,
                "Last Update": waktu_update
            })
            
        except Exception as e:
            print(f"  -> Kalkulasi gagal untuk {ticker}: {e}")

    df_result = pd.DataFrame(results)

    desired_order = [
        "Ticker", "Action", "Score", "Harga Skrg", "Target TP", "Potensi TP (%)", "ATR (%)",
        "Status Keltner", "Status VWAP", "Heikin Ashi", "UT Bot (1,10)", "Last Update"
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
        "AMFG.JK", "AMIN.JK", "APII.JK", "ARKA.JK", "ARNA.JK", "ASGR.JK", "ASII.JK", "BHIT.JK", "BINO.JK", "BLUE.JK", 
        "BNBR.JK", "CAKK.JK", "CCSI.JK", "CRSN.JK", "CTTH.JK", "DYAN.JK", "FOLK.JK", "GPSO.JK", "HEXA.JK", "HOPE.JK", 
        "HYGN.JK", "IBFN.JK", "ICON.JK", "IKAI.JK", "IKBI.JK", "IMPC.JK", "INDX.JK", "INTA.JK", "JECC.JK", "JTPE.JK", 
        "KBLI.JK", "KBLM.JK", "KIAS.JK", "KING.JK", "KOBX.JK", "KOIN.JK", "KONI.JK", "KUAS.JK", "LABA.JK", "LION.JK", "MARK.JK", 
        "MDRN.JK", "MFMI.JK", "MHKI.JK", "MLIA.JK", "MUTU.JK", "NAIK.JK", "NTBK.JK", "PADA.JK", "PIPA.JK", "PTMP.JK", 
        "SCCO.JK", "SINI.JK", "SKRN.JK", "SMIL.JK", "SOSS.JK", "SPTO.JK", "TIRA.JK", "TOTO.JK", "TRIL.JK", "UNTR.JK", 
        "VISI.JK", "VOKS.JK", "ZBRA.JK"
    ],
    "IDXNONCYC": [
        "AALI.JK", "ADES.JK", "AGAR.JK", "AISA.JK", "ALTO.JK", "AMMS.JK", "AMRT.JK", "ANDI.JK", "ANJT.JK", "ASHA.JK", 
        "AYAM.JK", "BEEF.JK", "BEER.JK", "BISI.JK", "BOBA.JK", "BRRC.JK", "BTEK.JK", "BUAH.JK", "BUDI.JK", "BWPT.JK", 
        "CAMP.JK", "CBUT.JK", "CEKA.JK", "CLEO.JK", "CMRY.JK", "COCO.JK", "CPIN.JK", "CRAB.JK", "CPRO.JK", "CSRA.JK", 
        "DAYA.JK", "DEWI.JK", "DLTA.JK", "DMND.JK", "DPUM.JK", "DSFI.JK", "DSNG.JK", "ENZO.JK", "EPMT.JK", "EURO.JK", "FAPA.JK", 
        "FISH.JK", "FLMC.JK", "FOOD.JK", "FORE.JK", "GGRM.JK", "GOLL.JK", "GOOD.JK", "GRPM.JK", "GULA.JK", "GUNA.JK", "GZCO.JK", 
        "HERO.JK", "HMSD.JK", "HMSP.JK", "HOKI.JK","IBOS.JK", "ICBP.JK", "IKAN.JK", "INDF.JK", "IPPE.JK", "ISEA.JK", "ITIC.JK", "JARR.JK", 
        "JAWA.JK", "JPFA.JK", "KEJU.JK", "KINO.JK", "KMDS.JK", "LAPD.JK", "LSIP.JK", "MAGP.JK", "MAIN.JK", "MAXI.JK", 
        "MBTO.JK", "MGRO.JK", "MIDI.JK", "MKTR.JK", "MLBI.JK", "MLPL.JK", "MPPA.JK", "MRAT.JK", "MSJA.JK", "MYOR.JK","NANO.JK", 
        "NASI.JK", "NAYZ.JK", "NEST.JK", "NSSS.JK", "OILS.JK", "PCAR.JK", "PGUN.JK", "PMMP.JK", "PNGO.JK", "PSDN.JK", "PSGO.JK", 
        "PTPS.JK", "RANC.JK", "RLCO.JK", "ROTI.JK", "SDPC.JK", "SGRO.JK", "SIMP.JK", "SIPD.JK", "SKBM.JK", "SKLT.JK", 
        "SMAR.JK","SOUL.JK", "SSMS.JK", "STAA.JK", "STRK.JK", "STTP.JK", "TAPG.JK", "TAYS.JK", "TBLA.JK", "TCID.JK", "TGKA.JK", 
        "TGUK.JK", "TLDN.JK", "TRGU.JK", "UCID.JK", "UDNG.JK", "ULTJ.JK", "UNSP.JK", "UNVR.JK", "VICI.JK", "WAPO.JK", 
        "WICO.JK", "WIIM.JK", "WINE.JK", "WMPP.JK", "WMUU.JK", "YUPI.JK"
    ],
    "IDXFINANCE": [
        "ABDA.JK", "ADMF.JK", "AGRO.JK", "AGRS.JK", "AHAP.JK", "AMAG.JK", "AMAR.JK", "AMOR.JK", "APIC.JK", "ARTO.JK", 
        "ASBI.JK", "ASDM.JK", "ASJT.JK", "ASMI.JK", "ASRM.JK", "BABP.JK", "BACA.JK", "BANK.JK", "BBCA.JK", "BBHI.JK", 
        "BBKP.JK", "BBLD.JK", "BBMD.JK", "BBNI.JK", "BBRI.JK", "BBSI.JK", "BBTN.JK", "BBYB.JK", "BCAP.JK", "BCIC.JK", 
        "BDMN.JK", "BEKS.JK", "BFIN.JK", "BGTG.JK", "BHAT.JK", "BHIT.JK", "BINA.JK", "BJBR.JK", "BJTM.JK", "BKSW.JK", "BMAS.JK", 
        "BMRI.JK", "BNBA.JK", "BNGA.JK", "BNII.JK", "BNLI.JK", "BPFI.JK", "BPII.JK", "BRIS.JK", "BSIM.JK", "BSWD.JK", 
        "BTPN.JK", "BTPS.JK", "BVIC.JK", "CASA.JK", "CFIN.JK", "COIN.JK", "DEFI.JK", "DNAR.JK", "DNET.JK", "FUJI.JK", "GSMF.JK", "HBAT.JK", 
        "HDFA.JK", "INPC.JK", "IPAC.JK", "JMAS.JK","KIJA.JK", "LIFE.JK", "LPGI.JK", "LPPS.JK", "MASB.JK", "MAYA.JK", "MCOR.JK", "MEGA.JK", 
        "MREI.JK", "MSIE.JK", "MTWI.JK", "NICK.JK", "NISP.JK", "NOBU.JK", "OCAP.JK", "PADI.JK", "PALM.JK", "PANS.JK", "PEGE.JK", 
        "PLAS.JK", "PNBN.JK", "PNBS.JK", "PNIN.JK", "PNLF.JK", "POLA.JK", "POOL.JK", "RELF.JK","RELI.JK", "SDRA.JK", "SFAN.JK", 
        "SMMA.JK", "SRTG.JK", "STAR.JK", "SUPA.JK", "TIFA.JK", "TRIM.JK", "TRUS.JK", "TUGU.JK", "VICO.JK", "VINS.JK", 
        "VRNA.JK", "VTNY.JK", "WIDI.JK", "WOMF.JK", "YOII.JK", "YULE.JK"
    ],
    "IDXCYCLIC": [
        "ABBA.JK", "ACES.JK", "ACRO.JK", "AEGS.JK", "AKKU.JK", "ARGO.JK", "ARTA.JK", "ASLC.JK", "AUTO.JK", "BABY.JK", 
        "BAIK.JK", "BATA.JK", "BAUT.JK", "BAYU.JK", "BELL.JK", "BIKE.JK", "BIMA.JK", "BLTZ.JK", "BMBL.JK", "BMTR.JK", "BOGA.JK", 
        "BOLA.JK", "BOLT.JK", "BRAM.JK", "BUVA.JK", "CARS.JK", "CBMF.JK", "CINT.JK", "CLAY.JK", "CNMA.JK", "CNTX.JK", 
        "CSAP.JK", "CSMI.JK", "DEPO.JK", "DFAM.JK", "DIGI.JK", "DOOH.JK", "DOSS.JK", "DRMA.JK", "DUCK.JK", "EAST.JK", 
        "ECII.JK", "ENAK.JK", "ERAA.JK", "ERAL.JK", "ERTX.JK", "ESTA.JK", "ESTI.JK", "FAST.JK", "FILM.JK", "FITT.JK", 
        "FORU.JK", "FUTR.JK", "GDYR.JK", "GEMA.JK", "GJTL.JK", "GLOB.JK", "GOLF.JK", "GRPH.JK", "GWSA.JK","HAJJ.JK", "HOME.JK", 
        "HOTL.JK", "HRME.JK", "HRTA.JK", "IDEA.JK", "IIKP.JK", "IMAS.JK", "INDR.JK", "INDS.JK", "INOV.JK", "IPTV.JK", "ISAP.JK", 
        "JGLE.JK", "JIHD.JK", "JSPT.JK", "KAQI.JK", "KDTN.JK", "KICI.JK", "KLIN.JK", "KOTA.JK", "KPIG.JK", "LFLO.JK", "LIVE.JK", "LMAX.JK", 
        "LMPI.JK", "LPIN.JK", "LPPF.JK", "LUCY.JK", "MABA.JK", "MAPA.JK", "MAPB.JK", "MAPI.JK", "MARI.JK", "MDIA.JK", "MDIY.JK", "MEJA.JK", 
        "MERI.JK", "MGNA.JK", "MGLV.JK", "MICE.JK", "MINA.JK", "MNCN.JK", "MPMX.JK", "MSIN.JK", "MSKY.JK", "MYTX.JK", "NATO.JK", 
        "NETV.JK", "NUSA.JK", "OLIV.JK", "PANR.JK", "PART.JK", "PBRX.JK", "PDES.JK", "PGLI.JK", "PJAA.JK", "PLAN.JK","PMJS.JK", "PMUI.JK", 
        "PNSE.JK", "POLU.JK", "POLY.JK", "PSKT.JK", "PTSP.JK", "PZZA.JK", "RAAM.JK", "RAFI.JK", "RALS.JK", "RICY.JK", 
        "SBAT.JK", "SCMA.JK", "SCNP.JK", "SHID.JK", "SLIS.JK", "SMSM.JK", "SNLK.JK","SOFA.JK" "SONA.JK", "SOTS.JK", 
       "SPRE.JK", "SRIL.JK", "SSTM.JK", "SWID.JK", "TELE.JK", "TFCO.JK", "TMPO.JK", "TOOL.JK", "TOYS.JK", "TRIO.JK", 
       "TRIS.JK", "TYRE.JK",  "UFOE.JK", "UNIT.JK", "UNTD.JK", "VERN.JK", "VIVA.JK", "VKTR.JK", "WOOD.JK", "YELO.JK", 
       "ZATA.JK", "ZONE.JK"
    ],
    "IDXTECHNO": [
        "AREA.JK", "ATIC.JK", "AWAN.JK", "AXIO.JK", "BELI.JK", "BUKA.JK", "CASH.JK", "CHIP.JK", "CYBR.JK", "DCII.JK", 
        "DIVA.JK", "DMMX.JK", "EDGE.JK", "ELIT.JK", "EMTK.JK", "ENVY.JK", "GLVA.JK", "GOTO.JK", "HDIT.JK", "IOTF.JK", 
        "IRSX.JK", "JATI.JK", "KIOS.JK", "KREN.JK", "LMAS.JK", "LUCK.JK", "MCAS.JK", "MLPT.JK", "MPIX.JK", "MSTI.JK", 
        "MTDL.JK", "NFCX.JK", "NINE.JK", "PGJO.JK", "PTSN.JK", "RUNS.JK", "SKYB.JK", "TECH.JK", "TFAS.JK", "TOSK.JK", 
        "TRON.JK", "UVCR.JK", "WGSH.JK", "WIFI.JK", "WIRG.JK", "ZYRX.JK"
    ],
    "IDXBASIC": [
        "ADMG.JK", "AGII.JK", "AKPI.JK", "ALDO.JK", "ALKA.JK", "ALMI.JK", "AMMN.JK", "ANTM.JK", "APLI.JK", "ARCI.JK", 
        "ASPR.JK", "AVIA.JK", "AYLS.JK", "BAJA.JK", "BATR.JK", "BEBS.JK", "BLES.JK", "BMSR.JK", "BRMS.JK", "BRNA.JK", 
        "BRPT.JK", "BTON.JK", "CHEM.JK", "CITA.JK", "CLPI.JK", "CMNT.JK", "CTBN.JK", "DAAZ.JK", "DGWG.JK", "DKFT.JK", 
        "DPNS.JK", "EKAD.JK", "EMAS.JK", "EPAC.JK", "ESIP.JK", "ESSA.JK", "ETWA.JK", "FASW.JK", "FPNI.JK", "FWCT.JK", 
        "GDST.JK", "GGRP.JK", "HKMU.JK", "IFII.JK", "IFSH.JK", "IGAR.JK", "INAI.JK", "INCF.JK", "INCI.JK", "INCO.JK", 
        "INKP.JK", "INRU.JK", "INTD.JK", "INTP.JK", "IPOL.JK", "ISSP.JK", "KAYU.JK", "KBRI.JK", "KDSI.JK", "KKES.JK", 
        "KMTR.JK", "KRAS.JK", "LMSH.JK", "LTLS.JK", "MBMA.JK", "MDKA.JK", "MDKI.JK", "MINE.JK", "MOLI.JK", "NCKL.JK", 
        "NICE.JK", "NICL.JK", "NIKL.JK", "NPGF.JK", "OBMD.JK", "OKAS.JK", "OPMS.JK", "PACK.JK", "PBID.JK", "PDPP.JK", 
        "PICO.JK", "PPRI.JK", "PSAB.JK", "PTMR.JK", "PURE.JK", "SAMF.JK", "SBMA.JK", "SIMA.JK", "SMBR.JK", "SMCB.JK", 
        "SMGA.JK", "SMGR.JK", "SMKL.JK", "SMLE.JK", "SOLA.JK", "SPMA.JK", "SQMI.JK", "SRSN.JK", "SULI.JK", "SWAT.JK", 
        "TALF.JK", "TBMS.JK", "TDPM.JK", "TINS.JK", "TIRT.JK", "TKIM.JK", "TPIA.JK", "TRST.JK", "UNIC.JK", "WSBP.JK", 
        "WTON.JK", "YPAS.JK", "ZINC.JK"
    ],
    "IDXENERGY": [
        "AADI.JK", "ABMM.JK", "ADMR.JK", "ADRO.JK", "AIMS.JK", "AKRA.JK", "ALII.JK", "APEX.JK", "ARII.JK", "ARTI.JK", 
        "ATLA.JK", "BBRM.JK", "BESS.JK", "BIPI.JK", "BOAT.JK", "BOSS.JK", "BSML.JK", "BSSR.JK", "BULL.JK", "BUMI.JK", 
        "BYAN.JK", "CANI.JK", "CBRE.JK", "CGAS.JK", "CNKO.JK", "COAL.JK", "CUAN.JK", "DEWA.JK", "DOID.JK", "DSSA.JK", 
        "DWGL.JK", "ELSA.JK", "ENRG.JK", "FIRE.JK", "GEMS.JK", "GTBO.JK", "GTSI.JK", "HILL.JK", "HITS.JK", "HRUM.JK", 
        "HUMI.JK", "IATA.JK", "INDY.JK", "INPS.JK", "ITMA.JK", "ITMG.JK", "JSKY.JK", "KKGI.JK", "KOPI.JK", "LEAD.JK", 
        "MAHA.JK", "MBAP.JK", "MBSS.JK", "MCOL.JK", "MEDC.JK", "MKAP.JK", "MTFN.JK", "MYOH.JK", "PGAS.JK", "PKPK.JK", 
        "PSAT.JK", "PSSI.JK", "PTBA.JK", "PTIS.JK", "PTRO.JK", "RAJA.JK", "RATU.JK", "RGAS.JK", "RIGS.JK", "RMKE.JK", 
        "RMKO.JK", "RUIS.JK", "SEMA.JK", "SGER.JK", "SHIP.JK", "SICO.JK", "SMMT.JK", "SMRU.JK", "SOCI.JK", "SUGI.JK", 
        "SUNI.JK", "SURE.JK", "TAMU.JK", "TCPI.JK", "TEBE.JK", "TOBA.JK", "TPMA.JK", "TRAM.JK", "UNIQ.JK", "WINS.JK", 
        "WOWS.JK"
    ],
    "IDXHEALTH": [
        "BMHS.JK", "CARE.JK", "CHEK.JK", "DGNS.JK", "DKHH.JK", "DVLA.JK", "HALO.JK", "HEAL.JK", "IKPM.JK", "INAF.JK", 
        "IRRA.JK", "KAEF.JK", "KLBF.JK", "LABS.JK", "MDLA.JK", "MEDS.JK", "MERK.JK", "MIKA.JK", "MMIX.JK", "MTMH.JK", 
        "OBAT.JK", "OMED.JK", "PEHA.JK", "PEVE.JK", "PRDA.JK", "PRAY.JK", "PRIM.JK", "PYFA.JK", "RSCH.JK", "RSGK.JK", 
        "SAME.JK", "SCPI.JK", "SIDO.JK", "SILO.JK", "SOHO.JK", "SRAJ.JK", "SURI.JK", "TSPC.JK"
    ],
    "IDXINFRA": [
        "ACST.JK", "ADHI.JK", "ARKO.JK", "ASLI.JK", "BALI.JK", "BDKR.JK", "BREN.JK", "BTEL.JK", "BUKK.JK", "CASS.JK", 
        "CDIA.JK", "CENT.JK", "CMNP.JK", "DATA.JK", "DGIK.JK", "EXCL.JK", "GHON.JK", "GMFI.JK", "GOLD.JK", "HADE.JK", 
        "HGII.JK", "IBST.JK", "IDPR.JK", "INET.JK", "IPCC.JK", "IPCM.JK", "ISAT.JK", "JAST.JK", "JKON.JK", "JSMR.JK", 
        "KARW.JK", "KBLV.JK", "KEEN.JK", "KETR.JK", "KOKA.JK", "KRYA.JK", "LCKM.JK", "LINK.JK", "META.JK", "MORA.JK", 
        "MPOW.JK", "MTEL.JK", "MTPS.JK", "MTRA.JK", "NRCA.JK", "OASA.JK", "PBSA.JK", "PGEO.JK", "PORT.JK", "POWR.JK", 
        "PPRE.JK", "PTDU.JK", "PTPP.JK", "PTPW.JK", "RONY.JK", "SMKM.JK","SSIA.JK", "SUPR.JK", "TAMA.JK", "TBIG.JK", 
        "TGRA.JK", "TLKM.JK", "TOPS.JK", "TOTL.JK", "TOWR.JK", "WEGE.JK", "WIKA.JK", "WSKT.JK"
    ],
    "IDXPROPERT": [
        "ADCP.JK", "AMAN.JK", "APLN.JK", "ARMY.JK", "ASPI.JK", "ASRI.JK", "ATAP.JK", "BAPA.JK", "BAPI.JK", "BBSS.JK", 
        "BCIP.JK", "BEST.JK", "BIKA.JK", "BIPP.JK", "BKDP.JK", "BKSL.JK", "BSBK.JK", "BSDE.JK", "CBDK.JK", "CBPE.JK", 
        "CITY.JK", "COWL.JK", "CPRI.JK", "CSIS.JK", "CTRA.JK", "DADA.JK", "DART.JK", "DILD.JK", "DMAS.JK", "DUTI.JK", 
        "ELTY.JK", "EMDE.JK", "FMII.JK", "GAMA.JK", "GMTD.JK", "GPRA.JK", "GRIA.JK", "HOMI.JK", "INDO.JK", "INPP.JK", 
        "JRPT.JK", "KBAG.JK", "KLJA.JK", "KOCI.JK", "KSIX.JK", "LAND.JK", "LCGP.JK", "LPCK.JK", "LPKR.JK", "LPLI.JK", "MANG.JK", 
        "MDLN.JK", "MKPI.JK", "MMLP.JK", "MPRO.JK", "MTLA.JK", "MTSM.JK", "NASA.JK", "NIRO.JK", "NZIA.JK", "OMRE.JK", 
        "PAMG.JK", "PANI.JK", "PLIN.JK", "POLI.JK", "POLL.JK", "POSA.JK", "PPRO.JK", "PUDP.JK", "PURI.JK", "PWON.JK", 
        "RBMS.JK", "RDTX.JK", "REAL.JK", "RIMO.JK", "RISE.JK", "ROCK.JK", "RODA.JK", "SAGE.JK", "SATU.JK", "SMDM.JK", 
        "SMRA.JK", "TARA.JK", "TRIN.JK", "TRUE.JK", "UANG.JK", "URBN.JK", "VAST.JK", "WINR.JK"
    ],
    "IDXTRANS": [
        "AKSI.JK", "ASSA.JK", "BIRD.JK", "BLOG.JK", "BLTA.JK", "BPTR.JK", "CMPP.JK", "DEAL.JK", "ELPI.JK", "GIAA.JK", 
        "GTRA.JK", "HAIS.JK", "HATM.JK", "HELI.JK", "IMJS.JK", "JAYA.JK", "KJEN.JK", "KLAS.JK", "LAJU.JK", "LOPI.JK", 
        "LRNA.JK", "MIRA.JK", "MITI.JK", "MPXL.JK", "NELY.JK", "PJHB.JK", "PPGL.JK", "PURA.JK", "RCCC.JK", "SAFE.JK", "SAPX.JK", 
        "SDMU.JK", "SMDR.JK", "TAXI.JK", "TMAS.JK", "TNCA.JK", "TRJA.JK", "TRUK.JK", "WEHA.JK"
    ]
}

# ==========================================
# MAIN
# ==========================================
if __name__ == "__main__":

    print("🤖 START MARKET SCANNER PRO (KELTNER CHANNEL NATIVE) 🤖")

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
