# ==========================================
# MARKET SCANNER - PRO (MINERVINI + FIBO + SMART NEWS)
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
from GoogleNews import GoogleNews 

warnings.filterwarnings('ignore')

SPREADSHEET_ID = "1I_SJ3InMZPiSS1XibF-w000lwjc1PIsRaJ_kXzQ3LxE"

# ==========================================
# DATABASE KATA KUNCI SEKTORAL (SMART DICTIONARY)
# ==========================================
# Ini memetakan saham ke isu spesifik (CPO, Emas, Tech, dll)
SECTOR_KEYWORDS_MAP = {
    # CPO / SAWIT (SGRO, DSNG, AALI, dll)
    "CPO": {
        "tickers": ["SGRO", "AALI", "LSIP", "DSNG", "TAPG", "STAA", "SIMP", "BWPT", "TBLA", "SSMS"],
        "keywords": ["cpo", "sawit", "minyak goreng", "biodiesel", "b35", "b40", "el nino", "harvest", "palm oil"]
    },
    # EMAS / GOLD (Safe Haven, The Fed)
    "GOLD": {
        "tickers": ["ANTM", "MDKA", "BRMS", "PSAB", "ARCI", "JMAS"],
        "keywords": ["emas", "gold", "logam mulia", "safe haven", "the fed", "suku bunga", "harga emas"]
    },
    # NICKEL / EV BATTERY (EV Boom)
    "EV": {
        "tickers": ["NCKL", "INCO", "MBMA", "HRUM", "ANTM", "DKFT", "NICL"],
        "keywords": ["nikel", "nickel", "baterai", "ev", "listrik", "tesla", "hilirisasi", "smelter", "kendaraan listrik"]
    },
    # DATA CENTER / TECH / AI BOOM
    "TECH": {
        "tickers": ["DCII", "EDGE", "GOTO", "BUKA", "EMTK", "WIFI", "MTDL"],
        "keywords": ["data center", "ai", "cloud", "artificial intelligence", "nvidia", "cyber", "teknologi", "startup", "digital"]
    },
    # OIL & GAS (Geopolitik)
    "ENERGY": {
        "tickers": ["MEDC", "PGAS", "ELSA", "AKRA", "ENRG", "APEX"],
        "keywords": ["minyak", "oil", "gas", "opec", "timur tengah", "brent", "wti", "konflik"]
    },
    # COAL (Winter, China)
    "COAL": {
        "tickers": ["ADRO", "PTBA", "ITMG", "BUMI", "INDY", "HRUM", "UNTR", "KKGI", "GEMS"],
        "keywords": ["batubara", "coal", "china", "musim dingin", "energi", "pltu", "newcastle"]
    },
    # BANKING (Interest Rate)
    "BANK": {
        "tickers": ["BBCA", "BBRI", "BMRI", "BBNI", "BBTN", "BRIS"],
        "keywords": ["kredit", "laba bersih", "bunga", "bi rate", "margin", "modal", "deviden"]
    }
}

# ==========================================
# FUNGSI SENTIMENT ANALISIS (UPDATED SECTOR AWARE)
# ==========================================
def check_news_sentiment(ticker_symbol):
    """
    Mencari berita via Google News dengan 'Sector Awareness'.
    """
    try:
        clean_ticker = ticker_symbol.replace(".JK", "")
        
        # 1. Cari Sektor Saham ini (Otomatis)
        extra_keywords = []
        sector_found = "General"
        
        # Cek apakah ticker ada di database sektoral kita
        for sector, data in SECTOR_KEYWORDS_MAP.items():
            if clean_ticker in data["tickers"]:
                extra_keywords = data["keywords"]
                sector_found = sector
                break
        
        # 2. Setup Google News
        googlenews = GoogleNews(lang='id', region='ID', period='7d')
        googlenews.search(clean_ticker)
        results = googlenews.result()
        
        if not results:
            return "No News", 0

        # 3. Keyword Scoring (General + Sector Specific)
        positive_keywords = [
            "laba", "naik", "dividen", "akuisisi", "merger", "buyback", 
            "proyek", "kerjasama", "investasi", "untung", "lonjakan",
            "ekspansi", "tertinggi", "positif", "rekor", "anggaran", "surplus"
        ]
        # Gabungkan keyword umum dengan keyword sektor
        positive_keywords.extend(extra_keywords) 
        
        negative_keywords = [
            "rugi", "turun", "anjlok", "pkpu", "pailit", "gugat", 
            "suspensi", "utang", "beban", "negatif", "korupsi", 
            "diperiksa", "sanksi", "denda", "phk", "batal", "defisit"
        ]

        sentiment_score = 0
        news_titles = []

        # Analisa 5 Berita Teratas
        for i, item in enumerate(results[:5]):
            title = item['title'].lower()
            news_titles.append(item['title'])
            
            # Scoring
            for word in positive_keywords:
                if word in title: 
                    sentiment_score += 1
                    # Jika keyword sektor muncul, beri poin bonus ekstra!
                    if word in extra_keywords: 
                        sentiment_score += 1 
            
            for word in negative_keywords:
                if word in title: sentiment_score -= 1

        # Kesimpulan Narasi
        if sentiment_score > 2:
            narrative = f"🟢 STRONG ({sector_found})"
        elif sentiment_score > 0:
            narrative = f"🟢 POSITIVE ({sector_found})"
        elif sentiment_score < 0:
            narrative = "🔴 NEGATIVE"
        else:
            narrative = "⚪ NEUTRAL"

        headline = news_titles[0] if news_titles else "-"
        # Potong headline biar rapi
        if len(headline) > 100: headline = headline[:97] + "..."
        
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
            # Download data
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
            news_info = "-"
            news_score = 0
            
            # Hanya cek berita jika skor teknikal lumayan (>= 65)
            if score >= 65:
                # print(f"🔍 Checking News for {ticker}...") 
                news_info, news_score = check_news_sentiment(ticker)
                
                # Update Score berdasarkan Berita
                if news_score > 0: score += 10 
                elif news_score < 0: score -= 20 # Hukuman lebih berat untuk bad news

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
            if news_score > 0: reasons.append("Positive News")
            
            alasan_text = ", ".join(reasons) if reasons else "-"

            results.append({
                "Ticker": ticker,
                "Harga Skrg": int(price),
                "Trend Status": trend_desc,
                "Action": action,
                "Score": score,
                "Narasi Berita": news_info,
                "Risk/Reward": round(rr, 2),
                "Target Aman": int(target_aman),
                "Level Target": fib_note,
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

    desired_order = [
        "Ticker", "Harga Skrg", "Trend Status", "Action", "Score", "Narasi Berita", 
        "Level Target", "Risk/Reward", "Target Aman", "Target Moon", 
        "Potensi Aman (%)", "Potensi Moon (%)", "Stop Loss", "Alasan", "Last Update"
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
    "SWAT.JK", "BTon.JK", "TALF.JK", "KDSI.JK", "INRU.JK", "CMNT.JK", "INTD.JK", "ALKA.JK", "KMTR.JK", "CTBN.JK", "YPAS.JK",
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
# MAIN
# ==========================================
if __name__ == "__main__":

    print("🤖 START MARKET SCANNER PRO (WITH SECTOR NEWS) 🤖")
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
        
        # Sleep lebih lama antar sektor untuk menghindari blocking Google News
        time.sleep(3) 

    print("🏁 SELESAI 🏁")
