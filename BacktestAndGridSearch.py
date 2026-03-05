# ============================================================
# BACKTESTING ENGINE v2.0 — OPTIMIZED
# Tambahan:
#   - Parameter Optimization (Grid Search)
#   - Walk-Forward Validation (3 windows)
#   - Trailing Stop (profit 4% → trail 2%)
#   - Monte Carlo Simulation (10.000 iterasi)
#   - Per-Pattern Performance Analysis
#   - IHSG Regime Filter
#   - Liquidity Filter
#   - Benchmark vs IHSG Buy & Hold
# ============================================================

import yfinance as yf
import pandas as pd
import numpy as np
import ta
import gspread
from gspread_dataframe import set_with_dataframe
from datetime import datetime, timedelta
import warnings
import json
import os
import time
import itertools
from copy import deepcopy

warnings.filterwarnings('ignore')

# ============================================================
# KONFIGURASI DASAR
# ============================================================
SPREADSHEET_ID  = "1Fnfotp0hpZxNqGEko3AP2GJqR5EMimQ-bJGj3AWsrnw"
MODAL_AWAL      = 2_000_000
BACKTEST_YEARS  = 2
LOT_SIZE        = 100

# Parameter default (akan di-override saat optimization)
DEFAULT_PARAMS = {
    "MAX_HOLD_DAYS"       : 10,
    "RR_TARGET"           : 2.0,
    "MIN_SCORE"           : 65,
    "MIN_TIER"            : 3,
    "TRAILING_ACTIVATE"   : 0.04,   # Aktifkan trailing setelah profit 4%
    "TRAILING_DISTANCE"   : 0.02,   # Trail 2% dari high tertinggi
    "USE_IHSG_FILTER"     : True,
    "USE_LIQUIDITY_FILTER": True,
    "MIN_VOLUME_IDR"      : 500_000_000,  # Min volume Rp 500 juta/hari
}

# ============================================================
# UNIVERSE SAHAM
# ============================================================
ALL_TICKERS = [
    "ASII.JK","UNTR.JK","HEXA.JK","IMPC.JK","TOTO.JK","ARNA.JK","KBLI.JK","SCCO.JK","JECC.JK","VOKS.JK",
    "AMFG.JK","KIAS.JK","LION.JK",
    "UNVR.JK","INDF.JK","ICBP.JK","MYOR.JK","CPIN.JK","JPFA.JK","ULTJ.JK","AALI.JK","LSIP.JK","SGRO.JK",
    "SSMS.JK","DSNG.JK","SMAR.JK","TBLA.JK","SIMP.JK","CLEO.JK","GOOD.JK","ROTI.JK","CAMP.JK","CEKA.JK",
    "ADES.JK","KINO.JK","DLTA.JK","MLBI.JK","STTP.JK","TCID.JK","SKBM.JK","SKLT.JK","EPMT.JK",
    "AMRT.JK","MIDI.JK",
    "BBCA.JK","BBRI.JK","BMRI.JK","BBNI.JK","BRIS.JK","BBTN.JK","BNGA.JK","BDMN.JK","NISP.JK","BJBR.JK",
    "BJTM.JK","BTPS.JK","MEGA.JK","PNBN.JK","BTPN.JK","ADMF.JK","BFIN.JK","CFIN.JK","WOMF.JK",
    "PNLF.JK","SMMA.JK",
    "MNCN.JK","SCMA.JK","ACES.JK","MAPI.JK","AUTO.JK","GJTL.JK","SMSM.JK","ERAA.JK","LPPF.JK","RALS.JK",
    "FAST.JK","CSAP.JK","BRAM.JK","INDS.JK","GDYR.JK",
    "GOTO.JK","EMTK.JK","DCII.JK","MTDL.JK","MLPT.JK",
    "ANTM.JK","INCO.JK","TINS.JK","MDKA.JK","AMMN.JK","BRMS.JK","SMGR.JK","INTP.JK","BRPT.JK","TPIA.JK",
    "INKP.JK","TKIM.JK","WTON.JK","ISSP.JK","FASW.JK","AVIA.JK","EKAD.JK","INCI.JK","IGAR.JK","TRST.JK",
    "AKPI.JK","BRNA.JK","SPMA.JK","PBID.JK","AGII.JK","UNIC.JK","CLPI.JK","SRSN.JK",
    "ADRO.JK","PTBA.JK","ITMG.JK","BUMI.JK","PGAS.JK","HRUM.JK","INDY.JK","DSSA.JK","BYAN.JK","MEDC.JK",
    "AKRA.JK","ELSA.JK","PTRO.JK","TOBA.JK","BSSR.JK","MBAP.JK","MYOH.JK","GEMS.JK","DOID.JK","ABMM.JK",
    "BULL.JK","WINS.JK",
    "KLBF.JK","SIDO.JK","KAEF.JK","TSPC.JK","DVLA.JK","MERK.JK","PEHA.JK","PYFA.JK","SOHO.JK","MIKA.JK",
    "SILO.JK","HEAL.JK","PRDA.JK","BMHS.JK",
    "TLKM.JK","ISAT.JK","EXCL.JK","TOWR.JK","TBIG.JK","MTEL.JK","JSMR.JK","ADHI.JK","WIKA.JK","PTPP.JK",
    "WSKT.JK","TOTL.JK","SSIA.JK","NRCA.JK","WEGE.JK","CMNP.JK","LINK.JK","BREN.JK","PGEO.JK","POWR.JK",
    "CTRA.JK","BSDE.JK","PWON.JK","SMRA.JK","ASRI.JK","LPKR.JK","DMAS.JK","DUTI.JK","JRPT.JK","MTLA.JK",
    "GPRA.JK","LPCK.JK","MDLN.JK","BEST.JK","MKPI.JK","RDTX.JK","INPP.JK","PLIN.JK",
    "GIAA.JK","SMDR.JK","BIRD.JK","ASSA.JK","TMAS.JK","WEHA.JK",
]

# ============================================================
# GOOGLE SHEETS
# ============================================================
def connect_gsheet(sheet_name):
    try:
        creds_json = os.environ.get("GCP_SA_KEY")
        if not creds_json:
            print("❌ GCP_SA_KEY tidak ditemukan.")
            return None
        creds_dict = json.loads(creds_json)
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        from google.oauth2.service_account import Credentials
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        gc    = gspread.authorize(creds)
        sh    = gc.open_by_key(SPREADSHEET_ID)
        try:
            ws = sh.worksheet(sheet_name)
        except:
            ws = sh.add_worksheet(title=sheet_name, rows="5000", cols="25")
        return ws
    except Exception as e:
        print(f"❌ GSheet Error ({sheet_name}): {e}")
        return None

def upload_df(sheet_name, df):
    ws = connect_gsheet(sheet_name)
    if ws is not None:
        ws.clear()
        set_with_dataframe(ws, df)
        print(f"   ✅ '{sheet_name}' — {len(df)} baris")
    time.sleep(1.5)

def upload_rows(sheet_name, rows):
    ws = connect_gsheet(sheet_name)
    if ws is not None:
        ws.clear()
        ws.update(rows)
        print(f"   ✅ '{sheet_name}' diupload")
    time.sleep(1.5)

# ============================================================
# HELPERS
# ============================================================
def is_near(p1, p2, pct=0.001):
    return abs(p1-p2)/max(p1,p2) <= pct

def close_near_high(row, thr=0.2):
    l = row['High']-row['Low']
    return (row['High']-max(row['Open'],row['Close'])) <= thr*l if l>0 else True

def close_near_low(row, thr=0.2):
    l = row['High']-row['Low']
    return (min(row['Open'],row['Close'])-row['Low']) <= thr*l if l>0 else True

def get_pattern_score(base_prob, tier, confluence):
    tm = {1:1.00, 2:1.15, 3:1.30}.get(tier, 1.00)
    cb = 1 + (confluence*0.05)
    return round(min(95, base_prob*100*tm*cb), 1)

def get_stars(tier):
    return {1:'⭐',2:'⭐⭐',3:'⭐⭐⭐'}.get(tier,'')

# ============================================================
# INDICATOR CALCULATION
# ============================================================
def calc_indicators(df):
    for p in [10,20,50,100,200]:
        df[f'SMA_{p}'] = ta.trend.sma_indicator(df['Close'], window=p)
        df[f'EMA_{p}'] = ta.trend.ema_indicator(df['Close'], window=p)
    ichi = ta.trend.IchimokuIndicator(df['High'],df['Low'],9,26,52)
    df['ISA']=ichi.ichimoku_a(); df['ISB']=ichi.ichimoku_b()
    df['ITS']=ichi.ichimoku_conversion_line(); df['IKS']=ichi.ichimoku_base_line()
    df['RSI'] = ta.momentum.rsi(df['Close'],14)
    st = ta.momentum.StochasticOscillator(df['High'],df['Low'],df['Close'],14,3)
    df['STOCH_K']=st.stoch(); df['STOCH_D']=st.stoch_signal()
    df['CCI'] = ta.trend.cci(df['High'],df['Low'],df['Close'],20)
    adx = ta.trend.ADXIndicator(df['High'],df['Low'],df['Close'],14)
    df['ADX']=adx.adx(); df['+DI']=adx.adx_pos(); df['-DI']=adx.adx_neg()
    df['AO']  = ta.momentum.awesome_oscillator(df['High'],df['Low'],5,34)
    df['MOM'] = df['Close'].diff(10)
    macd = ta.trend.MACD(df['Close'],26,12,9)
    df['MACD']=macd.macd(); df['MACD_SIGNAL']=macd.macd_signal()
    sr = ta.momentum.StochRSIIndicator(df['Close'],14,3,3)
    df['SRSI_K']=sr.stochrsi_k()*100; df['SRSI_D']=sr.stochrsi_d()*100
    df['WILLR'] = ta.momentum.williams_r(df['High'],df['Low'],df['Close'],14)
    df['EMA_13'] = ta.trend.ema_indicator(df['Close'],13)
    df['BULLP']  = df['High']-df['EMA_13']
    df['BEARP']  = df['Low'] -df['EMA_13']
    df['UO']     = ta.momentum.ultimate_oscillator(df['High'],df['Low'],df['Close'],7,14,28)
    df['VOL_SMA_20'] = df['Volume'].rolling(20).mean()
    df['VOL_VALUE']  = df['Close'] * df['Volume']
    df['ATR']     = ta.volatility.AverageTrueRange(df['High'],df['Low'],df['Close'],14).average_true_range()
    df['ATR_SMA20'] = df['ATR'].rolling(20).mean()
    return df

# ============================================================
# SIGNAL GENERATOR
# ============================================================
def generate_signal(df, idx, params):
    try:
        if idx < 5: return None
        d1=df.iloc[idx-4]; d2=df.iloc[idx-3]; d3=df.iloc[idx-2]
        d4=df.iloc[idx-1]; d5=df.iloc[idx]
        day1,day2,day3 = d3,d4,d5

        body1=abs(day1['Close']-day1['Open']); range1=day1['High']-day1['Low']
        body2=abs(day2['Close']-day2['Open']); range2=day2['High']-day2['Low']
        body3=abs(day3['Close']-day3['Open']); range3=day3['High']-day3['Low']

        us3=day3['High']-max(day3['Open'],day3['Close'])
        ls3=min(day3['Open'],day3['Close'])-day3['Low']

        bull1=day1['Close']>day1['Open']; bear1=not bull1
        bull2=day2['Close']>day2['Open']; bear2=not bull2
        bull3=day3['Close']>day3['Open']; bear3=not bull3

        atr3=day3['ATR']; valid_atr=atr3>0
        bt1=max(day1['Open'],day1['Close']); bb1=min(day1['Open'],day1['Close'])
        bt2=max(day2['Open'],day2['Close']); bb2=min(day2['Open'],day2['Close'])
        bt3=max(day3['Open'],day3['Close']); bb3=min(day3['Open'],day3['Close'])
        mid1=(day1['Open']+day1['Close'])/2

        # Trend context
        is_proper_down=(day3['SMA_20']<day3['SMA_50']) and (day3['SMA_20']<df['SMA_20'].iloc[idx-5]) and (day3['Close']<day3['SMA_50'])
        is_down_basic=day3['SMA_20']<day3['SMA_50']

        candle_ok=(range3>=atr3*0.7) if valid_atr else True
        vol_thrust=day3['Volume']>day3['VOL_SMA_20']*1.5
        vol_above=day3['Volume']>day3['VOL_SMA_20']
        is_volatile=day3['ATR']>day3['ATR_SMA20']*0.75 if valid_atr else True

        ext_os=day3['RSI']<35; os_ctx=day3['RSI']<45
        macd_b=day3['MACD']>day3['MACD_SIGNAL']
        ranging=day3['ADX']<20

        rh20=df['High'].iloc[max(0,idx-19):idx+1].max()
        rl20=df['Low'].iloc[max(0,idx-19):idx+1].min()
        pr20=rh20-rl20
        pp=(day3['Close']-rl20)/pr20 if pr20>0 else 0.5
        near_sup=pp<0.25

        # Liquidity filter
        if params.get("USE_LIQUIDITY_FILTER", True):
            avg_vol_idr = day3['VOL_VALUE'] if 'VOL_VALUE' in day3 else day3['Close']*day3['Volume']
            if avg_vol_idr < params.get("MIN_VOLUME_IDR", 500_000_000):
                return None

        # TV Score
        sc,cnt=0,0
        def add(v): nonlocal sc,cnt; sc+=v; cnt+=1
        for p in [10,20,50,100,200]:
            if pd.notna(day3[f'SMA_{p}']): add(1 if day3[f'SMA_{p}']<day3['Close'] else -1 if day3[f'SMA_{p}']>day3['Close'] else 0)
            if pd.notna(day3[f'EMA_{p}']): add(1 if day3[f'EMA_{p}']<day3['Close'] else -1 if day3[f'EMA_{p}']>day3['Close'] else 0)
        if (day3['ISA']>day3['ISB']) and (day3['IKS']>day3['ISA']) and (day3['ITS']>day3['IKS']) and (day3['Close']>day3['ITS']): add(1)
        elif (day3['ISA']<day3['ISB']) and (day3['IKS']<day3['ISA']) and (day3['ITS']<day3['IKS']) and (day3['Close']<day3['ITS']): add(-1)
        else: add(0)
        if day3['RSI']<30 and day3['RSI']>day2['RSI']: add(1)
        elif day3['RSI']>70 and day3['RSI']<day2['RSI']: add(-1)
        else: add(0)
        if day3['STOCH_K']<20 and day3['STOCH_D']<20 and day3['STOCH_K']>day3['STOCH_D']: add(1)
        elif day3['STOCH_K']>80 and day3['STOCH_D']>80 and day3['STOCH_K']<day3['STOCH_D']: add(-1)
        else: add(0)
        if day3['CCI']<-100 and day3['CCI']>day2['CCI']: add(1)
        elif day3['CCI']>100 and day3['CCI']<day2['CCI']: add(-1)
        else: add(0)
        if day3['+DI']>day3['-DI'] and day3['ADX']>20 and day3['ADX']>day2['ADX']: add(1)
        elif day3['+DI']<day3['-DI'] and day3['ADX']>20 and day3['ADX']>day2['ADX']: add(-1)
        else: add(0)
        ao_sb=(day3['AO']>0) and (d3['AO']>d4['AO']) and (d5['AO']>d4['AO'])
        ao_cb=(d4['AO']<0) and (d5['AO']>0)
        ao_ss=(day3['AO']<0) and (d3['AO']<d4['AO']) and (d5['AO']<d4['AO'])
        ao_cs=(d4['AO']>0) and (d5['AO']<0)
        if ao_sb or ao_cb: add(1)
        elif ao_ss or ao_cs: add(-1)
        else: add(0)
        if day3['MOM']>day2['MOM']: add(1)
        elif day3['MOM']<day2['MOM']: add(-1)
        else: add(0)
        if day3['MACD']>day3['MACD_SIGNAL']: add(1)
        elif day3['MACD']<day3['MACD_SIGNAL']: add(-1)
        else: add(0)
        tren_naik=day3['EMA_13']>day2['EMA_13']
        if not tren_naik and day3['SRSI_K']<20 and day3['SRSI_D']<20 and day3['SRSI_K']>day3['SRSI_D']: add(1)
        elif tren_naik and day3['SRSI_K']>80 and day3['SRSI_D']>80 and day3['SRSI_K']<day3['SRSI_D']: add(-1)
        else: add(0)
        if day3['WILLR']<-80 and day3['WILLR']>day2['WILLR']: add(1)
        elif day3['WILLR']>-20 and day3['WILLR']<day2['WILLR']: add(-1)
        else: add(0)
        if tren_naik and day3['BEARP']<0 and day3['BEARP']>day2['BEARP']: add(1)
        elif tren_naik and day3['BULLP']>0 and day3['BULLP']<day2['BULLP']: add(-1)
        else: add(0)
        if day3['UO']>70: add(1)
        elif day3['UO']<30: add(-1)
        else: add(0)

        tv_val=sc/cnt if cnt>0 else 0

        # Pattern detection
        BASE_PROB={'bull_abandoned':0.65,'3ws':0.60,'morning_star':0.60,'3_outside_up':0.58,
                   'bull_kicker':0.58,'bull_island':0.57,'3_inside_up':0.56,'rising_3':0.55,
                   'bull_engulf':0.54,'piercing':0.53,'tweezer_bottom':0.52,'bull_harami':0.51,
                   'hammer':0.52,'dragonfly_doji':0.50,'inv_hammer':0.50}

        pola,tier,pat_key="",0,""
        def ct(f): s=sum(f); return 3 if s>=4 else 2 if s>=2 else 1

        # Check each bullish pattern
        checks = []

        # 3 White Soldiers
        vacc=(day1['Volume']>day1['VOL_SMA_20']) and (day2['Volume']>=day1['Volume']) and (day3['Volume']>=day2['Volume'])
        if (is_down_basic and bull1 and bull2 and bull3 and body1>=0.5*range1 and body2>=0.5*range2 and body3>=0.5*range3 and
            day2['Close']>day1['Close'] and day3['Close']>day2['Close'] and
            day2['Open']>=day1['Open'] and day2['Open']<=day1['Close'] and
            day3['Open']>=day2['Open'] and day3['Open']<=day2['Close'] and
            close_near_high(day1) and close_near_high(day2) and close_near_high(day3) and candle_ok):
            checks.append(("Bullish: 3 White Soldiers", ct([vacc,ext_os,near_sup,macd_b,is_volatile]), "3ws"))

        # Abandoned Baby
        m1ab=(day1['Open']+day1['Close'])/2
        if (is_down_basic and bear1 and bull3 and body1>=0.5*range1 and body3>=0.5*range3 and
            (range2>0 and body2<=0.1*range2) and day2['High']<day1['Low'] and day2['High']<day3['Low'] and day3['Close']>m1ab):
            checks.append(("Bullish: Abandoned Baby", ct([ext_os,near_sup,vol_thrust,macd_b,True]), "bull_abandoned"))

        # Morning Star
        sm2=(range2>0) and (body2<=0.3*range2)
        if (is_proper_down and bear1 and range1>0 and body1>=0.6*range1 and sm2 and
            bull3 and range3>0 and body3>=0.6*range3 and day3['Close']>=mid1 and candle_ok):
            checks.append(("Bullish: Morning Star", ct([ext_os,near_sup,vol_thrust,macd_b,is_volatile]), "morning_star"))

        # 3 Inside Up
        bhi=(is_down_basic and bear1 and body1>=0.5*range1 and bull2 and body2<=0.6*body1 and bt2<=bt1 and bb2>=bb1)
        if bhi and bull3 and day3['Close']>day1['High']:
            checks.append(("Bullish: 3 Inside Up", ct([os_ctx,near_sup,vol_above,macd_b]), "3_inside_up"))

        # 3 Outside Up
        if (is_down_basic and bear1 and bull2 and day2['Close']>day1['Open'] and day2['Open']<day1['Close'] and
            body2>body1*1.2 and bull3 and body3>=0.5*range3 and day3['Close']>day2['High'] and candle_ok):
            checks.append(("Bullish: 3 Outside Up", ct([os_ctx,near_sup,vol_thrust,macd_b]), "3_outside_up"))

        # Bullish Engulfing
        if (bear2 and bull3 and day3['Close']>day2['Open'] and day3['Open']<day2['Close'] and
            body3>body2*1.08 and body2>=0.5*range2 and body3>=0.5*range3 and day3['Close']<day3['SMA_50'] and candle_ok):
            checks.append(("Bullish: Engulfing", ct([ext_os,near_sup,vol_thrust,ranging,macd_b]), "bull_engulf"))

        # Kicker
        if (bear2 and body2>=0.5*range2 and bull3 and body3>=0.5*range3 and
            day3['Open']>day2['High'] and day3['Low']>day2['High'] and candle_ok):
            checks.append(("Bullish: Kicker", ct([os_ctx,near_sup,vol_thrust,macd_b]), "bull_kicker"))

        # Island Reversal
        if (d1['Low']>max(d2['High'],d3['High'],d4['High']) and
            d5['Low']>max(d2['High'],d3['High'],d4['High']) and d5['Close']>d5['Open']):
            checks.append(("Bullish: Island Reversal", ct([os_ctx,vol_thrust,macd_b]), "bull_island"))

        # Piercing Line
        bbpl=day2['Open']-day2['Close']
        prpl=(day3['Close']-day2['Close'])/bbpl if bbpl>0 else 0
        if (is_proper_down and bear2 and bbpl>=0.6*range2 and bull3 and
            day3['Open']<day2['Close']*0.999 and prpl>=0.5 and day3['Close']<day2['Open'] and candle_ok):
            checks.append(("Bullish: Piercing Line", ct([ext_os,near_sup,vol_thrust,macd_b]), "piercing"))

        # Tweezer Bottom
        if (is_down_basic and bear2 and body2>=0.5*range2 and bull3 and body3>=0.5*range3 and
            is_near(day2['Low'],day3['Low']) and day3['Close']>day2['Close'] and
            (range3>=atr3*0.7 if valid_atr else True)):
            checks.append(("Bullish: Tweezer Bottom", ct([os_ctx,near_sup,vol_above,macd_b]), "tweezer_bottom"))

        # Harami
        if (is_down_basic and bear2 and body2>=0.5*range2 and bull3 and
            body3>=0.2*range3 and body3<=0.6*body2 and bt3<=bt2 and bb3>=bb2):
            checks.append(("Bullish: Harami", ct([os_ctx,near_sup,vol_above,macd_b]), "bull_harami"))

        # Hammer
        if (range3>0 and body3>0 and body3<=0.3*range3 and ls3>=2*body3 and
            us3<=0.15*range3 and (day3['High']-day3['Close'])<=0.25*range3 and
            day3['Close']<day3['SMA_20'] and candle_ok):
            checks.append(("Bullish: Hammer", ct([is_proper_down,ext_os,near_sup,vol_above]), "hammer"))

        # Inverted Hammer
        if (range3>0 and body3>0 and body3<=0.3*range3 and us3>=2*body3 and
            ls3<=0.15*range3 and (day3['Close']-day3['Low'])<=0.25*range3 and day3['SMA_20']<day3['SMA_50'] and candle_ok):
            checks.append(("Bullish: Inverted Hammer", ct([is_proper_down,os_ctx,near_sup]), "inv_hammer"))

        # Dragonfly Doji
        is_doji=range3>0 and body3<=0.08*range3
        if is_doji and ls3>=0.7*range3 and us3<=0.1*range3:
            checks.append(("Bullish: Dragonfly Doji", ct([os_ctx,near_sup]), "dragonfly_doji"))

        if not checks:
            return None

        # Pilih pola dengan tier tertinggi, lalu skor tertinggi
        best = max(checks, key=lambda x: (x[1], BASE_PROB.get(x[2], 0)))
        pola, tier, pat_key = best

        min_tier = params.get("MIN_TIER", 3)
        if tier < min_tier:
            return None

        cf = sum([near_sup, ext_os, vol_thrust, macd_b, os_ctx])
        pat_score = get_pattern_score(BASE_PROB.get(pat_key, 0.51), tier, cf)

        min_score = params.get("MIN_SCORE", 65)
        if pat_score <= min_score:
            return None

        sr_label = "Support" if near_sup else "Resistance" if pp>0.75 else "-"
        if sr_label != "Support":
            return None

        if not vol_thrust:
            return None

        return {
            "pola"       : pola + " " + get_stars(tier),
            "pola_base"  : pola,
            "tier"       : tier,
            "pat_score"  : pat_score,
            "tv_val"     : round(tv_val, 3),
            "sr_zone"    : sr_label,
            "vol_thrust" : vol_thrust,
            "sl_price"   : round(day3['Low'], 2),
            "signal_date": df.index[idx],
        }
    except:
        return None

# ============================================================
# EXIT SIMULATION WITH TRAILING STOP
# ============================================================
def simulate_exit_with_trailing(df, entry_idx, entry_price, sl_price, tp_price, params):
    max_hold    = params.get("MAX_HOLD_DAYS", 10)
    trail_act   = params.get("TRAILING_ACTIVATE", 0.04)
    trail_dist  = params.get("TRAILING_DISTANCE", 0.02)
    risk_ps     = entry_price - sl_price

    trailing_active = False
    trailing_sl     = sl_price
    highest_price   = entry_price

    for j in range(1, max_hold + 1):
        hold_idx = entry_idx + j
        if hold_idx >= len(df):
            hi = len(df)-1
            return df.index[hi], df['Close'].iloc[hi], "Time Exit", highest_price

        day_open  = df['Open'].iloc[hold_idx]
        day_high  = df['High'].iloc[hold_idx]
        day_low   = df['Low'].iloc[hold_idx]
        day_close = df['Close'].iloc[hold_idx]

        # Update highest intraday
        if day_high > highest_price:
            highest_price = day_high

        # Aktifkan trailing stop jika sudah profit >= trail_act
        profit_pct = (highest_price - entry_price) / entry_price
        if profit_pct >= trail_act:
            trailing_active = True

        # Update trailing SL
        if trailing_active:
            new_trail_sl = highest_price * (1 - trail_dist)
            trailing_sl = max(trailing_sl, new_trail_sl)

        current_sl = trailing_sl if trailing_active else sl_price

        # Check hits
        hit_sl = day_low <= current_sl
        hit_tp = day_high >= tp_price

        if hit_sl and hit_tp:
            if day_open <= current_sl:
                return df.index[hold_idx], min(day_open, current_sl), "SL", highest_price
            else:
                return df.index[hold_idx], tp_price, "TP", highest_price
        elif hit_sl:
            exit_p = max(day_open, current_sl) if day_open < current_sl else current_sl
            reason = "Trailing SL" if trailing_active else "SL"
            return df.index[hold_idx], exit_p, reason, highest_price
        elif hit_tp:
            return df.index[hold_idx], tp_price, "TP", highest_price
        elif j == max_hold:
            return df.index[hold_idx], day_close, "Time Exit", highest_price

    last = min(entry_idx + max_hold, len(df)-1)
    return df.index[last], df['Close'].iloc[last], "Time Exit", highest_price

# ============================================================
# IHSG REGIME FILTER
# ============================================================
def load_ihsg_data(start_date, end_date):
    try:
        df = yf.download("^JKSE", start=start_date.strftime("%Y-%m-%d"),
                         end=end_date.strftime("%Y-%m-%d"), interval="1d",
                         progress=False, auto_adjust=True)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(1)
        df['SMA50'] = df['Close'].rolling(50).mean()
        df.dropna(inplace=True)
        return df
    except:
        return None

def is_bull_market(ihsg_df, check_date):
    if ihsg_df is None:
        return True
    try:
        row = ihsg_df[ihsg_df.index <= check_date].iloc[-1]
        return row['Close'] > row['SMA50']
    except:
        return True

# ============================================================
# CORE BACKTEST FUNCTION
# ============================================================
def run_full_backtest(all_ticker_data, ihsg_df, params,
                      bt_start, bt_end, modal_awal=None):
    """
    Jalankan backtest penuh dengan parameter tertentu.
    Return: (portfolio_list, final_equity)
    """
    if modal_awal is None:
        modal_awal = MODAL_AWAL

    all_signals = []

    for ticker, df_ind in all_ticker_data.items():
        df_range = df_ind[(df_ind.index >= bt_start) & (df_ind.index <= bt_end)]
        if len(df_range) < 60:
            continue

        for i in range(5, len(df_range)-1):
            full_idx = df_ind.index.get_loc(df_range.index[i])
            if full_idx < 5:
                continue
            sig = generate_signal(df_ind, full_idx, params)
            if sig is None:
                continue

            # IHSG regime filter
            if params.get("USE_IHSG_FILTER", True):
                if not is_bull_market(ihsg_df, sig['signal_date']):
                    continue

            next_i = i + 1
            if next_i >= len(df_range):
                continue

            entry_date  = df_range.index[next_i]
            entry_price = df_range['Open'].iloc[next_i]
            if entry_price <= 0 or pd.isna(entry_price):
                continue

            sl_price    = sig['sl_price']
            risk_ps     = entry_price - sl_price
            if risk_ps <= 0:
                continue

            rr      = params.get("RR_TARGET", 2.0)
            tp_price= entry_price + risk_ps * rr

            all_signals.append({
                "ticker"      : ticker,
                "signal_date" : sig['signal_date'],
                "entry_date"  : entry_date,
                "entry_price" : entry_price,
                "sl_price"    : sl_price,
                "tp_price"    : tp_price,
                "risk_ps"     : risk_ps,
                "pola"        : sig['pola'],
                "pola_base"   : sig['pola_base'],
                "tier"        : sig['tier'],
                "pat_score"   : sig['pat_score'],
                "_entry_idx_in_range": next_i,
                "_df_range"   : df_range,
            })

    # Sort: entry_date ↑, tier ↓, skor ↓
    all_signals.sort(key=lambda x: (x['entry_date'], -x['tier'], -x['pat_score']))

    # Portfolio simulation
    equity     = float(modal_awal)
    portfolio  = []
    active_end = None

    for sig in all_signals:
        ed = sig['entry_date']
        if active_end is not None and ed <= active_end:
            continue

        entry_price = sig['entry_price']
        max_shares  = int((equity / entry_price) // LOT_SIZE) * LOT_SIZE
        if max_shares <= 0:
            continue

        lot_count    = max_shares // LOT_SIZE
        total_shares = lot_count * LOT_SIZE
        total_invest = total_shares * entry_price

        df_range   = sig['_df_range']
        entry_idx  = sig['_entry_idx_in_range']

        exit_date, exit_price, exit_reason, peak_price = simulate_exit_with_trailing(
            df_range, entry_idx, entry_price, sig['sl_price'], sig['tp_price'], params
        )

        pnl_rp     = (exit_price - entry_price) * total_shares
        return_pct = (exit_price - entry_price) / entry_price * 100
        r_mult     = (exit_price - entry_price) / sig['risk_ps'] if sig['risk_ps'] > 0 else 0
        equity_after = equity + pnl_rp
        active_end   = exit_date

        portfolio.append({
            "Ticker"          : sig['ticker'],
            "Pola"            : sig['pola'],
            "Pola Base"       : sig['pola_base'],
            "Tier"            : sig['tier'],
            "Skor Pola"       : sig['pat_score'],
            "Entry Date"      : ed.strftime("%Y-%m-%d") if hasattr(ed,'strftime') else str(ed),
            "Entry Price"     : round(entry_price, 0),
            "Lot Size"        : lot_count,
            "Total Investasi" : round(total_invest, 0),
            "Stop Loss"       : round(sig['sl_price'], 0),
            "Target (TP)"     : round(sig['tp_price'], 0),
            "Exit Date"       : exit_date.strftime("%Y-%m-%d") if hasattr(exit_date,'strftime') else str(exit_date),
            "Exit Price"      : round(exit_price, 0),
            "Exit Reason"     : exit_reason,
            "Profit/Loss (Rp)": round(pnl_rp, 0),
            "Return %"        : round(return_pct, 2),
            "Equity Setelah"  : round(equity_after, 0),
            "R Multiple"      : round(r_mult, 2),
        })
        equity = equity_after

    return portfolio, equity

# ============================================================
# SUMMARY STATS
# ============================================================
def calc_summary_stats(portfolio, final_equity, modal_awal=None, years=BACKTEST_YEARS):
    if modal_awal is None:
        modal_awal = MODAL_AWAL
    if not portfolio:
        return {"error": "no trades"}

    df = pd.DataFrame(portfolio)
    n  = len(df)
    wins  = df[df['Profit/Loss (Rp)']>0]
    loses = df[df['Profit/Loss (Rp)']<=0]
    wr    = len(wins)/n*100 if n>0 else 0

    avg_w = wins['Return %'].mean()  if len(wins)>0  else 0
    avg_l = loses['Return %'].mean() if len(loses)>0 else 0

    gp = wins['Profit/Loss (Rp)'].sum()   if len(wins)>0  else 0
    gl = abs(loses['Profit/Loss (Rp)'].sum()) if len(loses)>0 else 0
    pf = gp/gl if gl>0 else 999

    r_w = wins['R Multiple'].mean()  if len(wins)>0  else 0
    r_l = loses['R Multiple'].mean() if len(loses)>0 else 0
    exp = (wr/100)*r_w + (1-wr/100)*r_l

    eq = [modal_awal] + list(df['Equity Setelah'].astype(float))
    eq_s = pd.Series(eq)
    dd   = ((eq_s - eq_s.cummax()) / eq_s.cummax() * 100).min()

    total_ret = (final_equity - modal_awal) / modal_awal
    cagr = ((1+total_ret)**(1/years)-1)*100 if years>0 else 0

    # Time Exit ratio
    te_count = len(df[df['Exit Reason']=='Time Exit'])
    sl_count = len(df[df['Exit Reason'].str.contains('SL', na=False)])
    tp_count = len(df[df['Exit Reason']=='TP'])

    return {
        "Modal Awal"       : modal_awal,
        "Modal Akhir"      : round(final_equity, 0),
        "Total Return %"   : round(total_ret*100, 2),
        "Total Trade"      : n,
        "Win Rate %"       : round(wr, 1),
        "Total Win"        : len(wins),
        "Total Loss"       : len(loses),
        "TP Hit"           : tp_count,
        "SL Hit"           : sl_count,
        "Time Exit"        : te_count,
        "Avg Win %"        : round(avg_w, 2),
        "Avg Loss %"       : round(avg_l, 2),
        "Profit Factor"    : round(pf, 2),
        "Expectancy (R)"   : round(exp, 3),
        "Max Drawdown %"   : round(dd, 2),
        f"CAGR {years}Y %"  : round(cagr, 2),
        "Gross Profit"     : round(gp, 0),
        "Gross Loss"       : round(gl, 0),
    }

# ============================================================
# PARAMETER OPTIMIZATION (GRID SEARCH)
# ============================================================
def run_optimization(all_ticker_data, ihsg_df, bt_start, bt_end):
    print("\n🔍 PARAMETER OPTIMIZATION (Grid Search)...")

    param_grid = {
        "MAX_HOLD_DAYS"     : [7, 10, 15],
        "RR_TARGET"         : [1.5, 2.0, 2.5],
        "MIN_SCORE"         : [60, 65, 70],
        "TRAILING_ACTIVATE" : [0.03, 0.04, 0.05],
        "TRAILING_DISTANCE" : [0.015, 0.02, 0.025],
    }

    fixed_params = {
        "MIN_TIER"            : 3,
        "USE_IHSG_FILTER"     : True,
        "USE_LIQUIDITY_FILTER": True,
        "MIN_VOLUME_IDR"      : 500_000_000,
    }

    keys   = list(param_grid.keys())
    combos = list(itertools.product(*[param_grid[k] for k in keys]))
    total  = len(combos)
    print(f"   Total kombinasi: {total}")

    results = []
    for i, combo in enumerate(combos):
        params = dict(zip(keys, combo))
        params.update(fixed_params)

        try:
            portfolio, final_eq = run_full_backtest(
                all_ticker_data, ihsg_df, params, bt_start, bt_end
            )
            if len(portfolio) < 5:
                continue

            stats = calc_summary_stats(portfolio, final_eq)
            results.append({
                "MAX_HOLD_DAYS"     : params['MAX_HOLD_DAYS'],
                "RR_TARGET"         : params['RR_TARGET'],
                "MIN_SCORE"         : params['MIN_SCORE'],
                "TRAILING_ACTIVATE" : params['TRAILING_ACTIVATE'],
                "TRAILING_DISTANCE" : params['TRAILING_DISTANCE'],
                "Total Trade"       : stats['Total Trade'],
                "Win Rate %"        : stats['Win Rate %'],
                "Profit Factor"     : stats['Profit Factor'],
                "Expectancy (R)"    : stats['Expectancy (R)'],
                "Max Drawdown %"    : stats['Max Drawdown %'],
                f"CAGR"             : stats[f"CAGR {BACKTEST_YEARS}Y %"],
                "Total Return %"    : stats['Total Return %'],
                "Modal Akhir"       : stats['Modal Akhir'],
            })

            if (i+1) % 50 == 0:
                print(f"   Progress: {i+1}/{total}")
        except:
            continue

    if not results:
        print("   ❌ Optimization gagal — tidak ada kombinasi valid")
        return None, DEFAULT_PARAMS.copy()

    df_res = pd.DataFrame(results)
    # Sort: Expectancy ↓, Profit Factor ↓, Max DD ↑ (least negative)
    df_res = df_res.sort_values(
        by=['Expectancy (R)', 'Profit Factor', 'Max Drawdown %'],
        ascending=[False, False, False]
    )

    best_row = df_res.iloc[0]
    best_params = {
        "MAX_HOLD_DAYS"       : int(best_row['MAX_HOLD_DAYS']),
        "RR_TARGET"           : float(best_row['RR_TARGET']),
        "MIN_SCORE"           : float(best_row['MIN_SCORE']),
        "TRAILING_ACTIVATE"   : float(best_row['TRAILING_ACTIVATE']),
        "TRAILING_DISTANCE"   : float(best_row['TRAILING_DISTANCE']),
        "MIN_TIER"            : 3,
        "USE_IHSG_FILTER"     : True,
        "USE_LIQUIDITY_FILTER": True,
        "MIN_VOLUME_IDR"      : 500_000_000,
    }

    print(f"   ✅ Best params ditemukan:")
    for k, v in best_params.items():
        print(f"      {k}: {v}")

    return df_res, best_params

# ============================================================
# WALK-FORWARD VALIDATION
# ============================================================
def run_walk_forward(all_ticker_data, ihsg_df, end_date):
    print("\n🚶 WALK-FORWARD VALIDATION (3 windows)...")

    total_days = BACKTEST_YEARS * 365
    # Split: 75% train, 25% test per window, geser 6 bulan
    windows = []
    for i in range(3):
        offset      = i * 180  # geser 6 bulan
        w_end       = end_date - timedelta(days=offset)
        w_start     = w_end - timedelta(days=total_days)
        train_end   = w_start + timedelta(days=int(total_days * 0.75))
        test_start  = train_end + timedelta(days=1)
        test_end    = w_end
        windows.append({
            "window"      : i+1,
            "train_start" : w_start,
            "train_end"   : train_end,
            "test_start"  : test_start,
            "test_end"    : test_end,
        })

    wf_results = []
    for w in windows:
        print(f"\n   Window {w['window']}:")
        print(f"   Train: {w['train_start'].date()} → {w['train_end'].date()}")
        print(f"   Test : {w['test_start'].date()} → {w['test_end'].date()}")

        # Optimize on train
        _, best_p = run_optimization(all_ticker_data, ihsg_df,
                                      w['train_start'], w['train_end'])

        # Test on out-of-sample
        port_test, eq_test = run_full_backtest(
            all_ticker_data, ihsg_df, best_p,
            w['test_start'], w['test_end']
        )
        stats_test = calc_summary_stats(
            port_test, eq_test,
            years=(w['test_end']-w['test_start']).days/365
        )

        # Train performance (with best params)
        port_train, eq_train = run_full_backtest(
            all_ticker_data, ihsg_df, best_p,
            w['train_start'], w['train_end']
        )
        stats_train = calc_summary_stats(
            port_train, eq_train,
            years=(w['train_end']-w['train_start']).days/365
        )

        wf_results.append({
            "Window"                  : w['window'],
            "Train Period"            : f"{w['train_start'].date()} → {w['train_end'].date()}",
            "Test Period"             : f"{w['test_start'].date()} → {w['test_end'].date()}",
            "Best MAX_HOLD"           : best_p['MAX_HOLD_DAYS'],
            "Best RR"                 : best_p['RR_TARGET'],
            "Best MIN_SCORE"          : best_p['MIN_SCORE'],
            "Train Win Rate %"        : stats_train.get('Win Rate %', 0),
            "Train Expectancy"        : stats_train.get('Expectancy (R)', 0),
            "Train PF"                : stats_train.get('Profit Factor', 0),
            "Train Total Return %"    : stats_train.get('Total Return %', 0),
            "TEST Win Rate %"         : stats_test.get('Win Rate %', 0),
            "TEST Expectancy"         : stats_test.get('Expectancy (R)', 0),
            "TEST PF"                 : stats_test.get('Profit Factor', 0),
            "TEST Total Return %"     : stats_test.get('Total Return %', 0),
            "Consistency"             : "✅ OK" if abs(stats_test.get('Win Rate %',0) - stats_train.get('Win Rate %',0)) < 15 else "⚠️ Diverge",
        })
        print(f"   Train WR: {stats_train.get('Win Rate %',0):.1f}%  →  Test WR: {stats_test.get('Win Rate %',0):.1f}%")
        print(f"   Train Exp: {stats_train.get('Expectancy (R)',0):.3f}R  →  Test Exp: {stats_test.get('Expectancy (R)',0):.3f}R")

    return pd.DataFrame(wf_results)

# ============================================================
# MONTE CARLO SIMULATION
# ============================================================
def run_monte_carlo(portfolio, n_sim=10000):
    print(f"\n🎲 MONTE CARLO ({n_sim:,} simulasi)...")

    if not portfolio:
        return pd.DataFrame()

    df      = pd.DataFrame(portfolio)
    returns = df['Return %'].values / 100  # decimal
    n_trades= len(returns)

    final_equities = []
    max_drawdowns  = []
    win_rates      = []

    for _ in range(n_sim):
        # Acak urutan trade
        shuffled = np.random.choice(returns, size=n_trades, replace=True)
        equity   = float(MODAL_AWAL)
        peak     = equity
        max_dd   = 0.0
        wins     = 0

        for r in shuffled:
            equity *= (1 + r)
            if equity > peak:
                peak = equity
            dd = (equity - peak) / peak * 100
            if dd < max_dd:
                max_dd = dd
            if r > 0:
                wins += 1

        final_equities.append(equity)
        max_drawdowns.append(max_dd)
        win_rates.append(wins / n_trades * 100)

    fe   = np.array(final_equities)
    mdd  = np.array(max_drawdowns)

    mc_summary = {
        "Simulasi"             : n_sim,
        "Median Modal Akhir"   : round(np.median(fe), 0),
        "Mean Modal Akhir"     : round(np.mean(fe), 0),
        "P5  Modal Akhir (Worst 5%)"  : round(np.percentile(fe, 5), 0),
        "P25 Modal Akhir"      : round(np.percentile(fe, 25), 0),
        "P75 Modal Akhir"      : round(np.percentile(fe, 75), 0),
        "P95 Modal Akhir (Best 5%)"   : round(np.percentile(fe, 95), 0),
        "Prob Profit %"        : round((fe > MODAL_AWAL).mean()*100, 1),
        "Prob DD > 20%"        : round((mdd < -20).mean()*100, 1),
        "Prob DD > 30%"        : round((mdd < -30).mean()*100, 1),
        "Median Max DD %"      : round(np.median(mdd), 2),
        "Worst Max DD %"       : round(np.percentile(mdd, 5), 2),
    }

    print(f"   Prob Profit  : {mc_summary['Prob Profit %']}%")
    print(f"   Median Akhir : Rp {mc_summary['Median Modal Akhir']:,.0f}")
    print(f"   Worst 5%     : Rp {mc_summary['P5  Modal Akhir (Worst 5%)']:,.0f}")

    # Distribution untuk upload
    mc_rows = [[k, v] for k, v in mc_summary.items()]

    # Histogram buckets
    bins    = np.linspace(fe.min(), fe.max(), 21)
    hist, _ = np.histogram(fe, bins=bins)
    dist_df = pd.DataFrame({
        "Range Bawah" : [round(bins[i],0) for i in range(len(bins)-1)],
        "Range Atas"  : [round(bins[i+1],0) for i in range(len(bins)-1)],
        "Frekuensi"   : hist,
        "Probabilitas%": [round(h/n_sim*100, 2) for h in hist],
    })

    return mc_summary, mc_rows, dist_df

# ============================================================
# PER-PATTERN ANALYSIS
# ============================================================
def analyze_per_pattern(portfolio):
    if not portfolio:
        return pd.DataFrame()

    df = pd.DataFrame(portfolio)
    df['Win'] = df['Profit/Loss (Rp)'] > 0

    pattern_stats = []
    for pola, group in df.groupby('Pola Base'):
        n     = len(group)
        if n < 2:
            continue
        wins  = group[group['Win']==True]
        loses = group[group['Win']==False]
        wr    = len(wins)/n*100

        gp = wins['Profit/Loss (Rp)'].sum()   if len(wins)>0  else 0
        gl = abs(loses['Profit/Loss (Rp)'].sum()) if len(loses)>0 else 0
        pf = gp/gl if gl>0 else 999

        avg_r_w = wins['R Multiple'].mean()  if len(wins)>0  else 0
        avg_r_l = loses['R Multiple'].mean() if len(loses)>0 else 0
        exp     = (wr/100)*avg_r_w + (1-wr/100)*avg_r_l

        tp_c  = len(group[group['Exit Reason']=='TP'])
        sl_c  = len(group[group['Exit Reason'].str.contains('SL', na=False)])
        te_c  = len(group[group['Exit Reason']=='Time Exit'])
        trail_c = len(group[group['Exit Reason']=='Trailing SL'])

        verdict = "✅ Pertahankan" if (wr >= 50 and pf >= 1.2) else \
                  "⚠️ Review"      if (wr >= 45 or pf >= 1.0)  else \
                  "❌ Hapus"

        pattern_stats.append({
            "Pola"              : pola,
            "Total Trade"       : n,
            "Win"               : len(wins),
            "Loss"              : len(loses),
            "Win Rate %"        : round(wr, 1),
            "Avg Win %"         : round(wins['Return %'].mean(), 2) if len(wins)>0 else 0,
            "Avg Loss %"        : round(loses['Return %'].mean(), 2) if len(loses)>0 else 0,
            "Avg R Win"         : round(avg_r_w, 2),
            "Avg R Loss"        : round(avg_r_l, 2),
            "Profit Factor"     : round(pf, 2),
            "Expectancy (R)"    : round(exp, 3),
            "TP Hit"            : tp_c,
            "SL Hit"            : sl_c,
            "Trailing SL Hit"   : trail_c,
            "Time Exit"         : te_c,
            "Total P/L (Rp)"    : round(group['Profit/Loss (Rp)'].sum(), 0),
            "Verdict"           : verdict,
        })

    df_out = pd.DataFrame(pattern_stats)
    if not df_out.empty:
        df_out = df_out.sort_values('Expectancy (R)', ascending=False)
    return df_out

# ============================================================
# BENCHMARK: IHSG BUY & HOLD
# ============================================================
def calc_ihsg_benchmark(ihsg_df, bt_start, bt_end):
    if ihsg_df is None:
        return {}
    try:
        df = ihsg_df[(ihsg_df.index >= bt_start) & (ihsg_df.index <= bt_end)]
        if len(df) < 2:
            return {}
        start_p = df['Close'].iloc[0]
        end_p   = df['Close'].iloc[-1]
        total_ret = (end_p - start_p) / start_p * 100
        cagr    = ((end_p/start_p)**(1/BACKTEST_YEARS)-1)*100
        return {
            "IHSG Start Price"     : round(start_p, 0),
            "IHSG End Price"       : round(end_p, 0),
            "IHSG Total Return %"  : round(total_ret, 2),
            "IHSG CAGR %"          : round(cagr, 2),
        }
    except:
        return {}

# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    print("=" * 65)
    print("🤖 BACKTESTING ENGINE v2.0 — FULL OPTIMIZATION SUITE")
    print(f"   Modal Awal  : Rp {MODAL_AWAL:,.0f}")
    print(f"   Periode     : {BACKTEST_YEARS} Tahun")
    print(f"   Universe    : {len(ALL_TICKERS)} saham")
    print("=" * 65)

    end_date   = datetime.now()
    start_date = end_date - timedelta(days=BACKTEST_YEARS*365 + 90)
    bt_start   = end_date - timedelta(days=BACKTEST_YEARS*365)

    # ── 1. Download semua data ──────────────────────────────
    print(f"\n📥 Download data {len(ALL_TICKERS)} saham...")
    all_ticker_data = {}

    for i, ticker in enumerate(ALL_TICKERS):
        try:
            df_raw = yf.download(
                ticker,
                start=start_date.strftime("%Y-%m-%d"),
                end=end_date.strftime("%Y-%m-%d"),
                interval="1d", progress=False, auto_adjust=True
            )
            if df_raw.empty or len(df_raw) < 60:
                continue
            if isinstance(df_raw.columns, pd.MultiIndex):
                df_raw.columns = df_raw.columns.droplevel(1)
            df_raw.dropna(inplace=True)
            df_ind = calc_indicators(df_raw.copy())
            df_ind.dropna(inplace=True)
            if len(df_ind) >= 60:
                all_ticker_data[ticker] = df_ind
            if (i+1) % 30 == 0:
                print(f"   [{i+1}/{len(ALL_TICKERS)}] {len(all_ticker_data)} berhasil")
        except:
            continue

    print(f"✅ Data siap: {len(all_ticker_data)} saham")

    # ── 2. IHSG ─────────────────────────────────────────────
    print("\n📊 Download IHSG data...")
    ihsg_df = load_ihsg_data(start_date, end_date)
    ihsg_bm = calc_ihsg_benchmark(ihsg_df, bt_start, end_date)
    print(f"   IHSG Return: {ihsg_bm.get('IHSG Total Return %','N/A')}%  |  CAGR: {ihsg_bm.get('IHSG CAGR %','N/A')}%")

    # ── 3. Baseline (parameter default) ─────────────────────
    print("\n📌 Baseline backtest (parameter default)...")
    base_params = DEFAULT_PARAMS.copy()
    port_base, eq_base = run_full_backtest(all_ticker_data, ihsg_df, base_params, bt_start, end_date)
    stats_base = calc_summary_stats(port_base, eq_base)
    print(f"   Trade: {stats_base['Total Trade']}  WR: {stats_base['Win Rate %']}%  Exp: {stats_base['Expectancy (R)']}R")

    # ── 4. Parameter Optimization ───────────────────────────
    df_optim, best_params = run_optimization(all_ticker_data, ihsg_df, bt_start, end_date)

    # ── 5. Final backtest dengan best params ─────────────────
    print("\n🏆 Final backtest dengan best parameters...")
    port_best, eq_best = run_full_backtest(all_ticker_data, ihsg_df, best_params, bt_start, end_date)
    stats_best = calc_summary_stats(port_best, eq_best)
    print(f"   Trade: {stats_best['Total Trade']}  WR: {stats_best['Win Rate %']}%  Exp: {stats_best['Expectancy (R)']}R  CAGR: {stats_best.get(f'CAGR {BACKTEST_YEARS}Y %', '?')}%")

    # ── 6. Walk-Forward ──────────────────────────────────────
    df_wf = run_walk_forward(all_ticker_data, ihsg_df, end_date)

    # ── 7. Monte Carlo ───────────────────────────────────────
    mc_summary, mc_rows, mc_dist = run_monte_carlo(port_best, n_sim=10000)

    # ── 8. Per-Pattern Analysis ──────────────────────────────
    print("\n🔬 Per-Pattern Analysis...")
    df_pattern = analyze_per_pattern(port_best)
    if not df_pattern.empty:
        print(df_pattern[['Pola','Total Trade','Win Rate %','Profit Factor','Expectancy (R)','Verdict']].to_string(index=False))

    # ── 9. Upload ke Google Sheets ───────────────────────────
    print("\n📤 Upload ke Google Sheets...")

    # Sheet: Transaksi (best params)
    if port_best:
        upload_df("Transaksi", pd.DataFrame(port_best))

    # Sheet: Summary
    summary_rows = [["Metrik", "Nilai"], ["", ""], ["=== BEST PARAMS ===", ""]]
    for k, v in stats_best.items():
        summary_rows.append([k, f"Rp {v:,.0f}" if isinstance(v, float) and v > 1000 else v])
    summary_rows += [["", ""], ["=== BENCHMARK IHSG ===", ""]]
    for k, v in ihsg_bm.items():
        summary_rows.append([k, v])
    summary_rows += [["", ""], ["=== BASELINE (Default Params) ===", ""]]
    for k, v in stats_base.items():
        summary_rows.append([k, v])
    summary_rows += [["", ""], ["=== BEST PARAMETERS ===", ""]]
    for k, v in best_params.items():
        summary_rows.append([k, v])
    upload_rows("Summary", summary_rows)

    # Sheet: Optimasi
    if df_optim is not None and not df_optim.empty:
        upload_df("Optimasi", df_optim)

    # Sheet: Walk Forward
    if not df_wf.empty:
        upload_df("Walk Forward", df_wf)

    # Sheet: Monte Carlo
    mc_df = pd.DataFrame(mc_rows, columns=["Metrik","Nilai"])
    mc_dist_combined = pd.concat([
        mc_df,
        pd.DataFrame([["",""],["=== DISTRIBUSI MODAL AKHIR ===",""]],columns=["Metrik","Nilai"]),
        mc_dist.rename(columns={"Range Bawah":"Metrik","Range Atas":"Nilai","Frekuensi":"","Probabilitas%":""})
    ], ignore_index=True)
    upload_df("Monte Carlo", mc_dist_combined)
    # Upload distribusi terpisah
    upload_df("MC Distribusi", mc_dist)

    # Sheet: Per Pattern
    if not df_pattern.empty:
        upload_df("Per Pattern", df_pattern)

    print("\n" + "=" * 65)
    print("🏁 SELESAI — Semua hasil sudah di-upload ke Google Sheets")
    print("=" * 65)
    print(f"\nSheet yang tersedia:")
    print("  📋 Summary        — Ringkasan + Benchmark + Best Params")
    print("  📊 Transaksi      — Detail setiap trade (best params)")
    print("  🔍 Optimasi       — Semua kombinasi parameter + hasil")
    print("  🚶 Walk Forward   — Validasi out-of-sample 3 windows")
    print("  🎲 Monte Carlo    — Distribusi probabilitas 10.000 sim")
    print("  📈 MC Distribusi  — Histogram modal akhir")
    print("  🔬 Per Pattern    — Breakdown performa per pola")
    print("=" * 65)
