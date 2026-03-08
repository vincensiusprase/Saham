"""
╔══════════════════════════════════════════════════════════════════╗
║     IHSG CHANNEL BREAKOUT SCREENER — VERSION 2.2                 ║
║     Fixed: BB Logic | Signal Date Tracking | Clean Columns       ║
║     Sector Framework | Tier System | SL/TP | Commodity Index     ║
╚══════════════════════════════════════════════════════════════════╝

Scoring (8 base conditions, each = +1):
  1. Liquidity       : Price × AvgVol20D > 10B IDR
  2. ATR             : ATR14 antara 1%–3% dari harga
  3. Price Change 1W : antara -3% dan +3%
  4. Ichimoku Cloud  : Close > max(SpanA, SpanB)
  5. MACD Bullish    : MACD Line > Signal Line
  6. Volume Surge    : Volume > VolMA20 × 1.5
  7. BB Strategy     : Close cross UP dari Lower Band (kemarin < lower, hari ini ≥ lower)
  8. Supertrend      : Supertrend direction = Bullish (ST di bawah harga)
  9. RSI Strategy    : RSI cross UP dari Oversold (30)

Signal Date Tracking:
  - ChBrkLE / ChBrkSE  : tanggal dan berapa hari lalu sinyal channel breakout keluar
  - STBrkLE / STBrkSE  : tanggal dan berapa hari lalu sinyal supertrend flip keluar
  - BBBrkLE / BBBrkSE  : tanggal cross BB lower band
  - RSIBrkLE           : tanggal RSI cross oversold

Output    : Google Spreadsheet (per-sheet per-sector + SCREENER summary)
Schedule  : Daily via GitHub Actions (07:00 WIB)
"""

import os
import sys
import json
import time
import warnings
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
import yfinance as yf
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials

warnings.filterwarnings("ignore")

# ══════════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════════
SPREADSHEET_ID  = "1qhEZkfdtEGDEA5tWTVCeOahOXDso3H4O7B69xkOREDA"
SUMMARY_SHEET   = "SCREENER"
SERVICE_ACCOUNT = "service_account.json"

CHANNEL_LENGTH   = 5
BASE_MIN_SCORE   = 4
MIN_SCORE_STRONG = 3
MIN_SCORE_STRICT = 6

ATR_MIN_PCT      = 1.0
ATR_MAX_PCT      = 3.0
MIN_LIQUIDITY    = 10_000_000_000
VOLUME_SURGE     = 1.5
PRICE_CHANGE_MAX = 3.0

TENKAN  = 9
KIJUN   = 26
SENKOU  = 52

MACD_FAST    = 12
MACD_SLOW    = 26
MACD_SIGNAL  = 9

BB_LENGTH    = 20
BB_MULT      = 2.0

ST_ATR_LEN   = 10
ST_FACTOR    = 3.0

RSI_LENGTH    = 14
RSI_OVERSOLD  = 30
RSI_OVERBOUGHT= 70

DOWNLOAD_DAYS = 300
WIB           = timezone(timedelta(hours=7))

# Jumlah candle lookback untuk mencari tanggal sinyal terakhir
SIGNAL_LOOKBACK = 10   # cari sinyal dalam 10 candle terakhir


# ══════════════════════════════════════════════════════════════════
# SECTOR CONFIG
# ══════════════════════════════════════════════════════════════════
SECTOR_CONFIG = {
    "IDXFINANCE": [
        "BBCA.JK","BBRI.JK","BMRI.JK","BBNI.JK","BRIS.JK",
        "BBTN.JK","BNGA.JK","NISP.JK","BDMN.JK","BTPS.JK",
    ],
    "IDXENERGY": [
        "ADRO.JK","BYAN.JK","PTBA.JK","ITMG.JK","HRUM.JK",
        "MEDC.JK","PGAS.JK","ELSA.JK","AKRA.JK","INDY.JK",
    ],
    "IDXBASIC": [
        "ANTM.JK","MDKA.JK","INCO.JK","TINS.JK","SMGR.JK",
        "INTP.JK","TPIA.JK","INKP.JK","TKIM.JK","MBMA.JK",
    ],
    "IDXINDUST": [
        "ASII.JK","UNTR.JK","SCCO.JK","KBLI.JK","VOKS.JK",
        "AMFG.JK","ARNA.JK","TOTO.JK","LION.JK","ASGR.JK",
    ],
    "IDXNONCYC": [
        "UNVR.JK","ICBP.JK","INDF.JK","MYOR.JK","CPIN.JK",
        "JPFA.JK","ULTJ.JK","SOFA.JK","KLBF.JK","GGRM.JK",
    ],
    "IDXCYCLIC": [
        "MAPI.JK","ACES.JK","ERAA.JK","LPPF.JK","MNCN.JK",
        "SCMA.JK","AUTO.JK","GJTL.JK","SMSM.JK","FAST.JK",
    ],
    "IDXTECHNO": [
        "GOTO.JK","EMTK.JK","BUKA.JK","MSTI.JK","MTDL.JK",
        "MLPT.JK","CASH.JK","KREN.JK","HDIT.JK","NFCX.JK",
    ],
    "IDXHEALTH": [
        "KLBF.JK","SIDO.JK","KAEF.JK","TSPC.JK","MIKA.JK",
        "SILO.JK","HEAL.JK","MERK.JK","DVLA.JK","PRDA.JK",
    ],
    "IDXINFRA": [
        "TLKM.JK","EXCL.JK","ISAT.JK","TOWR.JK","JSMR.JK",
        "TBIG.JK","MTEL.JK","WIKA.JK","PTPP.JK","ADHI.JK",
    ],
    "IDXPROPERT": [
        "CTRA.JK","BSDE.JK","PWON.JK","SMRA.JK","ASRI.JK",
        "LPKR.JK","DMAS.JK","DUTI.JK","MTLA.JK","BEST.JK",
    ],
    "IDXTRANS": [
        "GIAA.JK","SMDR.JK","BIRD.JK","ASSA.JK","TMAS.JK",
        "JSMR.JK","WEHA.JK","NELY.JK","SAFE.JK","LRNA.JK",
    ],
}

# ══════════════════════════════════════════════════════════════════
# COMMODITY CONFIG
# ══════════════════════════════════════════════════════════════════
COMMODITY_CONFIG = {
    "Gold"      : {"ticker":"GC=F",     "ma":20, "sectors":["IDXBASIC","IDXFINANCE"]},
    "Silver"    : {"ticker":"SI=F",     "ma":20, "sectors":["IDXBASIC","IDXINDUST"]},
    "Copper"    : {"ticker":"HG=F",     "ma":20, "sectors":["IDXBASIC","IDXINDUST","IDXINFRA"]},
    "Nickel"    : {"ticker":"NI=F",     "ma":20, "sectors":["IDXBASIC","IDXINDUST"]},
    "Aluminium" : {"ticker":"ALI=F",    "ma":20, "sectors":["IDXBASIC","IDXINDUST"]},
    "Zinc"      : {"ticker":"ZNC=F",    "ma":20, "sectors":["IDXBASIC","IDXINDUST"]},
    "Tin"       : {"ticker":"JJT",      "ma":20, "sectors":["IDXBASIC"]},
    "Brent Oil" : {"ticker":"BZ=F",     "ma":20, "sectors":["IDXENERGY","IDXNONCYC","IDXTRANS"]},
    "Crude Oil" : {"ticker":"CL=F",     "ma":20, "sectors":["IDXENERGY","IDXNONCYC","IDXTRANS"]},
    "Nat Gas"   : {"ticker":"NG=F",     "ma":20, "sectors":["IDXENERGY","IDXINFRA"]},
    "Coal"      : {"ticker":"KOL",      "ma":20, "sectors":["IDXENERGY","IDXBASIC"]},
    "CPO"       : {"ticker":"FCPO.KL",  "ma":20, "sectors":["IDXNONCYC","IDXBASIC"]},
    "Corn"      : {"ticker":"ZC=F",     "ma":20, "sectors":["IDXNONCYC"]},
    "Wheat"     : {"ticker":"ZW=F",     "ma":20, "sectors":["IDXNONCYC"]},
    "DXY"       : {"ticker":"DX-Y.NYB", "ma":20, "sectors":["ALL"]},
    "IHSG"      : {"ticker":"^JKSE",    "ma":50, "sectors":["ALL"]},
}

COMMODITY_FALLBACK = {
    "Nickel"   :"DBB",
    "Aluminium":"DBB",
    "Zinc"     :"DBB",
    "Tin"      :"JJT",
    "Coal"     :"ARCH",
    "CPO"      :"POW.L",
}


# ══════════════════════════════════════════════════════════════════
# GOOGLE SHEETS
# ══════════════════════════════════════════════════════════════════
def connect_gsheet(target_sheet_name: str):
    try:
        creds_json = os.environ.get("GCP_SA_KEY") or os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
        if creds_json:
            creds_dict = json.loads(creds_json)
            scopes = ["https://www.googleapis.com/auth/spreadsheets",
                      "https://www.googleapis.com/auth/drive"]
            creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        elif os.path.exists(SERVICE_ACCOUNT):
            scopes = ["https://spreadsheets.google.com/feeds",
                      "https://www.googleapis.com/auth/drive"]
            creds = Credentials.from_service_account_file(SERVICE_ACCOUNT, scopes=scopes)
        else:
            print("❌ Tidak ada credentials.")
            return None

        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SPREADSHEET_ID)
        try:
            ws = sh.worksheet(target_sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            print(f"  📋 Membuat sheet baru: {target_sheet_name}")
            ws = sh.add_worksheet(title=target_sheet_name, rows="500", cols="40")
        return ws
    except Exception as e:
        print(f"❌ Error GSheet: {e}")
        return None


# ══════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════
def get_tier_label(tier: int) -> str:
    return {1:"⭐", 2:"⭐⭐", 3:"⭐⭐⭐"}.get(tier, "-")

def calculate_trade_params(price: float, atr: float, direction: str, tier: int) -> dict:
    if atr <= 0 or direction not in ("bullish","bearish"):
        return {"SL":"-","TP1":"-","TP2":"-","RR":"-"}
    sl_mult  = {1:1.5, 2:1.2, 3:1.0}.get(tier, 1.5)
    tp1_mult = sl_mult * 2.0
    tp2_mult = sl_mult * 3.0
    if direction == "bullish":
        sl  = round(price - atr * sl_mult, 0)
        tp1 = round(price + atr * tp1_mult, 0)
        tp2 = round(price + atr * tp2_mult, 0)
    else:
        sl  = round(price + atr * sl_mult, 0)
        tp1 = round(price - atr * tp1_mult, 0)
        tp2 = round(price - atr * tp2_mult, 0)
    risk   = abs(price - sl)
    reward = abs(tp1 - price)
    rr     = round(reward / risk, 2) if risk > 0 else 0
    return {"SL":int(sl), "TP1":int(tp1), "TP2":int(tp2), "RR":rr}

def fmt_signal_date(date_val, days_ago: int, signal_label: str) -> str:
    """Format: 'ChBrkLE 06-Mar (2h lalu)'"""
    if date_val is None:
        return "-"
    try:
        if isinstance(date_val, str):
            d = pd.to_datetime(date_val)
        else:
            d = pd.Timestamp(date_val)
        date_str = d.strftime("%d-%b")
        ago_str  = f"{days_ago}h lalu" if days_ago > 0 else "hari ini"
        return f"{signal_label} {date_str} ({ago_str})"
    except Exception:
        return signal_label

def get_score_gate(commodity_score: dict) -> int:
    pct = commodity_score["bullish_pct"]
    if pct >= 70: return MIN_SCORE_STRONG
    elif pct <= 35: return MIN_SCORE_STRICT
    return BASE_MIN_SCORE


# ══════════════════════════════════════════════════════════════════
# COMMODITY CONTEXT
# ══════════════════════════════════════════════════════════════════
def fetch_commodity_context() -> dict:
    print("\n📦 Fetching commodity & market context...")
    context = {}
    for name, cfg in COMMODITY_CONFIG.items():
        ticker = cfg["ticker"]
        ma_len = cfg["ma"]
        success = False
        for attempt in [ticker, COMMODITY_FALLBACK.get(name,"")]:
            if not attempt:
                continue
            try:
                df = yf.download(attempt, period="90d", interval="1d",
                                 progress=False, auto_adjust=True)
                if df is None or len(df) < ma_len + 5:
                    continue
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.droplevel(1)
                close_s   = df["Close"]
                ma_val    = float(close_s.rolling(ma_len).mean().iloc[-1])
                close_val = float(close_s.iloc[-1])
                prev_val  = float(close_s.iloc[-6]) if len(df) >= 6 else close_val
                chg_pct   = round((close_val - prev_val)/prev_val*100, 2) if prev_val > 0 else 0
                uptrend   = (close_val < ma_val) if name == "DXY" else (close_val > ma_val)
                context[name] = {
                    "uptrend":uptrend, "close":round(close_val,2), "ma":round(ma_val,2),
                    "change_pct":chg_pct, "sectors":cfg["sectors"], "ticker":attempt,
                }
                icon  = "✅" if uptrend else "⚠️"
                label = ("Weak(Bullish EM)" if (name=="DXY" and uptrend)
                         else "Strong(Bearish EM)" if (name=="DXY") else
                         ("Bullish" if uptrend else "Bearish"))
                print(f"  {icon} {name:<12} ({attempt:<12}) "
                      f"Close={close_val:>10.2f}  MA{ma_len}={ma_val:>10.2f}  "
                      f"1W={chg_pct:>+6.2f}%  {label}")
                success = True
                break
            except Exception:
                continue
        if not success:
            print(f"  ❌ {name:<12} — gagal")
            context[name] = {"uptrend":None,"close":None,"ma":None,
                             "change_pct":None,"sectors":cfg["sectors"],"ticker":ticker}
    return context


def get_sector_commodity_score(sector: str, ctx: dict) -> dict:
    relevant = []
    for name, data in ctx.items():
        if data.get("uptrend") is None:
            continue
        secs = data.get("sectors",[])
        if "ALL" in secs or sector in secs:
            relevant.append((name, data["uptrend"]))
    if not relevant:
        return {"bullish_pct":50.0,"bullish_count":0,"total":0,"relevant":[]}
    bullish = sum(1 for _,u in relevant if u)
    pct     = round(bullish/len(relevant)*100, 1)
    return {"bullish_pct":pct,"bullish_count":bullish,"total":len(relevant),"relevant":relevant}


# ══════════════════════════════════════════════════════════════════
# CORE INDICATORS
# ══════════════════════════════════════════════════════════════════
def calc_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    h, l, c = df["High"], df["Low"], df["Close"]
    prev_c  = c.shift(1)
    tr = pd.concat([(h-l),(h-prev_c).abs(),(l-prev_c).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1/period, min_periods=period).mean()

def calc_ichimoku(df: pd.DataFrame):
    h, l   = df["High"], df["Low"]
    tenkan = (h.rolling(TENKAN).max() + l.rolling(TENKAN).min()) / 2
    kijun  = (h.rolling(KIJUN).max()  + l.rolling(KIJUN).min())  / 2
    span_a = ((tenkan + kijun) / 2).shift(KIJUN)
    span_b = ((h.rolling(SENKOU).max() + l.rolling(SENKOU).min()) / 2).shift(KIJUN)
    return span_a, span_b

def calc_macd(close: pd.Series):
    ema_fast  = close.ewm(span=MACD_FAST,  adjust=False).mean()
    ema_slow  = close.ewm(span=MACD_SLOW,  adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal    = macd_line.ewm(span=MACD_SIGNAL, adjust=False).mean()
    return macd_line, signal

def calc_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain  = delta.clip(lower=0).ewm(com=period-1, adjust=False).mean()
    loss  = (-delta.clip(upper=0)).ewm(com=period-1, adjust=False).mean()
    rs    = gain / loss.replace(0, np.nan)
    return 100 - 100 / (1 + rs)

def calc_bollinger_bands(close: pd.Series, length: int = BB_LENGTH, mult: float = BB_MULT):
    middle = close.rolling(length).mean()
    std    = close.rolling(length).std(ddof=0)
    upper  = middle + mult * std
    lower  = middle - mult * std
    return upper, middle, lower

def calc_supertrend(df: pd.DataFrame, atr_len: int = ST_ATR_LEN, factor: float = ST_FACTOR):
    """Pine Script–exact Supertrend implementation."""
    atr = calc_atr(df, atr_len)
    hl2 = (df["High"] + df["Low"]) / 2

    basic_upper = hl2 + factor * atr
    basic_lower = hl2 - factor * atr

    final_upper = basic_upper.copy()
    final_lower = basic_lower.copy()
    direction   = pd.Series(1, index=df.index, dtype=int)
    supertrend  = pd.Series(np.nan, index=df.index)

    for i in range(1, len(df)):
        close_prev = df["Close"].iloc[i - 1]
        final_upper.iloc[i] = (
            basic_upper.iloc[i]
            if basic_upper.iloc[i] < final_upper.iloc[i-1] or close_prev > final_upper.iloc[i-1]
            else final_upper.iloc[i-1]
        )
        final_lower.iloc[i] = (
            basic_lower.iloc[i]
            if basic_lower.iloc[i] > final_lower.iloc[i-1] or close_prev < final_lower.iloc[i-1]
            else final_lower.iloc[i-1]
        )
        close_now = df["Close"].iloc[i]
        prev_dir  = direction.iloc[i-1]
        if prev_dir == 1:
            direction.iloc[i]  = -1 if close_now > final_upper.iloc[i] else 1
            supertrend.iloc[i] = final_lower.iloc[i] if direction.iloc[i] == -1 else final_upper.iloc[i]
        else:
            direction.iloc[i]  = 1 if close_now < final_lower.iloc[i] else -1
            supertrend.iloc[i] = final_upper.iloc[i] if direction.iloc[i] == 1 else final_lower.iloc[i]
    return supertrend, direction


# ══════════════════════════════════════════════════════════════════
# SIGNAL DATE TRACKER
# Mencari kapan terakhir kali sinyal terjadi dalam SIGNAL_LOOKBACK candle
# ══════════════════════════════════════════════════════════════════
def find_last_signal_date(signal_series: pd.Series, dates: pd.Series,
                          lookback: int = SIGNAL_LOOKBACK) -> tuple:
    """
    signal_series : Boolean Series (True = sinyal aktif pada candle tsb)
    dates         : Series tanggal/index
    Returns       : (date_val, days_ago) atau (None, None) jika tidak ada sinyal
    """
    # Cek dalam lookback candle terakhir
    window = signal_series.iloc[-(lookback + 1):-1]  # exclude candle hari ini dari search
    dates_window = dates.iloc[-(lookback + 1):-1]

    # Tambahkan candle hari ini juga
    all_signals = signal_series.iloc[-lookback:]
    all_dates   = dates.iloc[-lookback:]

    # Cari dari yang paling baru
    for i in range(len(all_signals) - 1, -1, -1):
        if all_signals.iloc[i]:
            signal_date = all_dates.iloc[i]
            today       = pd.Timestamp.now()
            try:
                days_ago = (today - pd.Timestamp(signal_date)).days
            except Exception:
                days_ago = 0
            return signal_date, days_ago

    return None, None


# ══════════════════════════════════════════════════════════════════
# CHANNEL BREAKOUT SIGNAL HISTORY
# ══════════════════════════════════════════════════════════════════
def get_channel_breakout_history(df: pd.DataFrame) -> dict:
    """
    Hitung Upper/Lower Channel untuk setiap bar, cari kapan terakhir
    ChBrkLE (Long Entry) dan ChBrkSE (Short Entry) terjadi.

    ChBrkLE = High hari ini > Upper Channel kemarin
    ChBrkSE = Low hari ini < Lower Channel kemarin
    """
    upper_channel = df["High"].rolling(CHANNEL_LENGTH).max().shift(1)
    lower_channel = df["Low"].rolling(CHANNEL_LENGTH).min().shift(1)

    le_signal = df["High"] > upper_channel   # Long Entry
    se_signal = df["Low"]  < lower_channel   # Short Entry

    # Tanggal (ambil dari kolom Date/index)
    if "Date" in df.columns:
        dates = df["Date"]
    else:
        dates = pd.Series(df.index, index=df.index)

    le_date, le_days = find_last_signal_date(le_signal, dates)
    se_date, se_days = find_last_signal_date(se_signal, dates)

    # Sinyal hari ini (candle terakhir)
    today_le = bool(le_signal.iloc[-1])
    today_se = bool(se_signal.iloc[-1])

    # Tentukan label
    if le_date is not None:
        ch_label = fmt_signal_date(le_date, le_days, "ChBrkLE")
        ch_days  = le_days
        ch_type  = "ChBrkLE"
    elif se_date is not None:
        ch_label = fmt_signal_date(se_date, se_days, "ChBrkSE")
        ch_days  = se_days
        ch_type  = "ChBrkSE"
    else:
        ch_label = "-"
        ch_days  = 999
        ch_type  = "-"

    return {
        "ch_breakout_today": today_le,
        "ch_label"         : ch_label,
        "ch_days_ago"      : ch_days,
        "ch_type"          : ch_type,
        "le_active"        : le_date is not None,
        "se_active"        : se_date is not None,
    }


# ══════════════════════════════════════════════════════════════════
# BB STRATEGY — FIXED LOGIC
# Cross UP dari Lower Band: kemarin Close < Lower, hari ini Close >= Lower
# ══════════════════════════════════════════════════════════════════
def check_bb_strategy(df: pd.DataFrame) -> dict:
    """
    Logika BARU (diperbaiki):
    BBBrkLE = Cross UP Lower Band:
              Close[kemarin] < Lower[kemarin]  AND  Close[hari ini] >= Lower[hari ini]

    Ini menangkap momen persis saat harga bounce keluar dari zona oversold BB,
    bukan saat masih di bawah (yang sering terus turun).

    Juga tracking: kapan terakhir cross ini terjadi dalam SIGNAL_LOOKBACK bar.
    """
    try:
        upper, middle, lower = calc_bollinger_bands(df["Close"])

        # Cross UP signal series
        cross_up   = (df["Close"].shift(1) < lower.shift(1)) & (df["Close"] >= lower)
        cross_down = (df["Close"].shift(1) > upper.shift(1)) & (df["Close"] <= upper)

        if "Date" in df.columns:
            dates = df["Date"]
        else:
            dates = pd.Series(df.index, index=df.index)

        le_date, le_days = find_last_signal_date(cross_up,   dates)
        se_date, se_days = find_last_signal_date(cross_down, dates)

        close_now  = float(df["Close"].iloc[-1])
        lower_now  = float(lower.iloc[-1])
        upper_now  = float(upper.iloc[-1])
        mid_now    = float(middle.iloc[-1])

        # %B untuk info
        bw    = upper_now - lower_now
        pct_b = round((close_now - lower_now) / bw, 3) if bw > 0 else 0.5

        # Posisi harga
        if close_now > upper_now:
            bb_pos = "Above Upper"
        elif close_now < lower_now:
            bb_pos = "Below Lower"
        else:
            bb_pos = "Inside Band"

        # Signal hari ini
        today_le = bool(cross_up.iloc[-1])
        today_se = bool(cross_down.iloc[-1])

        # Label dengan tanggal
        if le_date is not None:
            bb_label = fmt_signal_date(le_date, le_days, "BBBrkLE")
            bb_days  = le_days
        elif se_date is not None:
            bb_label = fmt_signal_date(se_date, se_days, "BBBrkSE")
            bb_days  = se_days
        else:
            bb_label = "-"
            bb_days  = 999

        # Untuk scoring: aktif jika sinyal LE ada dalam SIGNAL_LOOKBACK bar
        bb_ok = le_date is not None

        return {
            "bb_ok"      : bb_ok,
            "bb_today"   : today_le,
            "bb_label"   : bb_label,
            "bb_days_ago": bb_days,
            "bb_position": bb_pos,
            "bb_pct_b"   : pct_b,
        }

    except Exception as e:
        return {
            "bb_ok":False,"bb_today":False,"bb_label":"-",
            "bb_days_ago":999,"bb_position":"Error","bb_pct_b":None,
        }


# ══════════════════════════════════════════════════════════════════
# SUPERTREND STRATEGY — WITH SIGNAL DATE
# ══════════════════════════════════════════════════════════════════
def check_supertrend_strategy(df: pd.DataFrame) -> dict:
    """
    STBrkLE = Direction flip +1 → -1 (Supertrend turun ke bawah harga = bullish)
    STBrkSE = Direction flip -1 → +1 (Supertrend naik ke atas harga = bearish)

    Tracking: kapan terakhir flip terjadi + status saat ini.
    """
    try:
        supertrend, direction = calc_supertrend(df, ST_ATR_LEN, ST_FACTOR)

        # Flip signals
        flip_le = (direction.shift(1) == 1) & (direction == -1)   # bearish→bullish
        flip_se = (direction.shift(1) == -1) & (direction == 1)   # bullish→bearish

        if "Date" in df.columns:
            dates = df["Date"]
        else:
            dates = pd.Series(df.index, index=df.index)

        le_date, le_days = find_last_signal_date(flip_le, dates)
        se_date, se_days = find_last_signal_date(flip_se, dates)

        dir_now    = int(direction.iloc[-1])
        st_in_long = dir_now == -1   # Supertrend di bawah harga = bullish

        # Label
        if le_date is not None:
            st_label = fmt_signal_date(le_date, le_days, "STBrkLE")
            st_days  = le_days
        elif se_date is not None:
            st_label = fmt_signal_date(se_date, se_days, "STBrkSE")
            st_days  = se_days
        else:
            st_label = "STBrkSE" if not st_in_long else "STBrkLE"
            st_days  = 999

        # Status ringkas
        if dir_now == -1:
            st_status = "🟢 Bullish (Long)"
        else:
            st_status = "🔴 Bearish (Short)"

        # Untuk scoring: in long = bullish kondisi terpenuhi
        st_ok = st_in_long

        return {
            "st_ok"      : st_ok,
            "st_label"   : st_label,
            "st_days_ago": st_days,
            "st_status"  : st_status,
            "st_direction": dir_now,
        }

    except Exception as e:
        return {
            "st_ok":False,"st_label":"-","st_days_ago":999,
            "st_status":"Error","st_direction":0,
        }


# ══════════════════════════════════════════════════════════════════
# RSI STRATEGY — WITH SIGNAL DATE
# ══════════════════════════════════════════════════════════════════
def check_rsi_strategy(df: pd.DataFrame) -> dict:
    """
    RSIBrkLE = RSI cross UP dari Oversold:
               RSI[kemarin] < 30  AND  RSI[hari ini] >= 30

    Tracking tanggal terakhir cross tersebut terjadi.
    """
    try:
        rsi_series = calc_rsi(df["Close"], RSI_LENGTH)

        # Cross up oversold
        cross_up_os   = (rsi_series.shift(1) < RSI_OVERSOLD)  & (rsi_series >= RSI_OVERSOLD)
        # Cross down overbought
        cross_dn_ob   = (rsi_series.shift(1) > RSI_OVERBOUGHT) & (rsi_series <= RSI_OVERBOUGHT)

        if "Date" in df.columns:
            dates = df["Date"]
        else:
            dates = pd.Series(df.index, index=df.index)

        le_date, le_days = find_last_signal_date(cross_up_os, dates)
        se_date, se_days = find_last_signal_date(cross_dn_ob, dates)

        rsi_now = float(rsi_series.iloc[-1])

        if rsi_now < RSI_OVERSOLD:
            rsi_zone = "Oversold"
        elif rsi_now > RSI_OVERBOUGHT:
            rsi_zone = "Overbought"
        else:
            rsi_zone = "Neutral"

        # Label
        if le_date is not None:
            rsi_label = fmt_signal_date(le_date, le_days, "RSIBrkLE")
            rsi_days  = le_days
        elif se_date is not None:
            rsi_label = fmt_signal_date(se_date, se_days, "RSIBrkSE")
            rsi_days  = se_days
        else:
            rsi_label = f"RSI {rsi_now:.0f} ({rsi_zone})"
            rsi_days  = 999

        # Untuk scoring: sinyal LE ada dalam lookback
        rsi_ok = le_date is not None

        return {
            "rsi_ok"      : rsi_ok,
            "rsi_label"   : rsi_label,
            "rsi_days_ago": rsi_days,
            "rsi_zone"    : rsi_zone,
            "rsi_value"   : round(rsi_now, 1),
        }

    except Exception as e:
        return {
            "rsi_ok":False,"rsi_label":"-","rsi_days_ago":999,
            "rsi_zone":"Error","rsi_value":None,
        }


# ══════════════════════════════════════════════════════════════════
# TV OSCILLATOR SCORE
# ══════════════════════════════════════════════════════════════════
def calc_tv_score(df: pd.DataFrame) -> tuple:
    try:
        import ta as ta_lib
    except ImportError:
        return 0.0, "Netral"

    score, counted = 0, 0
    def add(v):
        nonlocal score, counted
        score += v; counted += 1

    try:
        d_now = df.iloc[-1]
        # Moving Averages
        for p in [10, 20, 50, 100, 200]:
            sma = df["Close"].rolling(p).mean().iloc[-1]
            ema = df["Close"].ewm(span=p, adjust=False).mean().iloc[-1]
            if pd.notna(sma): add(1 if sma < d_now["Close"] else -1)
            if pd.notna(ema): add(1 if ema < d_now["Close"] else -1)
        # Ichimoku
        try:
            ichi = ta_lib.trend.IchimokuIndicator(df["High"],df["Low"],9,26,52)
            isa  = ichi.ichimoku_a().iloc[-1]
            isb  = ichi.ichimoku_b().iloc[-1]
            if pd.notna(isa) and pd.notna(isb):
                add(1 if d_now["Close"] > max(isa,isb) else -1)
        except Exception:
            pass
        # RSI
        rsi_s = calc_rsi(df["Close"], 14)
        r_now, r_prev = rsi_s.iloc[-1], rsi_s.iloc[-2]
        if r_now < 30 and r_now > r_prev: add(1)
        elif r_now > 70 and r_now < r_prev: add(-1)
        else: add(0)
        # MACD
        ml, sig = calc_macd(df["Close"])
        add(1 if ml.iloc[-1] > sig.iloc[-1] else -1)
        # ADX
        try:
            adx_i = ta_lib.trend.ADXIndicator(df["High"],df["Low"],df["Close"],14)
            adx   = adx_i.adx().iloc[-1]
            pdi   = adx_i.adx_pos().iloc[-1]
            mdi   = adx_i.adx_neg().iloc[-1]
            if pd.notna(adx) and adx > 20:
                add(1 if pdi > mdi else -1)
            else: add(0)
        except Exception: add(0)
        # Momentum
        mom = df["Close"].diff(10)
        add(1 if mom.iloc[-1] > mom.iloc[-2] else -1)

        fv = score / counted if counted > 0 else 0
        if   fv <= -0.5: label = "Jual Kuat"
        elif fv <= -0.1: label = "Jual"
        elif fv <   0.1: label = "Netral"
        elif fv <   0.5: label = "Beli"
        else:            label = "Beli Kuat"
        return round(fv, 2), label
    except Exception:
        return 0.0, "Netral"


# ══════════════════════════════════════════════════════════════════
# SINGLE TICKER ANALYSIS
# ══════════════════════════════════════════════════════════════════
def analyze_ticker(ticker: str, sector: str, commodity_ctx: dict) -> dict | None:
    end_dt   = datetime.today()
    start_dt = end_dt - timedelta(days=DOWNLOAD_DAYS)

    try:
        df = yf.download(
            ticker,
            start=start_dt.strftime("%Y-%m-%d"),
            end=end_dt.strftime("%Y-%m-%d"),
            interval="1d",
            progress=False,
            auto_adjust=True,
        )
    except Exception as e:
        print(f"    [ERROR] {ticker}: {e}")
        return None

    if df is None or len(df) < 80:
        print(f"    [SKIP]  {ticker} — data tidak cukup")
        return None

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.droplevel(1)

    df = df.dropna(subset=["Open","High","Low","Close","Volume"]).copy()
    df.reset_index(inplace=True)   # Date jadi kolom

    if len(df) < 60:
        return None

    close_now = float(df["Close"].iloc[-1])
    if close_now <= 0:
        return None

    # ── Date column ───────────────────────────────────────────────
    date_col = "Date" if "Date" in df.columns else df.columns[0]
    today_date = pd.Timestamp(df[date_col].iloc[-1]).strftime("%d-%b-%Y")

    # ── Base Indicators ───────────────────────────────────────────
    atr_series = calc_atr(df, 14)
    atr_val    = float(atr_series.iloc[-1])
    atr_pct    = (atr_val / close_now) * 100

    vol_ma20  = df["Volume"].rolling(20).mean().iloc[-1]
    liquidity = close_now * vol_ma20
    vol_surge = bool(float(df["Volume"].iloc[-1]) > vol_ma20 * VOLUME_SURGE)

    close_5d  = float(df["Close"].iloc[-6]) if len(df) >= 6 else close_now
    price_chg = (close_now - close_5d) / close_5d * 100 if close_5d > 0 else 0.0

    span_a, span_b = calc_ichimoku(df)
    cloud_top      = pd.concat([span_a, span_b], axis=1).max(axis=1).iloc[-1]
    ichimoku_ok    = bool(close_now > cloud_top) if pd.notna(cloud_top) else False

    macd_line, macd_sig = calc_macd(df["Close"])
    macd_ok = bool(macd_line.iloc[-1] > macd_sig.iloc[-1])

    sma20      = df["Close"].rolling(20).mean().iloc[-1]
    sma50      = df["Close"].rolling(50).mean().iloc[-1]
    sma20_prev = df["Close"].rolling(20).mean().iloc[-6]
    is_uptrend = bool((sma20 > sma50) and (sma20 > sma20_prev) and (close_now > sma50))

    rsi_val = float(calc_rsi(df["Close"], RSI_LENGTH).iloc[-1])

    adx_val = 0.0
    try:
        import ta as ta_lib
        adx_ind = ta_lib.trend.ADXIndicator(df["High"],df["Low"],df["Close"],14)
        adx_val = float(adx_ind.adx().iloc[-1])
    except Exception:
        pass

    rh20         = df["High"].rolling(20).max().iloc[-1]
    rl20         = df["Low"].rolling(20).min().iloc[-1]
    rng          = rh20 - rl20
    pos          = (close_now - rl20) / rng if rng > 0 else 0.5
    sr_label     = "Support" if pos < 0.25 else "Resistance" if pos > 0.75 else "-"
    near_support = bool(pos < 0.25)

    tv_score, tv_label = calc_tv_score(df)

    # ── Channel Breakout History ──────────────────────────────────
    ch_result = get_channel_breakout_history(df)

    # ── Three TV Strategies ───────────────────────────────────────
    bb_result = check_bb_strategy(df)
    st_result = check_supertrend_strategy(df)
    rsi_strat = check_rsi_strategy(df)

    # ── Scoring (9 conditions) ────────────────────────────────────
    liq_ok  = bool(liquidity > MIN_LIQUIDITY)
    atr_ok  = bool(ATR_MIN_PCT <= atr_pct <= ATR_MAX_PCT)
    pchg_ok = bool(abs(price_chg) <= PRICE_CHANGE_MAX)
    bb_ok   = bb_result["bb_ok"]
    st_ok   = st_result["st_ok"]
    rsi_ok  = rsi_strat["rsi_ok"]

    score_base = sum([
        liq_ok,       # 1
        atr_ok,       # 2
        pchg_ok,      # 3
        ichimoku_ok,  # 4
        macd_ok,      # 5
        vol_surge,    # 6
        bb_ok,        # 7
        st_ok,        # 8
        rsi_ok,       # 9
    ])

    # ── Tier ─────────────────────────────────────────────────────
    tier = 0
    if ch_result["ch_breakout_today"] or ch_result["le_active"]:
        tc = sum([
            liq_ok, atr_ok, pchg_ok, ichimoku_ok, macd_ok, vol_surge,
            bb_ok, st_ok, rsi_ok, is_uptrend, near_support
        ])
        if tc >= 8:   tier = 3
        elif tc >= 5: tier = 2
        elif tc >= 3: tier = 1

    # ── Commodity ─────────────────────────────────────────────────
    comm_score = get_sector_commodity_score(sector, commodity_ctx)
    score_gate = get_score_gate(comm_score)

    # ── Decision ─────────────────────────────────────────────────
    # Channel Breakout aktif = sinyal LE ada dalam lookback
    cb_active = ch_result["le_active"]
    direction = "bullish" if cb_active else "neutral"

    tv_strat_count = sum([bb_ok, st_ok, rsi_ok])

    if cb_active and score_base >= score_gate:
        if comm_score["bullish_pct"] >= 70 and tier >= 2 and tv_strat_count >= 2:
            decision = "⭐ OPEN BUY BESOK (STRONG)"
        elif comm_score["bullish_pct"] <= 35:
            decision = "⚠️ OPEN BUY (CAUTIOUS)"
        else:
            decision = "✅ OPEN BUY BESOK"
    elif cb_active:
        decision = "👀 WATCHLIST (CB Aktif)"
    else:
        decision = "WATCHLIST"

    # ── SL/TP ─────────────────────────────────────────────────────
    trade = calculate_trade_params(close_now, atr_val, direction, tier if tier > 0 else 1)

    # ── Commodity Summary ─────────────────────────────────────────
    comm_summary = " | ".join([
        f"{'✅' if u else '⚠️'}{n}"
        for n, u in comm_score["relevant"][:5]
    ])

    # ── Sorting key: hari ini lebih kecil = di atas ───────────────
    sort_key = ch_result["ch_days_ago"]

    return {
        # ── Identitas ─────────────────────────────────────────────
        "Ticker"               : ticker.replace(".JK",""),
        "Sektor"               : sector,
        "Harga"                : int(close_now),
        "Tgl Data"             : today_date,

        # ── Channel Breakout ──────────────────────────────────────
        "Channel Breakout"     : ch_result["ch_label"],
        "CB Hari Lalu"         : ch_result["ch_days_ago"] if ch_result["ch_days_ago"] < 999 else "-",

        # ── BB Strategy ───────────────────────────────────────────
        "BB Signal"            : bb_result["bb_label"],
        "BB Position"          : bb_result["bb_position"],
        "BB %B"                : bb_result["bb_pct_b"],

        # ── Supertrend ────────────────────────────────────────────
        "Supertrend Signal"    : st_result["st_label"],
        "ST Status"            : st_result["st_status"],

        # ── RSI Strategy ─────────────────────────────────────────
        "RSI Strategy"         : rsi_strat["rsi_label"],

        # ── Score ─────────────────────────────────────────────────
        "Score (/9)"           : score_base,
        "Tier"                 : get_tier_label(tier) if tier > 0 else "-",

        # ── 6 Kondisi Detail ──────────────────────────────────────
        "Liquidity (Bil IDR)"  : round(liquidity / 1e9, 2),
        "Price Change 1W (%)"  : round(price_chg, 2),
        "Ichimoku Cloud"       : "✅" if ichimoku_ok else "❌",
        "MACD Bullish"         : "✅" if macd_ok else "❌",
        "Volume Surge"         : "✅" if vol_surge else "❌",
        "ATR 14 (%)"           : round(atr_pct, 2),
        "ATR"                  : int(atr_val),

        # ── Tech ──────────────────────────────────────────────────
        "RSI"                  : round(rsi_val, 1),
        "ADX"                  : round(adx_val, 1),
        "Skor TV"              : tv_score,
        "Rek TV"               : tv_label,
        "S/R Zone"             : sr_label,

        # ── Trade Management ──────────────────────────────────────
        "SL"                   : trade["SL"],
        "TP1"                  : trade["TP1"],
        "TP2"                  : trade["TP2"],
        "RR"                   : trade["RR"],

        # ── Commodity ─────────────────────────────────────────────
        "Commodity Bullish %"  : comm_score["bullish_pct"],
        "Score Gate"           : score_gate,
        "Commodity Context"    : comm_summary,

        # ── Decision ──────────────────────────────────────────────
        "Decision"             : decision,

        # ── Meta ──────────────────────────────────────────────────
        "Waktu Run"            : datetime.now(WIB).strftime("%Y-%m-%d %H:%M"),

        # Internal sort key (tidak ditampilkan)
        "_sort_cb_days"        : sort_key,
        "_score"               : score_base,
    }


# ══════════════════════════════════════════════════════════════════
# SECTOR ANALYSIS
# ══════════════════════════════════════════════════════════════════
def analyze_sector(sector_name: str, tickers: list, commodity_ctx: dict) -> pd.DataFrame:
    print(f"\n📊 Scan: {sector_name} ({len(tickers)} emiten)")
    comm_score = get_sector_commodity_score(sector_name, commodity_ctx)
    gate       = get_score_gate(comm_score)
    print(f"   Commodity bullish: {comm_score['bullish_pct']}% → Gate: Score ≥ {gate}")

    results = []
    for i, ticker in enumerate(tickers, 1):
        print(f"  [{i:>2}/{len(tickers)}] {ticker}...", end=" ")
        try:
            res = analyze_ticker(ticker, sector_name, commodity_ctx)
            if res:
                results.append(res)
                cb   = res["Channel Breakout"]
                st   = res["ST Status"]
                print(f"Score={res['Score (/9)']}  CB={cb[:15]}  {res['Decision']}")
            else:
                print("skip")
        except Exception as e:
            print(f"ERROR: {e}")
        time.sleep(0.25)

    if not results:
        return pd.DataFrame()

    df_out = pd.DataFrame(results)
    # Sort: CB aktif dulu (days_ago terkecil), lalu score tertinggi
    df_out = df_out.sort_values(
        by=["_sort_cb_days", "_score"],
        ascending=[True, False]
    ).reset_index(drop=True)

    # Hapus kolom internal
    df_out = df_out.drop(columns=["_sort_cb_days","_score"], errors="ignore")
    return df_out


# ══════════════════════════════════════════════════════════════════
# UPLOAD SECTOR SHEET
# ══════════════════════════════════════════════════════════════════
def upload_sector_sheet(sector: str, df: pd.DataFrame, commodity_ctx: dict):
    ws = connect_gsheet(sector)
    if not ws:
        return
    try:
        ws.clear()
        run_time   = datetime.now(WIB).strftime("%Y-%m-%d %H:%M WIB")
        comm_score = get_sector_commodity_score(sector, commodity_ctx)

        ws.update("A1", [[
            f"📊 {sector} — Channel Breakout Screener v2.2 | {run_time}"
        ]])
        ws.update("A2", [[
            f"Commodity Bullish: {comm_score['bullish_pct']}%",
            f"({comm_score['bullish_count']}/{comm_score['total']})",
            f"Score Gate: ≥ {get_score_gate(comm_score)}",
            f"BB Cross-Up Lower Band({BB_LENGTH},{BB_MULT})",
            f"Supertrend({ST_ATR_LEN},{ST_FACTOR})",
            f"RSI({RSI_LENGTH} OS:{RSI_OVERSOLD})",
        ]])
        ws.update("A3", [[""]])

        set_with_dataframe(ws, df, row=4, col=1, include_index=False)

        n_cols = len(df.columns)
        ws.format(f"A4:{chr(64 + min(n_cols,26))}4", {
            "textFormat"     : {"bold": True},
            "backgroundColor": {"red":0.12,"green":0.20,"blue":0.50},
        })

        n_buy    = df[df["Decision"].str.contains("OPEN BUY", na=False)].shape[0]
        n_strong = df[df["Decision"].str.contains("STRONG",   na=False)].shape[0]
        print(f"  ✅ {sector}: {len(df)} emiten | {n_buy} OPEN BUY ({n_strong} STRONG)")

    except Exception as e:
        print(f"  ❌ Upload error {sector}: {e}")


# ══════════════════════════════════════════════════════════════════
# UPLOAD SUMMARY SHEET
# ══════════════════════════════════════════════════════════════════
def upload_summary_sheet(all_results: list, commodity_ctx: dict):
    print(f"\n📤 Uploading SUMMARY → '{SUMMARY_SHEET}'...")
    ws = connect_gsheet(SUMMARY_SHEET)
    if not ws:
        return
    try:
        ws.clear()
        run_time = datetime.now(WIB).strftime("%Y-%m-%d %H:%M WIB")

        ws.update("A1", [[
            f"🔍 IHSG Channel Breakout Screener v2.2 — {run_time}",
            "", "",
            f"BB Cross-Up({BB_LENGTH},{BB_MULT}) | "
            f"Supertrend({ST_ATR_LEN},{ST_FACTOR}) | "
            f"RSI Strategy({RSI_LENGTH},OS={RSI_OVERSOLD})"
        ]])

        # Commodity block
        comm_rows = [["Commodity","Ticker","Close","MA","1W Chg%","Status"]]
        for name, data in commodity_ctx.items():
            if data.get("close") is None:
                comm_rows.append([name, data["ticker"],"N/A","N/A","N/A","❌ Error"])
                continue
            if name == "DXY":
                status = "✅ Weak (Bullish EM)" if data["uptrend"] else "⚠️ Strong (Bearish EM)"
            else:
                status = "✅ Bullish" if data["uptrend"] else "⚠️ Bearish"
            comm_rows.append([
                name, data["ticker"], data["close"], data["ma"],
                f"{data['change_pct']:+.2f}%", status,
            ])
        ws.update("A2", comm_rows)

        sep_row   = len(comm_rows) + 3
        ws.update(f"A{sep_row}", [[""]])
        sum_start = sep_row + 1

        # Build summary dataframe
        df_all = pd.DataFrame(all_results)
        if df_all.empty:
            print("  ⚠️  Tidak ada hasil.")
            return

        # Remove internal cols if still present
        df_all = df_all.drop(columns=["_sort_cb_days","_score"], errors="ignore")

        # Sort: Open Buy di atas, lalu CB days_ago terkecil, lalu score
        df_sum = df_all.copy()
        decision_order = df_sum["Decision"].map(lambda x: (
            0 if "STRONG" in x else
            1 if "OPEN BUY" in x else
            2 if "Watchlist (CB" in x or "WATCHLIST (CB" in x else 3
        ))
        cb_days_num = df_sum["CB Hari Lalu"].apply(lambda x: int(x) if str(x).isdigit() else 999)

        df_sum = df_sum.assign(_order=decision_order, _cbdays=cb_days_num)
        df_sum = df_sum.sort_values(
            by=["_order","_cbdays","Score (/9)"],
            ascending=[True,True,False]
        ).drop(columns=["_order","_cbdays"]).reset_index(drop=True)

        # Ordered columns for summary
        cols_summary = [
            "Sektor","Ticker","Harga","Tgl Data",
            "Channel Breakout","CB Hari Lalu",
            "BB Signal","BB Position","BB %B",
            "Supertrend Signal","ST Status",
            "RSI Strategy",
            "Score (/9)","Tier",
            "Liquidity (Bil IDR)","Price Change 1W (%)","ATR 14 (%)",
            "Ichimoku Cloud","MACD Bullish","Volume Surge",
            "RSI","ADX","Skor TV","Rek TV","S/R Zone",
            "SL","TP1","TP2","RR",
            "Commodity Bullish %","Score Gate","Commodity Context",
            "Decision","Waktu Run",
        ]
        available  = [c for c in cols_summary if c in df_sum.columns]
        df_display = df_sum[available]

        set_with_dataframe(ws, df_display, row=sum_start, col=1, include_index=False)

        n_cols = len(df_display.columns)
        ws.format(
            f"A{sum_start}:{chr(64 + min(n_cols,26))}{sum_start}",
            {"textFormat":{"bold":True},"backgroundColor":{"red":0.10,"green":0.35,"blue":0.22}}
        )

        n_buy    = df_sum[df_sum["Decision"].str.contains("OPEN BUY", na=False)].shape[0]
        n_strong = df_sum[df_sum["Decision"].str.contains("STRONG",   na=False)].shape[0]
        n_cb     = df_sum[df_sum["CB Hari Lalu"].astype(str).str.isdigit()].shape[0]
        print(f"  ✅ Summary: {len(df_sum)} emiten | {n_buy} OPEN BUY ({n_strong} STRONG) | "
              f"{n_cb} punya CB aktif")

    except Exception as e:
        print(f"  ❌ Upload summary error: {e}")


# ══════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════
def main():
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║   IHSG CHANNEL BREAKOUT SCREENER v2.2                        ║")
    print("║   BB Cross-Up | Supertrend | RSI Strategy | Signal Dates     ║")
    print(f"║   {datetime.now(WIB).strftime('%Y-%m-%d %H:%M WIB'):<58}║")
    print("╚══════════════════════════════════════════════════════════════╝")
    print(f"\n  BB Strategy   : Cross-UP Lower Band  ({BB_LENGTH},{BB_MULT})")
    print(f"  Supertrend    : ATR={ST_ATR_LEN}, Factor={ST_FACTOR}")
    print(f"  RSI Strategy  : Length={RSI_LENGTH}, OS={RSI_OVERSOLD}, OB={RSI_OVERBOUGHT}")
    print(f"  Signal Lookback: {SIGNAL_LOOKBACK} candle terakhir")
    print(f"  Score Max     : 9 kondisi\n")

    commodity_ctx = fetch_commodity_context()

    all_results = []
    for sector_name, tickers in SECTOR_CONFIG.items():
        df_sector = analyze_sector(sector_name, tickers, commodity_ctx)
        if df_sector.empty:
            print(f"  ⚠️  Tidak ada data untuk {sector_name}")
            continue
        all_results.extend(df_sector.to_dict("records"))
        upload_sector_sheet(sector_name, df_sector, commodity_ctx)
        time.sleep(2)

    if all_results:
        upload_summary_sheet(all_results, commodity_ctx)
    else:
        print("\n❌ Tidak ada hasil.")
        sys.exit(1)

    df_final  = pd.DataFrame(all_results)
    n_buy     = df_final[df_final["Decision"].str.contains("OPEN BUY", na=False)].shape[0]
    n_strong  = df_final[df_final["Decision"].str.contains("STRONG",   na=False)].shape[0]
    cb_active = df_final[df_final["CB Hari Lalu"].astype(str).str.isdigit()].shape[0]

    print("\n" + "═"*65)
    print(f"  Total emiten   : {len(df_final)}")
    print(f"  CB Aktif       : {cb_active}")
    print(f"  OPEN BUY BESOK : {n_buy}  (incl. {n_strong} ⭐ STRONG)")
    print("═"*65)
    print("\n✅ Done!\n")


if __name__ == "__main__":
    main()
