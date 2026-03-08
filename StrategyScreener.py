"""
╔═════════════════════════════════════════════════════════════════════════════════════╗
║     IHSG STRATEGY SCREENER — VERSION 2.1                                            ║
║     Enhanced: BB Strategy | Supertrend | RSI Strategy | Channel Breakout            ║
║     Sector Framework | Tier System | SL/TP | Commodity Index                        ║
╚═════════════════════════════════════════════════════════════════════════════════════╝

Strategy  : Channel Breakout (Long only, per-sector)
Universe  : IDX / IHSG stocks (.JK suffix), by sector

Scoring (9 base conditions, each = +1):
  1.  Liquidity      : Price × AvgVol20D > 10B IDR
  2.  ATR            : ATR14 antara 1%–3% dari harga
  3.  Price Change 1W: antara -3% dan +3%
  4.  Ichimoku Cloud : Close > max(SpanA, SpanB)
  5.  MACD Bullish   : MACD Line > Signal Line
  6.  Volume Surge   : Volume > VolMA20 × 1.5
  7.  BB Strategy ✨ : Close < Lower BB (oversold bounce setup)
  8.  Supertrend ✨  : Supertrend berubah dari ATAS → BAWAH harga (Long signal)
  9.  RSI Strategy ✨: RSI cross UP dari bawah level Oversold (30)

Tier System:
  Tier 3 ⭐⭐⭐ : Breakout + 7+ confluence
  Tier 2 ⭐⭐   : Breakout + 5+ confluence
  Tier 1 ⭐     : Breakout + 3+ confluence

Commodity Context:
  Bullish ≥ 70% → Gate Score ≥ 3 (agresif)
  Normal 35–70% → Gate Score ≥ 4
  Bearish ≤ 35% → Gate Score ≥ 5 (selektif)

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

# Channel Breakout
CHANNEL_LENGTH   = 5

# Score gates (from 9 possible base points)
BASE_MIN_SCORE   = 4
MIN_SCORE_STRONG = 3    # gate turun saat commodity bullish ≥ 70%
MIN_SCORE_STRICT = 6    # gate naik saat commodity bearish ≤ 35%

# Kondisi individual
ATR_MIN_PCT      = 1.0
ATR_MAX_PCT      = 3.0
MIN_LIQUIDITY    = 10_000_000_000   # 10 Miliar IDR
VOLUME_SURGE     = 1.5
PRICE_CHANGE_MAX = 3.0              # ±3%

# Ichimoku
TENKAN  = 9
KIJUN   = 26
SENKOU  = 52

# MACD
MACD_FAST   = 12
MACD_SLOW   = 26
MACD_SIGNAL = 9

# ── Bollinger Bands Strategy (TradingView default) ────────────────
BB_LENGTH   = 20
BB_MULT     = 2.0   # Std dev multiplier

# ── Supertrend Strategy (TradingView default) ─────────────────────
ST_ATR_LEN  = 10    # ATR lookback for Supertrend
ST_FACTOR   = 3.0   # ATR multiplier for band offset

# ── RSI Strategy (TradingView default) ───────────────────────────
RSI_LENGTH   = 14
RSI_OVERSOLD = 30
RSI_OVERBOUGHT = 70

DOWNLOAD_DAYS = 300   # buffer untuk Ichimoku (52 bar) + SMA200 + Supertrend warmup
WIB           = timezone(timedelta(hours=7))


# ══════════════════════════════════════════════════════════════════
# SECTOR CONFIG  (10 ticker per sektor — tambah sesuai kebutuhan)
# ══════════════════════════════════════════════════════════════════
SECTOR_CONFIG = {
    "IDXFINANCE": [
        "BBCA.JK", "BBRI.JK", "BMRI.JK", "BBNI.JK", "BRIS.JK",
        "BBTN.JK", "BNGA.JK", "NISP.JK", "BDMN.JK", "BTPS.JK",
    ],
    "IDXENERGY": [
        "ADRO.JK", "BYAN.JK", "PTBA.JK", "ITMG.JK", "HRUM.JK",
        "MEDC.JK", "PGAS.JK", "ELSA.JK", "AKRA.JK", "INDY.JK",
    ],
    "IDXBASIC": [
        "ANTM.JK", "MDKA.JK", "INCO.JK", "TINS.JK", "SMGR.JK",
        "INTP.JK", "TPIA.JK", "INKP.JK", "TKIM.JK", "MBMA.JK",
    ],
    "IDXINDUST": [
        "ASII.JK", "UNTR.JK", "SCCO.JK", "KBLI.JK", "VOKS.JK",
        "AMFG.JK", "ARNA.JK", "TOTO.JK", "LION.JK", "ASGR.JK",
    ],
    "IDXNONCYC": [
        "UNVR.JK", "ICBP.JK", "INDF.JK", "MYOR.JK", "CPIN.JK",
        "JPFA.JK", "ULTJ.JK", "SOFA.JK", "KLBF.JK", "GGRM.JK",
    ],
    "IDXCYCLIC": [
        "MAPI.JK", "ACES.JK", "ERAA.JK", "LPPF.JK", "MNCN.JK",
        "SCMA.JK", "AUTO.JK", "GJTL.JK", "SMSM.JK", "FAST.JK",
    ],
    "IDXTECHNO": [
        "GOTO.JK", "CASH.JK", "BUKA.JK", "MSTI.JK", "MTDL.JK",
        "MLPT.JK", "MCAS.JK", "KREN.JK", "HDIT.JK", "NFCX.JK",
    ],
    "IDXHEALTH": [
        "KLBF.JK", "SIDO.JK", "KAEF.JK", "TSPC.JK", "MIKA.JK",
        "SILO.JK", "HEAL.JK", "MERK.JK", "DVLA.JK", "PRDA.JK",
    ],
    "IDXINFRA": [
        "TLKM.JK", "EXCL.JK", "ISAT.JK", "TOWR.JK", "JSMR.JK",
        "TBIG.JK", "MTEL.JK", "WIKA.JK", "PTPP.JK", "ADHI.JK",
    ],
    "IDXPROPERT": [
        "CTRA.JK", "BSDE.JK", "PWON.JK", "SMRA.JK", "ASRI.JK",
        "LPKR.JK", "DMAS.JK", "DUTI.JK", "MTLA.JK", "BEST.JK",
    ],
    "IDXTRANS": [
        "GIAA.JK", "SMDR.JK", "BIRD.JK", "ASSA.JK", "TMAS.JK",
        "JSMR.JK", "WEHA.JK", "NELY.JK", "SAFE.JK", "LRNA.JK",
    ],
}


# ══════════════════════════════════════════════════════════════════
# COMMODITY INDEX CONFIG
# ══════════════════════════════════════════════════════════════════
COMMODITY_CONFIG = {
    # ── Metals ───────────────────────────────────────────────────
    "Gold"      : {"ticker": "GC=F",    "ma": 20, "sectors": ["IDXBASIC", "IDXFINANCE"]},
    "Silver"    : {"ticker": "SI=F",    "ma": 20, "sectors": ["IDXBASIC", "IDXINDUST"]},
    "Copper"    : {"ticker": "HG=F",    "ma": 20, "sectors": ["IDXBASIC", "IDXINDUST", "IDXINFRA"]},
    "Nickel"    : {"ticker": "NI=F",    "ma": 20, "sectors": ["IDXBASIC", "IDXINDUST"]},
    "Aluminium" : {"ticker": "ALI=F",   "ma": 20, "sectors": ["IDXBASIC", "IDXINDUST"]},
    "Zinc"      : {"ticker": "ZNC=F",   "ma": 20, "sectors": ["IDXBASIC", "IDXINDUST"]},
    "Tin"       : {"ticker": "JJT",     "ma": 20, "sectors": ["IDXBASIC"]},
    # ── Energy ───────────────────────────────────────────────────
    "Brent Oil" : {"ticker": "BZ=F",    "ma": 20, "sectors": ["IDXENERGY", "IDXNONCYC", "IDXTRANS"]},
    "Crude Oil" : {"ticker": "CL=F",    "ma": 20, "sectors": ["IDXENERGY", "IDXNONCYC", "IDXTRANS"]},
    "Nat Gas"   : {"ticker": "NG=F",    "ma": 20, "sectors": ["IDXENERGY", "IDXINFRA"]},
    "Coal"      : {"ticker": "KOL",     "ma": 20, "sectors": ["IDXENERGY", "IDXBASIC"]},
    # ── Agri ─────────────────────────────────────────────────────
    "CPO"       : {"ticker": "FCPO.KL", "ma": 20, "sectors": ["IDXNONCYC", "IDXBASIC"]},
    "Corn"      : {"ticker": "ZC=F",    "ma": 20, "sectors": ["IDXNONCYC"]},
    "Wheat"     : {"ticker": "ZW=F",    "ma": 20, "sectors": ["IDXNONCYC"]},
    # ── Macro ────────────────────────────────────────────────────
    "DXY"       : {"ticker": "DX-Y.NYB","ma": 20, "sectors": ["ALL"]},   # inverse: weak DXY = bullish EM
    "IHSG"      : {"ticker": "^JKSE",   "ma": 50, "sectors": ["ALL"]},
}

COMMODITY_FALLBACK = {
    "Nickel"    : "^NIKKI",
    "Aluminium" : "DBB",
    "Zinc"      : "DBB",
    "Tin"       : "JJT",
    "Coal"      : "ARCH",
    "CPO"       : "POW.L",
}


# ══════════════════════════════════════════════════════════════════
# GOOGLE SHEETS CONNECTION
# ══════════════════════════════════════════════════════════════════
def connect_gsheet(target_sheet_name: str):
    try:
        creds_json = os.environ.get("GCP_SA_KEY") or os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
        if creds_json:
            creds_dict = json.loads(creds_json)
            scopes = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ]
            creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        elif os.path.exists(SERVICE_ACCOUNT):
            scopes = [
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive",
            ]
            creds = Credentials.from_service_account_file(SERVICE_ACCOUNT, scopes=scopes)
        else:
            print("❌ Tidak ada credentials ditemukan.")
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
        print(f"❌ Error koneksi GSheet: {e}")
        return None


# ══════════════════════════════════════════════════════════════════
# TIER & SCORE HELPERS
# ══════════════════════════════════════════════════════════════════
def get_tier_label(tier: int) -> str:
    return {1: "⭐", 2: "⭐⭐", 3: "⭐⭐⭐"}.get(tier, "")

def get_pattern_score(base_prob: float, tier: int, confluence: int) -> float:
    """Probabilistic score 0–95."""
    tier_mult  = {1: 1.00, 2: 1.15, 3: 1.30}.get(tier, 1.00)
    conf_bonus = 1 + (confluence * 0.04)
    return round(min(95, base_prob * 100 * tier_mult * conf_bonus), 1)

def calculate_trade_params(price: float, atr: float, direction: str, tier: int) -> dict:
    """ATR-based SL/TP. Tier makin tinggi → SL makin ketat, RR ≥ 2:1."""
    if atr <= 0 or direction not in ("bullish", "bearish"):
        return {"SL": "-", "TP1": "-", "TP2": "-", "RR": "-"}

    sl_mult  = {1: 1.5, 2: 1.2, 3: 1.0}.get(tier, 1.5)
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

    return {"SL": int(sl), "TP1": int(tp1), "TP2": int(tp2), "RR": rr}


# ══════════════════════════════════════════════════════════════════
# COMMODITY CONTEXT
# ══════════════════════════════════════════════════════════════════
def fetch_commodity_context() -> dict:
    print("\n📦 Fetching commodity & market context...")
    context = {}

    for name, cfg in COMMODITY_CONFIG.items():
        ticker  = cfg["ticker"]
        ma_len  = cfg["ma"]
        success = False

        for attempt in [ticker, COMMODITY_FALLBACK.get(name, "")]:
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
                chg_pct   = round((close_val - prev_val) / prev_val * 100, 2) if prev_val > 0 else 0

                # DXY bersifat inverse untuk EM
                uptrend = (close_val < ma_val) if name == "DXY" else (close_val > ma_val)

                context[name] = {
                    "uptrend"    : uptrend,
                    "close"      : round(close_val, 2),
                    "ma"         : round(ma_val, 2),
                    "change_pct" : chg_pct,
                    "sectors"    : cfg["sectors"],
                    "ticker"     : attempt,
                }
                icon = "✅" if uptrend else "⚠️"
                label = "Weak (Bullish EM)" if (name == "DXY" and uptrend) else \
                        "Strong (Bearish EM)" if (name == "DXY" and not uptrend) else \
                        ("Bullish" if uptrend else "Bearish")
                print(f"  {icon} {name:<12} ({attempt:<12}) "
                      f"Close={close_val:>10.2f}  MA{ma_len}={ma_val:>10.2f}  "
                      f"1W={chg_pct:>+6.2f}%  {label}")
                success = True
                break

            except Exception:
                continue

        if not success:
            print(f"  ❌ {name:<12} — gagal, skip")
            context[name] = {
                "uptrend": None, "close": None, "ma": None,
                "change_pct": None, "sectors": cfg["sectors"], "ticker": ticker,
            }

    return context


def get_sector_commodity_score(sector: str, ctx: dict) -> dict:
    relevant = []
    for name, data in ctx.items():
        if data.get("uptrend") is None:
            continue
        secs = data.get("sectors", [])
        if "ALL" in secs or sector in secs:
            relevant.append((name, data["uptrend"]))

    if not relevant:
        return {"bullish_pct": 50.0, "bullish_count": 0, "total": 0, "relevant": []}

    bullish = sum(1 for _, u in relevant if u)
    pct     = round(bullish / len(relevant) * 100, 1)
    return {
        "bullish_pct"  : pct,
        "bullish_count": bullish,
        "total"        : len(relevant),
        "relevant"     : relevant,
    }


def get_score_gate(commodity_score: dict) -> int:
    pct = commodity_score["bullish_pct"]
    if pct >= 70:
        return MIN_SCORE_STRONG
    elif pct <= 35:
        return MIN_SCORE_STRICT
    else:
        return BASE_MIN_SCORE


# ══════════════════════════════════════════════════════════════════
# CORE INDICATOR CALCULATIONS
# ══════════════════════════════════════════════════════════════════
def calc_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    h, l, c = df["High"], df["Low"], df["Close"]
    prev_c  = c.shift(1)
    tr = pd.concat([(h - l), (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / period, min_periods=period).mean()


def calc_ichimoku(df: pd.DataFrame):
    h, l   = df["High"], df["Low"]
    tenkan = (h.rolling(TENKAN).max() + l.rolling(TENKAN).min()) / 2
    kijun  = (h.rolling(KIJUN).max()  + l.rolling(KIJUN).min())  / 2
    span_a = ((tenkan + kijun) / 2).shift(KIJUN)
    span_b = ((h.rolling(SENKOU).max() + l.rolling(SENKOU).min()) / 2).shift(KIJUN)
    return span_a, span_b


def calc_macd(close: pd.Series):
    ema_fast  = close.ewm(span=MACD_FAST,   adjust=False).mean()
    ema_slow  = close.ewm(span=MACD_SLOW,   adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal    = macd_line.ewm(span=MACD_SIGNAL, adjust=False).mean()
    return macd_line, signal


def calc_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta  = close.diff()
    gain   = delta.clip(lower=0).ewm(com=period - 1, adjust=False).mean()
    loss   = (-delta.clip(upper=0)).ewm(com=period - 1, adjust=False).mean()
    rs     = gain / loss.replace(0, np.nan)
    return 100 - 100 / (1 + rs)


# ══════════════════════════════════════════════════════════════════
# ★ NEW: BOLLINGER BANDS STRATEGY (TradingView)
# ══════════════════════════════════════════════════════════════════
def calc_bollinger_bands(close: pd.Series, length: int = BB_LENGTH, mult: float = BB_MULT):
    """
    Returns (upper, middle, lower) Bollinger Bands Series.
    Middle = SMA(length), Upper/Lower = middle ± mult × StdDev(length).
    """
    middle = close.rolling(length).mean()
    std    = close.rolling(length).std(ddof=0)
    upper  = middle + mult * std
    lower  = middle - mult * std
    return upper, middle, lower


def check_bb_strategy(df: pd.DataFrame) -> dict:
    """
    TradingView Bollinger Bands Strategy — Long Setup:

    Sinyal LONG = Close hari ini TUTUP DI BAWAH Lower Band
    (oversold condition — setup untuk bounce ke Middle Band)

    Kondisi keluar / TP alami = harga menyentuh Middle Band kembali.

    Returns dict dengan:
      bb_long_signal : bool  — apakah setup long aktif hari ini
      bb_position    : str   — "Below Lower" / "Above Upper" / "Inside"
      bb_upper       : float
      bb_middle      : float
      bb_lower       : float
      bb_pct_b       : float — %B indicator (0=lower, 0.5=middle, 1=upper, <0=di bawah lower)
    """
    try:
        upper, middle, lower = calc_bollinger_bands(df["Close"])

        close_now  = float(df["Close"].iloc[-1])
        close_prev = float(df["Close"].iloc[-2])
        upper_now  = float(upper.iloc[-1])
        mid_now    = float(middle.iloc[-1])
        lower_now  = float(lower.iloc[-1])

        if pd.isna(lower_now) or pd.isna(upper_now):
            return {
                "bb_long_signal": False,
                "bb_position"   : "N/A",
                "bb_upper"      : None,
                "bb_middle"     : round(mid_now, 0) if not pd.isna(mid_now) else None,
                "bb_lower"      : None,
                "bb_pct_b"      : None,
            }

        # %B = (Close - Lower) / (Upper - Lower)
        band_width = upper_now - lower_now
        pct_b      = round((close_now - lower_now) / band_width, 3) if band_width > 0 else 0.5

        # Posisi harga relatif terhadap band
        if close_now > upper_now:
            bb_position = "Above Upper 🔴"
        elif close_now < lower_now:
            bb_position = "Below Lower 🟢"
        else:
            bb_position = "Inside Band"

        # Long signal: Close di bawah Lower Band
        # (setup bounce — TradingView BB Strategy: buy close below lower band)
        bb_long_signal = bool(close_now < lower_now)

        return {
            "bb_long_signal": bb_long_signal,
            "bb_position"   : bb_position,
            "bb_upper"      : round(upper_now, 0),
            "bb_middle"     : round(mid_now, 0),
            "bb_lower"      : round(lower_now, 0),
            "bb_pct_b"      : pct_b,
        }

    except Exception as e:
        return {
            "bb_long_signal": False,
            "bb_position"   : "Error",
            "bb_upper"      : None,
            "bb_middle"     : None,
            "bb_lower"      : None,
            "bb_pct_b"      : None,
        }


# ══════════════════════════════════════════════════════════════════
# ★ NEW: SUPERTREND STRATEGY (TradingView)
# ══════════════════════════════════════════════════════════════════
def calc_supertrend(df: pd.DataFrame, atr_len: int = ST_ATR_LEN, factor: float = ST_FACTOR):
    """
    Supertrend indicator — persis logika TradingView Pine Script.

    Upper Band = (High + Low) / 2 + Factor × ATR(atr_len)
    Lower Band = (High + Low) / 2 - Factor × ATR(atr_len)

    Direction:
      -1 = Supertrend DI BAWAH harga (bullish / long)
      +1 = Supertrend DI ATAS harga (bearish / short)

    Long signal (entry) = Direction berubah dari +1 → -1
    """
    atr = calc_atr(df, atr_len)
    hl2 = (df["High"] + df["Low"]) / 2

    basic_upper = hl2 + factor * atr
    basic_lower = hl2 - factor * atr

    supertrend = pd.Series(np.nan, index=df.index)
    direction  = pd.Series(1,      index=df.index, dtype=int)   # 1=bearish, -1=bullish

    # Final bands (adjusted to never widen against current trend)
    final_upper = basic_upper.copy()
    final_lower = basic_lower.copy()

    for i in range(1, len(df)):
        close_prev = df["Close"].iloc[i - 1]

        # Upper band: only tighten when price is below it
        final_upper.iloc[i] = (
            basic_upper.iloc[i]
            if basic_upper.iloc[i] < final_upper.iloc[i - 1] or close_prev > final_upper.iloc[i - 1]
            else final_upper.iloc[i - 1]
        )
        # Lower band: only tighten when price is above it
        final_lower.iloc[i] = (
            basic_lower.iloc[i]
            if basic_lower.iloc[i] > final_lower.iloc[i - 1] or close_prev < final_lower.iloc[i - 1]
            else final_lower.iloc[i - 1]
        )

        close_now = df["Close"].iloc[i]
        prev_dir  = direction.iloc[i - 1]

        if prev_dir == 1:    # was bearish
            direction.iloc[i]  = -1 if close_now > final_upper.iloc[i] else 1
            supertrend.iloc[i] = final_lower.iloc[i] if direction.iloc[i] == -1 else final_upper.iloc[i]
        else:                # was bullish (-1)
            direction.iloc[i]  = 1 if close_now < final_lower.iloc[i] else -1
            supertrend.iloc[i] = final_upper.iloc[i] if direction.iloc[i] == 1 else final_lower.iloc[i]

    return supertrend, direction


def check_supertrend_strategy(df: pd.DataFrame) -> dict:
    """
    TradingView Supertrend Strategy:
      Long entry  = Direction berubah dari +1 (bearish) → -1 (bullish)
                    yaitu Supertrend baru saja flip dari ATAS → BAWAH harga
      In Long     = Direction saat ini = -1 (Supertrend di bawah harga)

    Returns dict dengan:
      st_long_signal   : bool — terjadi long entry hari ini (fresh flip)
      st_in_long       : bool — saat ini dalam posisi long (ST di bawah harga)
      st_direction     : int  — -1 (bullish) atau +1 (bearish)
      st_value         : float — nilai Supertrend hari ini
      st_signal_label  : str
    """
    try:
        supertrend, direction = calc_supertrend(df, ST_ATR_LEN, ST_FACTOR)

        dir_now  = int(direction.iloc[-1])
        dir_prev = int(direction.iloc[-2])
        st_val   = float(supertrend.iloc[-1])

        # Fresh long flip: kemarin bearish (+1), hari ini bullish (-1)
        st_long_signal = bool(dir_prev == 1 and dir_now == -1)
        st_in_long     = bool(dir_now == -1)

        if st_long_signal:
            label = "🟢 LONG ENTRY (Flip)"
        elif st_in_long:
            label = "🟢 In Long"
        else:
            label = "🔴 Bearish / Short"

        return {
            "st_long_signal" : st_long_signal,
            "st_in_long"     : st_in_long,
            "st_direction"   : dir_now,
            "st_value"       : round(st_val, 0),
            "st_signal_label": label,
        }

    except Exception as e:
        return {
            "st_long_signal" : False,
            "st_in_long"     : False,
            "st_direction"   : 0,
            "st_value"       : None,
            "st_signal_label": "Error",
        }


# ══════════════════════════════════════════════════════════════════
# ★ NEW: RSI STRATEGY (TradingView)
# ══════════════════════════════════════════════════════════════════
def check_rsi_strategy(df: pd.DataFrame) -> dict:
    """
    TradingView RSI Strategy:
      Long entry  = RSI cross UP melewati level Oversold (default 30)
                    yaitu RSI[kemarin] < 30 AND RSI[hari ini] >= 30
      In Long     = RSI saat ini masih di antara Oversold dan Overbought
                    (belum trigger exit)
      Short/Exit  = RSI cross DOWN dari Overbought (70)

    Returns dict dengan:
      rsi_long_signal  : bool — terjadi long entry hari ini (fresh cross up oversold)
      rsi_in_long      : bool — saat ini RSI di zona normal (masih hold long)
      rsi_value        : float
      rsi_signal_label : str
      rsi_zone         : str — "Oversold" / "Overbought" / "Neutral"
    """
    try:
        rsi_series = calc_rsi(df["Close"], RSI_LENGTH)
        rsi_now    = float(rsi_series.iloc[-1])
        rsi_prev   = float(rsi_series.iloc[-2])

        # Long signal: cross UP melewati level oversold
        rsi_long_signal = bool(rsi_prev < RSI_OVERSOLD and rsi_now >= RSI_OVERSOLD)

        # Still in long: RSI naik dari oversold, belum overbought
        rsi_in_long = bool(rsi_now >= RSI_OVERSOLD and rsi_now < RSI_OVERBOUGHT)

        # RSI Zone
        if rsi_now < RSI_OVERSOLD:
            rsi_zone = "Oversold 🟢"
        elif rsi_now > RSI_OVERBOUGHT:
            rsi_zone = "Overbought 🔴"
        else:
            rsi_zone = "Neutral"

        # Signal label
        if rsi_long_signal:
            label = f"🟢 LONG ENTRY (Cross {RSI_OVERSOLD})"
        elif rsi_now < RSI_OVERSOLD:
            label = f"⏳ Approaching Entry (RSI={rsi_now:.1f})"
        elif rsi_in_long:
            label = f"🟢 In Long (RSI={rsi_now:.1f})"
        elif rsi_now > RSI_OVERBOUGHT:
            label = f"🔴 Exit Signal (RSI={rsi_now:.1f})"
        else:
            label = f"Neutral (RSI={rsi_now:.1f})"

        return {
            "rsi_long_signal" : rsi_long_signal,
            "rsi_in_long"     : rsi_in_long,
            "rsi_value"       : round(rsi_now, 1),
            "rsi_signal_label": label,
            "rsi_zone"        : rsi_zone,
        }

    except Exception as e:
        return {
            "rsi_long_signal" : False,
            "rsi_in_long"     : False,
            "rsi_value"       : None,
            "rsi_signal_label": "Error",
            "rsi_zone"        : "N/A",
        }


# ══════════════════════════════════════════════════════════════════
# TRADINGVIEW-STYLE OSCILLATOR SCORE
# ══════════════════════════════════════════════════════════════════
def calc_tv_score(df: pd.DataFrame) -> tuple:
    """Hitung skor gaya TradingView dari berbagai indikator. Returns (score -1..1, label)."""
    try:
        import ta as ta_lib
    except ImportError:
        return 0.0, "Netral"

    score, counted = 0, 0

    def add(val):
        nonlocal score, counted
        score += val
        counted += 1

    try:
        d_now  = df.iloc[-1]
        d_prev = df.iloc[-2]

        # Moving Averages
        for p in [10, 20, 50, 100, 200]:
            sma = df["Close"].rolling(p).mean().iloc[-1]
            ema = df["Close"].ewm(span=p, adjust=False).mean().iloc[-1]
            if pd.notna(sma): add(1 if sma < d_now["Close"] else -1)
            if pd.notna(ema): add(1 if ema < d_now["Close"] else -1)

        # Ichimoku
        try:
            ichi = ta_lib.trend.IchimokuIndicator(df["High"], df["Low"], 9, 26, 52)
            isa  = ichi.ichimoku_a().iloc[-1]
            isb  = ichi.ichimoku_b().iloc[-1]
            if pd.notna(isa) and pd.notna(isb):
                add(1 if d_now["Close"] > max(isa, isb) else -1)
        except Exception:
            pass

        # RSI
        rsi_s = calc_rsi(df["Close"], 14)
        r_now, r_prev = rsi_s.iloc[-1], rsi_s.iloc[-2]
        if r_now < 30 and r_now > r_prev:  add(1)
        elif r_now > 70 and r_now < r_prev: add(-1)
        else: add(0)

        # MACD
        ml, sig = calc_macd(df["Close"])
        add(1 if ml.iloc[-1] > sig.iloc[-1] else -1)

        # ADX
        try:
            adx_i = ta_lib.trend.ADXIndicator(df["High"], df["Low"], df["Close"], 14)
            adx   = adx_i.adx().iloc[-1]
            pdi   = adx_i.adx_pos().iloc[-1]
            mdi   = adx_i.adx_neg().iloc[-1]
            if pd.notna(adx) and adx > 20:
                add(1 if pdi > mdi else -1)
            else:
                add(0)
        except Exception:
            add(0)

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
        print(f"    [ERROR] Download gagal {ticker}: {e}")
        return None

    if df is None or len(df) < 80:
        print(f"    [SKIP]  {ticker} — data tidak cukup "
              f"({len(df) if df is not None else 0} baris)")
        return None

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.droplevel(1)

    df = df.dropna(subset=["Open", "High", "Low", "Close", "Volume"]).copy()
    df.reset_index(inplace=True)

    if len(df) < 60:
        return None

    close_now = float(df["Close"].iloc[-1])
    if close_now <= 0:
        return None

    # ──────────────────────────────────────────────────────────────
    # BASE INDICATORS
    # ──────────────────────────────────────────────────────────────

    # ATR
    atr_series = calc_atr(df, 14)
    atr_val    = float(atr_series.iloc[-1])
    atr_pct    = (atr_val / close_now) * 100

    # Channel Breakout
    upper_prev = df["High"].rolling(CHANNEL_LENGTH).max().shift(1).iloc[-1]
    channel_bo = bool(df["High"].iloc[-1] > upper_prev)

    # Volume
    vol_ma20  = df["Volume"].rolling(20).mean().iloc[-1]
    liquidity = close_now * vol_ma20
    vol_surge = bool(float(df["Volume"].iloc[-1]) > vol_ma20 * VOLUME_SURGE)

    # Price Change 1W
    close_5d  = float(df["Close"].iloc[-6]) if len(df) >= 6 else close_now
    price_chg = (close_now - close_5d) / close_5d * 100 if close_5d > 0 else 0.0

    # Ichimoku
    span_a, span_b = calc_ichimoku(df)
    cloud_top      = pd.concat([span_a, span_b], axis=1).max(axis=1).iloc[-1]
    ichimoku_ok    = bool(close_now > cloud_top) if pd.notna(cloud_top) else False

    # MACD
    macd_line, macd_sig = calc_macd(df["Close"])
    macd_ok = bool(macd_line.iloc[-1] > macd_sig.iloc[-1])

    # SMA context
    sma20      = df["Close"].rolling(20).mean().iloc[-1]
    sma50      = df["Close"].rolling(50).mean().iloc[-1]
    sma20_prev = df["Close"].rolling(20).mean().iloc[-6]
    is_uptrend = bool((sma20 > sma50) and (sma20 > sma20_prev) and (close_now > sma50))

    # RSI value (for TV score & display)
    rsi_series = calc_rsi(df["Close"], RSI_LENGTH)
    rsi_val    = float(rsi_series.iloc[-1])

    # ADX
    adx_val = 0.0
    try:
        import ta as ta_lib
        adx_ind = ta_lib.trend.ADXIndicator(df["High"], df["Low"], df["Close"], 14)
        adx_val = float(adx_ind.adx().iloc[-1])
    except Exception:
        pass

    # S/R position
    rh20     = df["High"].rolling(20).max().iloc[-1]
    rl20     = df["Low"].rolling(20).min().iloc[-1]
    rng      = rh20 - rl20
    pos      = (close_now - rl20) / rng if rng > 0 else 0.5
    sr_label     = "Support" if pos < 0.25 else "Resistance" if pos > 0.75 else "-"
    near_support = bool(pos < 0.25)

    # TV Score
    tv_score, tv_label = calc_tv_score(df)

    # ──────────────────────────────────────────────────────────────
    # ★ THREE TV STRATEGIES
    # ──────────────────────────────────────────────────────────────
    bb_result = check_bb_strategy(df)
    st_result = check_supertrend_strategy(df)
    rsi_strat = check_rsi_strategy(df)

    # ──────────────────────────────────────────────────────────────
    # SCORING (9 kondisi, masing-masing +1)
    # ──────────────────────────────────────────────────────────────
    liq_ok    = bool(liquidity > MIN_LIQUIDITY)
    atr_ok    = bool(ATR_MIN_PCT <= atr_pct <= ATR_MAX_PCT)
    pchg_ok   = bool(abs(price_chg) <= PRICE_CHANGE_MAX)
    vol_ok    = vol_surge
    # Kondisi 7-9: TV Strategies
    bb_ok     = bool(bb_result["bb_long_signal"])    # BB: setup long (close < lower band)
    st_ok     = bool(st_result["st_in_long"])         # ST: in long (direction = bullish)
    rsi_ok    = bool(rsi_strat["rsi_long_signal"] or
                     (rsi_strat["rsi_in_long"] and rsi_val < 50))  # RSI: just crossed OR building

    score_base = sum([
        liq_ok,       # 1
        atr_ok,       # 2
        pchg_ok,      # 3
        ichimoku_ok,  # 4
        macd_ok,      # 5
        vol_ok,       # 6
        bb_ok,        # 7 ★ BB Strategy
        st_ok,        # 8 ★ Supertrend Strategy
        rsi_ok,       # 9 ★ RSI Strategy
    ])

    # ──────────────────────────────────────────────────────────────
    # TIER DETERMINATION (updated untuk 9 kondisi)
    # ──────────────────────────────────────────────────────────────
    tier = 0
    if channel_bo:
        tier_confluences = sum([
            liq_ok, atr_ok, pchg_ok, ichimoku_ok, macd_ok, vol_ok,
            bb_ok, st_ok, rsi_ok,
            is_uptrend, near_support
        ])
        if tier_confluences >= 8:
            tier = 3
        elif tier_confluences >= 5:
            tier = 2
        elif tier_confluences >= 3:
            tier = 1

    # Probabilistic score
    BASE_PROB = 0.58
    pat_score = 0.0
    if tier > 0:
        pat_score = get_pattern_score(BASE_PROB, tier, score_base)

    # ──────────────────────────────────────────────────────────────
    # COMMODITY CONTEXT
    # ──────────────────────────────────────────────────────────────
    comm_score = get_sector_commodity_score(sector, commodity_ctx)
    score_gate = get_score_gate(comm_score)

    # ──────────────────────────────────────────────────────────────
    # DECISION
    # ──────────────────────────────────────────────────────────────
    direction = "bullish" if channel_bo else "neutral"

    # Hitung berapa dari 3 strategi TV yang lolos
    tv_strategy_count = sum([bb_ok, st_ok, rsi_ok])

    if channel_bo and score_base >= score_gate:
        if comm_score["bullish_pct"] >= 70 and tier >= 2 and tv_strategy_count >= 2:
            decision = "⭐ OPEN BUY BESOK (STRONG)"
        elif comm_score["bullish_pct"] <= 35:
            decision = "⚠️ OPEN BUY (CAUTIOUS)"
        else:
            decision = "✅ OPEN BUY BESOK"
    elif channel_bo:
        decision = "👀 WATCHLIST (Breakout)"
    else:
        decision = "WATCHLIST"

    # ──────────────────────────────────────────────────────────────
    # SL / TP
    # ──────────────────────────────────────────────────────────────
    trade = calculate_trade_params(close_now, atr_val, direction, tier if tier > 0 else 1)

    # ──────────────────────────────────────────────────────────────
    # COMMODITY SUMMARY
    # ──────────────────────────────────────────────────────────────
    comm_summary = " | ".join([
        f"{'✅' if u else '⚠️'}{n}"
        for n, u in comm_score["relevant"][:5]
    ])

    # ──────────────────────────────────────────────────────────────
    # RETURN DICT (semua kolom output)
    # ──────────────────────────────────────────────────────────────
    return {
        # ── Identitas ─────────────────────────────────────────
        "Ticker"                 : ticker.replace(".JK", ""),
        "Sektor"                 : sector,
        "Harga"                  : int(close_now),

        # ── Channel Breakout ──────────────────────────────────
        "Channel Breakout"       : "✅ BREAKOUT" if channel_bo else "❌ NO",

        # ── Score & Tier ──────────────────────────────────────
        "Score (/9)"             : score_base,
        "Tier"                   : tier,
        "Tier Label"             : get_tier_label(tier) if tier > 0 else "-",
        "Skor Probabilistik"     : pat_score,

        # ── 6 Kondisi Dasar ───────────────────────────────────
        "Liquidity (Bil IDR)"    : round(liquidity / 1e9, 2),
        "ATR 14 (%)"             : round(atr_pct, 2),
        "Price Change 1W (%)"    : round(price_chg, 2),
        "Harga>Ichimoku Cloud"   : "✅" if ichimoku_ok else "❌",
        "MACD Bullish"           : "✅" if macd_ok else "❌",
        "Volume Surge"           : "✅" if vol_surge else "❌",

        # ── ★ BB Strategy ─────────────────────────────────────
        "BB Signal"              : "✅ LOLOS" if bb_ok else "❌",
        "BB Position"            : bb_result["bb_position"],
        "BB Upper"               : bb_result["bb_upper"],
        "BB Middle"              : bb_result["bb_middle"],
        "BB Lower"               : bb_result["bb_lower"],
        "BB %B"                  : bb_result["bb_pct_b"],

        # ── ★ Supertrend Strategy ─────────────────────────────
        "Supertrend Signal"      : "✅ LOLOS" if st_ok else "❌",
        "Supertrend Status"      : st_result["st_signal_label"],
        "Supertrend Value"       : st_result["st_value"],
        "ST Direction"           : "Bullish 🟢" if st_result["st_direction"] == -1 else "Bearish 🔴",

        # ── ★ RSI Strategy ────────────────────────────────────
        "RSI Strategy Signal"    : "✅ LOLOS" if rsi_ok else "❌",
        "RSI Status"             : rsi_strat["rsi_signal_label"],
        "RSI Zone"               : rsi_strat["rsi_zone"],

        # ── TV Strategies Summary ─────────────────────────────
        "TV Strategies Lolos"    : f"{tv_strategy_count}/3",

        # ── Tech Indicators ───────────────────────────────────
        "RSI"                    : round(rsi_val, 1),
        "ADX"                    : round(adx_val, 1),
        "Skor TV"                : tv_score,
        "Rek TV"                 : tv_label,
        "S/R Zone"               : sr_label,

        # ── Trade Management ──────────────────────────────────
        "SL"                     : trade["SL"],
        "TP1"                    : trade["TP1"],
        "TP2"                    : trade["TP2"],
        "RR"                     : trade["RR"],
        "ATR"                    : int(atr_val),

        # ── Commodity Context ─────────────────────────────────
        "Commodity Bullish %"    : comm_score["bullish_pct"],
        "Score Gate"             : score_gate,
        "Commodity Context"      : comm_summary,

        # ── Decision ──────────────────────────────────────────
        "Decision"               : decision,

        # ── Meta ──────────────────────────────────────────────
        "Waktu"                  : datetime.now(WIB).strftime("%Y-%m-%d %H:%M"),
    }


# ══════════════════════════════════════════════════════════════════
# SECTOR ANALYSIS
# ══════════════════════════════════════════════════════════════════
def analyze_sector(sector_name: str, tickers: list, commodity_ctx: dict) -> pd.DataFrame:
    print(f"\n📊 Scan sektor: {sector_name} ({len(tickers)} emiten)")

    comm_score = get_sector_commodity_score(sector_name, commodity_ctx)
    gate       = get_score_gate(comm_score)
    print(f"   Commodity bullish: {comm_score['bullish_pct']}% "
          f"({comm_score['bullish_count']}/{comm_score['total']})  → Gate: Score ≥ {gate}")

    results = []
    for i, ticker in enumerate(tickers, 1):
        print(f"  [{i:>2}/{len(tickers)}] Processing {ticker}...", end=" ")
        try:
            res = analyze_ticker(ticker, sector_name, commodity_ctx)
            if res:
                results.append(res)
                cb   = "🔼" if res["Channel Breakout"] == "✅ BREAKOUT" else "  "
                tv_s = res["TV Strategies Lolos"]
                print(f"Score={res['Score (/9)']}  Tier={res['Tier']}  "
                      f"TV={tv_s}  {cb}  → {res['Decision']}")
            else:
                print("skip")
        except Exception as e:
            print(f"ERROR: {e}")
        time.sleep(0.25)

    if not results:
        return pd.DataFrame()

    df_out = pd.DataFrame(results)
    df_out = df_out.sort_values(
        by=["Tier", "Skor Probabilistik", "Score (/9)"],
        ascending=[False, False, False]
    ).reset_index(drop=True)
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
            f"📊 {sector} — Channel Breakout Screener v2.1 | {run_time}"
        ]])
        ws.update("A2", [[
            f"Commodity Bullish: {comm_score['bullish_pct']}%",
            f"({comm_score['bullish_count']}/{comm_score['total']} relevan)",
            "",
            f"Score Gate: ≥ {get_score_gate(comm_score)}",
            "",
            f"BB(20,2) | Supertrend({ST_ATR_LEN},{ST_FACTOR}) | RSI({RSI_LENGTH},{RSI_OVERSOLD}/{RSI_OVERBOUGHT})",
        ]])
        ws.update("A3", [[""]])

        set_with_dataframe(ws, df, row=4, col=1, include_index=False)

        n_cols = len(df.columns)
        ws.format(f"A4:{chr(64 + min(n_cols, 26))}4", {
            "textFormat"     : {"bold": True},
            "backgroundColor": {"red": 0.18, "green": 0.18, "blue": 0.54},
        })

        n_strong = df[df["Decision"].str.contains("STRONG", na=False)].shape[0]
        n_buy    = df[df["Decision"].str.contains("OPEN BUY", na=False)].shape[0]
        n_3s     = df[df["TV Strategies Lolos"] == "3/3"].shape[0]
        print(f"  ✅ Upload {sector}: {len(df)} emiten | "
              f"{n_buy} OPEN BUY ({n_strong} STRONG) | {n_3s} lolos 3/3 TV Strategy")

    except Exception as e:
        print(f"  ❌ Upload error {sector}: {e}")


# ══════════════════════════════════════════════════════════════════
# UPLOAD SUMMARY SHEET
# ══════════════════════════════════════════════════════════════════
def upload_summary_sheet(all_results: list, commodity_ctx: dict):
    print(f"\n📤 Uploading SUMMARY → sheet '{SUMMARY_SHEET}'...")
    ws = connect_gsheet(SUMMARY_SHEET)
    if not ws:
        return

    try:
        ws.clear()
        run_time = datetime.now(WIB).strftime("%Y-%m-%d %H:%M WIB")

        # ── Header & Commodity Status Block ──
        ws.update("A1", [[
            f"🔍 IHSG Channel Breakout Screener v2.1 — {run_time}",
            "", "",
            f"BB({BB_LENGTH},{BB_MULT}) | "
            f"Supertrend({ST_ATR_LEN},{ST_FACTOR}) | "
            f"RSI({RSI_LENGTH} | OS:{RSI_OVERSOLD} OB:{RSI_OVERBOUGHT})"
        ]])

        comm_rows = [[
            "Commodity / Index", "Ticker", "Close", f"MA", "1W Chg%", "Status"
        ]]
        for name, data in commodity_ctx.items():
            if data.get("close") is None:
                comm_rows.append([name, data["ticker"], "N/A", "N/A", "N/A", "❌ Error"])
                continue
            if name == "DXY":
                status = "✅ Weak (Bullish EM)" if data["uptrend"] else "⚠️ Strong (Bearish EM)"
            else:
                status = "✅ Bullish" if data["uptrend"] else "⚠️ Bearish"
            comm_rows.append([
                name, data["ticker"],
                data["close"], data["ma"],
                f"{data['change_pct']:+.2f}%", status,
            ])

        ws.update("A2", comm_rows)
        separator_row = len(comm_rows) + 3
        ws.update(f"A{separator_row}", [[""]])

        # ── Summary Data ──
        summary_start = separator_row + 1
        df_all = pd.DataFrame(all_results)
        if df_all.empty:
            print("  ⚠️  Tidak ada hasil.")
            return

        df_summary = df_all.sort_values(
            by=["Tier", "Skor Probabilistik", "Score (/9)"],
            ascending=[False, False, False]
        ).reset_index(drop=True)

        cols_summary = [
            "Sektor", "Ticker", "Harga",
            "Channel Breakout",
            "Score (/9)", "Tier", "Tier Label", "Skor Probabilistik",
            # 6 kondisi dasar
            "Liquidity (Bil IDR)", "ATR 14 (%)", "Price Change 1W (%)",
            "Harga>Ichimoku Cloud", "MACD Bullish", "Volume Surge",
            # ★ 3 TV Strategies
            "TV Strategies Lolos",
            "BB Signal", "BB Position", "BB %B",
            "Supertrend Signal", "Supertrend Status", "ST Direction",
            "RSI Strategy Signal", "RSI Status", "RSI Zone",
            # Tech
            "RSI", "ADX", "Rek TV",
            # Trade
            "SL", "TP1", "TP2", "RR",
            # Context
            "Commodity Bullish %", "Score Gate",
            "Decision", "Waktu",
        ]

        available  = [c for c in cols_summary if c in df_summary.columns]
        df_display = df_summary[available]

        set_with_dataframe(ws, df_display, row=summary_start, col=1, include_index=False)

        n_cols = len(df_display.columns)
        ws.format(
            f"A{summary_start}:{chr(64 + min(n_cols, 26))}{summary_start}",
            {
                "textFormat"     : {"bold": True},
                "backgroundColor": {"red": 0.12, "green": 0.36, "blue": 0.24},
            }
        )

        n_buys   = df_summary[df_summary["Decision"].str.contains("OPEN BUY", na=False)].shape[0]
        n_strong = df_summary[df_summary["Decision"].str.contains("STRONG", na=False)].shape[0]
        n_3s     = df_summary[df_summary["TV Strategies Lolos"] == "3/3"].shape[0]
        print(f"  ✅ Summary: {len(df_summary)} emiten | "
              f"{n_buys} OPEN BUY ({n_strong} STRONG) | {n_3s} lolos 3/3 TV Strategy")

    except Exception as e:
        print(f"  ❌ Upload summary error: {e}")


# ══════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════
def main():
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║   IHSG CHANNEL BREAKOUT SCREENER v2.1                        ║")
    print("║   BB Strategy | Supertrend | RSI Strategy                    ║")
    print("║   Sector Framework | Tier System | SL/TP | Commodity         ║")
    print(f"║   {datetime.now(WIB).strftime('%Y-%m-%d %H:%M WIB'):<58}║")
    print("╚══════════════════════════════════════════════════════════════╝")

    print(f"\n  BB Strategy   : Length={BB_LENGTH}, Mult={BB_MULT}")
    print(f"  Supertrend    : ATR Length={ST_ATR_LEN}, Factor={ST_FACTOR}")
    print(f"  RSI Strategy  : Length={RSI_LENGTH}, Oversold={RSI_OVERSOLD}, "
          f"Overbought={RSI_OVERBOUGHT}")
    print(f"  Score Max     : 9 kondisi (6 base + 3 TV strategies)\n")

    # 1. Commodity context
    commodity_ctx = fetch_commodity_context()

    # 2. Per-sektor scan
    all_results = []
    for sector_name, tickers in SECTOR_CONFIG.items():
        df_sector = analyze_sector(sector_name, tickers, commodity_ctx)
        if df_sector.empty:
            print(f"  ⚠️  Tidak ada data untuk {sector_name}")
            continue

        all_results.extend(df_sector.to_dict("records"))
        upload_sector_sheet(sector_name, df_sector, commodity_ctx)
        time.sleep(2)

    # 3. Summary
    if all_results:
        upload_summary_sheet(all_results, commodity_ctx)
    else:
        print("\n❌ Tidak ada hasil. Exit.")
        sys.exit(1)

    # 4. Final print
    df_final = pd.DataFrame(all_results)
    n_bo     = df_final[df_final["Channel Breakout"] == "✅ BREAKOUT"].shape[0]
    n_buy    = df_final[df_final["Decision"].str.contains("OPEN BUY", na=False)].shape[0]
    n_strong = df_final[df_final["Decision"].str.contains("STRONG", na=False)].shape[0]
    n_t3     = df_final[df_final["Tier"] == 3].shape[0]
    n_t2     = df_final[df_final["Tier"] == 2].shape[0]
    n_3strat = df_final[df_final["TV Strategies Lolos"] == "3/3"].shape[0]
    n_2strat = df_final[df_final["TV Strategies Lolos"] == "2/3"].shape[0]

    print("\n" + "═" * 65)
    print("  FINAL SUMMARY")
    print(f"  Total emiten discreen  : {len(df_final)}")
    print(f"  Channel Breakout       : {n_bo}")
    print(f"  OPEN BUY BESOK         : {n_buy}  (incl. {n_strong} ⭐ STRONG)")
    print(f"  Tier 3 ⭐⭐⭐           : {n_t3}")
    print(f"  Tier 2 ⭐⭐             : {n_t2}")
    print(f"  Lolos 3/3 TV Strategies: {n_3strat}")
    print(f"  Lolos 2/3 TV Strategies: {n_2strat}")
    print("═" * 65)
    print("\n✅ Done!\n")


if __name__ == "__main__":
    main()
