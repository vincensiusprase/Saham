"""
IHSG Channel Breakout Screener v3.5 (Complete Trading System)
=============================================================
Pine Script (exact stateful emulation):
  upBound   = ta.highest(high, length)
  downBound = ta.lowest(low, length)
  ChBrkLE : high[t] > upBound[t-1]
  ChBrkSE : low[t]  < downBound[t-1]
"""

import os, sys, json, time, warnings
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
import yfinance as yf
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials

warnings.filterwarnings("ignore")

# ── CONFIG ─────────────────────────────────────────────────────────────────
SPREADSHEET_ID  = "1qhEZkfdtEGDEA5tWTVCeOahOXDso3H4O7B69xkOREDA"
SUMMARY_SHEET   = "SCREENER"
SERVICE_ACCOUNT = "service_account.json"
LENGTH          = 5
DOWNLOAD_DAYS   = 365
WIB             = timezone(timedelta(hours=7))

# ── SECTORS ────────────────────────────────────────────────────────────────
SECTORS = {
    "IDXFINANCE" : ["BBCA.JK","BBRI.JK","BMRI.JK","BBNI.JK","BRIS.JK",
                    "BBTN.JK","BNGA.JK","NISP.JK","BDMN.JK","BTPS.JK"],
    "IDXENERGY"  : ["ADRO.JK","BYAN.JK","PTBA.JK","ITMG.JK","HRUM.JK",
                    "MEDC.JK","PGAS.JK","ELSA.JK","AKRA.JK","INDY.JK"],
    "IDXBASIC"   : ["ANTM.JK","MDKA.JK","INCO.JK","TINS.JK","SMGR.JK",
                    "INTP.JK","TPIA.JK","INKP.JK","TKIM.JK","MBMA.JK"],
    "IDXINDUST"  : ["ASII.JK","UNTR.JK","SCCO.JK","KBLI.JK","VOKS.JK",
                    "AMFG.JK","ARNA.JK","TOTO.JK","LION.JK","ASGR.JK"],
    "IDXNONCYC"  : ["UNVR.JK","ICBP.JK","INDF.JK","MYOR.JK","CPIN.JK",
                    "JPFA.JK","ULTJ.JK","SOFA.JK","KLBF.JK","GGRM.JK"],
    "IDXCYCLIC"  : ["MAPI.JK","ACES.JK","ERAA.JK","LPPF.JK","MNCN.JK",
                    "SCMA.JK","AUTO.JK","GJTL.JK","SMSM.JK","FAST.JK"],
    "IDXTECHNO"  : ["GOTO.JK","EMTK.JK","BUKA.JK","MSTI.JK","MTDL.JK",
                    "MLPT.JK","CASH.JK","KREN.JK","HDIT.JK","NFCX.JK"],
    "IDXHEALTH"  : ["KLBF.JK","SIDO.JK","KAEF.JK","TSPC.JK","MIKA.JK",
                    "SILO.JK","HEAL.JK","MERK.JK","DVLA.JK","PRDA.JK"],
    "IDXINFRA"   : ["TLKM.JK","EXCL.JK","ISAT.JK","TOWR.JK","JSMR.JK",
                    "TBIG.JK","MTEL.JK","WIKA.JK","PTPP.JK","ADHI.JK"],
    "IDXPROPERT" : ["CTRA.JK","BSDE.JK","PWON.JK","SMRA.JK","ASRI.JK",
                    "LPKR.JK","DMAS.JK","DUTI.JK","MTLA.JK","BEST.JK"],
    "IDXTRANS"   : ["GIAA.JK","SMDR.JK","BIRD.JK","ASSA.JK","TMAS.JK",
                    "JSMR.JK","WEHA.JK","NELY.JK","SAFE.JK","LRNA.JK"],
}

# ── COMMODITIES ────────────────────────────────────────────────────────────
COMMODITIES = {
    "Gold"      : {"t":"GC=F",     "ma":20, "s":["IDXBASIC","IDXFINANCE"]},
    "Silver"    : {"t":"SI=F",     "ma":20, "s":["IDXBASIC","IDXINDUST"]},
    "Copper"    : {"t":"HG=F",     "ma":20, "s":["IDXBASIC","IDXINDUST","IDXINFRA"]},
    "Nickel"    : {"t":"NI=F",     "ma":20, "s":["IDXBASIC","IDXINDUST"]},
    "Aluminium" : {"t":"ALI=F",    "ma":20, "s":["IDXBASIC","IDXINDUST"]},
    "Zinc"      : {"t":"ZNC=F",    "ma":20, "s":["IDXBASIC","IDXINDUST"]},
    "Tin"       : {"t":"JJT",      "ma":20, "s":["IDXBASIC"]},
    "Brent Oil" : {"t":"BZ=F",     "ma":20, "s":["IDXENERGY","IDXNONCYC","IDXTRANS"]},
    "Crude Oil" : {"t":"CL=F",     "ma":20, "s":["IDXENERGY","IDXNONCYC","IDXTRANS"]},
    "Nat Gas"   : {"t":"NG=F",     "ma":20, "s":["IDXENERGY","IDXINFRA"]},
    "Coal"      : {"t":"KOL",      "ma":20, "s":["IDXENERGY","IDXBASIC"]},
    "CPO"       : {"t":"FCPO.KL",  "ma":20, "s":["IDXNONCYC","IDXBASIC"]},
    "Corn"      : {"t":"ZC=F",     "ma":20, "s":["IDXNONCYC"]},
    "Wheat"     : {"t":"ZW=F",     "ma":20, "s":["IDXNONCYC"]},
    "DXY"       : {"t":"DX-Y.NYB", "ma":20, "s":["ALL"]},
    "IHSG"      : {"t":"^JKSE",    "ma":50, "s":["ALL"]},
}
FALLBACK = {"Nickel":"DBB","Aluminium":"DBB","Zinc":"DBB",
            "Tin":"JJT","Coal":"ARCH","CPO":"POW.L"}

DISPLAY_COLS = [
    "Ticker", "Sektor", "Action", "Harga", "Batas Jual (SL)", 
    "Channel Breakout", "Tgl Breakout", "Skor Tambahan", "ADTV (M)",
    "Skor TV", "Rek TV",
    "Commodity Bullish %", "Commodity Context",
]


# ── GOOGLE SHEETS ──────────────────────────────────────────────────────────
def gsheet(name):
    try:
        raw = os.environ.get("GCP_SA_KEY") or os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
        if raw:
            creds = Credentials.from_service_account_info(
                json.loads(raw),
                scopes=["https://www.googleapis.com/auth/spreadsheets",
                        "https://www.googleapis.com/auth/drive"])
        elif os.path.exists(SERVICE_ACCOUNT):
            creds = Credentials.from_service_account_file(
                SERVICE_ACCOUNT,
                scopes=["https://spreadsheets.google.com/feeds",
                        "https://www.googleapis.com/auth/drive"])
        else:
            print("❌ No credentials"); return None
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SPREADSHEET_ID)
        try:    return sh.worksheet(name)
        except: return sh.add_worksheet(title=name, rows="500", cols="15")
    except Exception as e:
        print(f"❌ GSheet: {e}"); return None


# ── DATA DOWNLOAD ──────────────────────────────────────────────────────────
def get_ohlcv(ticker, days=DOWNLOAD_DAYS):
    end   = datetime.today()
    start = end - timedelta(days=days)
    try:
        df = yf.download(
            ticker,
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
            interval="1d",
            progress=False,
            auto_adjust=False,
        )
    except Exception as e:
        return None

    if df is None or df.empty:
        return None

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]

    if "Close" not in df.columns and "Adj Close" in df.columns:
        df["Close"] = df["Adj Close"]

    needed = ["High", "Low", "Close", "Volume"]
    if not all(c in df.columns for c in needed):
        return None

    df = df[["Open","High","Low","Close","Volume"]].dropna(
        subset=["High","Low","Close"]).copy()
    df.index = pd.to_datetime(df.index)
    df.sort_index(inplace=True)
    return df


# ── CHANNEL BREAKOUT (PERBAIKAN AKURASI) ───────────────────────────────────
def calc_cb(df):
    up   = df["High"].rolling(LENGTH).max().shift(1)
    down = df["Low"].rolling(LENGTH).min().shift(1)

    le_condition = df["High"] > up
    se_condition = df["Low"]  < down

    df_sig = pd.DataFrame(index=df.index)
    conditions = [le_condition, se_condition]
    choices = [1, -1] 
    
    df_sig['state'] = np.select(conditions, choices, default=np.nan)
    df_sig['state'] = df_sig['state'].ffill()

    df_sig['entry_long']  = (df_sig['state'] == 1)  & (df_sig['state'].shift(1) != 1)
    df_sig['entry_short'] = (df_sig['state'] == -1) & (df_sig['state'].shift(1) != -1)

    def get_last_signal(series):
        arr = series.values
        idx = df.index
        for i in range(len(arr)-1, -1, -1):
            if arr[i]:
                bars = len(arr) - 1 - i
                return idx[i], bars
        return None, None

    le_dt, le_b = get_last_signal(df_sig['entry_long'])
    se_dt, se_b = get_last_signal(df_sig['entry_short'])

    def fmt_label(bars, lbl):
        ago = "hari ini" if bars == 0 else f"{bars} bar lalu"
        return f"{lbl} ({ago})"

    def fmt_date(dt):
        if dt is None: return "-"
        return pd.Timestamp(dt).strftime('%d-%b-%y')

    lb = le_b if le_b is not None else 999999
    sb = se_b if se_b is not None else 999999

    if lb <= sb:
        return {"label": fmt_label(lb, "ChBrkLE"), "date": fmt_date(le_dt), "type": "ChBrkLE", "bars": lb}
    else:
        return {"label": fmt_label(sb, "ChBrkSE"), "date": fmt_date(se_dt), "type": "ChBrkSE", "bars": sb}


# ── CUSTOM SCORE & ADTV ────────────────────────────────────────────────────
def calc_custom_score(df):
    score = 0
    c = df["Close"]
    v = df["Volume"]
    tv = c * v  
    
    tv_ma20 = tv.rolling(20).mean()
    v_ma20 = v.rolling(20).mean()
    
    adtv_1m = 0
    if len(tv_ma20) > 0 and pd.notna(tv_ma20.iloc[-1]):
        adtv_1m = tv_ma20.iloc[-1] / 1_000_000_000  # Konversi ke Miliar
        
    if tv.iloc[-1] > tv_ma20.iloc[-1]: score += 1
    if v.iloc[-1] > v_ma20.iloc[-1]: score += 1
        
    if len(df) >= 52:
        tenkan = (df["High"].rolling(9).max() + df["Low"].rolling(9).min()) / 2
        kijun = (df["High"].rolling(26).max() + df["Low"].rolling(26).min()) / 2
        span_a = ((tenkan + kijun) / 2).shift(26)
        span_b = ((df["High"].rolling(52).max() + df["Low"].rolling(52).min()) / 2).shift(26)
        cloud_top = pd.concat([span_a, span_b], axis=1).max(axis=1)
        
        if pd.notna(cloud_top.iloc[-1]) and c.iloc[-1] > cloud_top.iloc[-1]:
            score += 2
            
    return score, round(adtv_1m, 2)


# ── TV SCORE ───────────────────────────────────────────────────────────────
def calc_tv(df):
    s, n = 0, 0
    def add(v): nonlocal s,n; s+=v; n+=1
    try:
        c  = df["Close"];  h = df["High"];  l = df["Low"]
        cn = float(c.iloc[-1])
        for p in [10,20,50,100,200]:
            sma = c.rolling(p).mean().iloc[-1]
            ema = c.ewm(span=p,adjust=False).mean().iloc[-1]
            if pd.notna(sma): add(1 if cn>sma else -1)
            if pd.notna(ema): add(1 if cn>ema else -1)
        tk = (h.rolling(9).max()+l.rolling(9).min())/2
        kj = (h.rolling(26).max()+l.rolling(26).min())/2
        sa = ((tk+kj)/2).shift(26); sb2 = ((h.rolling(52).max()+l.rolling(52).min())/2).shift(26)
        ct = pd.concat([sa,sb2],axis=1).max(axis=1).iloc[-1]
        if pd.notna(ct): add(1 if cn>ct else -1)
        d  = c.diff()
        g  = d.clip(lower=0).ewm(com=13,adjust=False).mean()
        ls = (-d.clip(upper=0)).ewm(com=13,adjust=False).mean()
        rsi= (100-100/(1+g/ls.replace(0,np.nan)))
        r0,r1 = float(rsi.iloc[-1]),float(rsi.iloc[-2])
        if r0<30 and r0>r1: add(1)
        elif r0>70 and r0<r1: add(-1)
        else: add(0)
        fast=c.ewm(span=12,adjust=False).mean(); slow=c.ewm(span=26,adjust=False).mean()
        macd=fast-slow; sig=macd.ewm(span=9,adjust=False).mean()
        add(1 if macd.iloc[-1]>sig.iloc[-1] else -1)
        try:
            import ta as _ta
            adxi=_ta.trend.ADXIndicator(h,l,c,14)
            av=adxi.adx().iloc[-1]; pdi=adxi.adx_pos().iloc[-1]; mdi=adxi.adx_neg().iloc[-1]
            if pd.notna(av) and av>20: add(1 if pdi>mdi else -1)
            else: add(0)
        except: add(0)
        mom=c.diff(10); add(1 if mom.iloc[-1]>mom.iloc[-2] else -1)
        fv = s/n if n>0 else 0
        lbl=("Jual Kuat" if fv<=-0.5 else "Jual" if fv<=-0.1 else
             "Netral"   if fv< 0.1  else "Beli" if fv< 0.5  else "Beli Kuat")
        return round(fv,2), lbl
    except:
        return 0.0,"Netral"


# ── COMMODITY ──────────────────────────────────────────────────────────────
def fetch_commodities():
    print("\n📦 Commodities...")
    ctx = {}
    for name, cfg in COMMODITIES.items():
        for ticker in [cfg["t"], FALLBACK.get(name,"")]:
            if not ticker: continue
            try:
                raw = yf.download(ticker, period="90d", interval="1d",
                                  progress=False, auto_adjust=True)
                if raw is None or len(raw) < cfg["ma"]+5: continue
                if isinstance(raw.columns, pd.MultiIndex):
                    raw.columns = [c[0] for c in raw.columns]
                c  = raw["Close"]
                ma = float(c.rolling(cfg["ma"]).mean().iloc[-1])
                cl = float(c.iloc[-1])
                pr = float(c.iloc[-6]) if len(raw)>=6 else cl
                chg= round((cl-pr)/pr*100,2) if pr>0 else 0
                up = (cl<ma) if name=="DXY" else (cl>ma)
                ctx[name]={"up":up,"close":round(cl,2),"ma":round(ma,2),
                           "chg":chg,"s":cfg["s"],"ticker":ticker}
                print(f"  {'✅' if up else '⚠️'} {name:<12} {cl:>10.2f}  MA={ma:>10.2f}  {chg:>+6.2f}%")
                break
            except: continue
        if name not in ctx:
            ctx[name]={"up":None,"s":cfg["s"],"ticker":cfg["t"]}
            print(f"  ❌ {name}")
    return ctx

def comm_sector(sector, ctx):
    rel=[(n,d["up"]) for n,d in ctx.items()
         if d.get("up") is not None and ("ALL" in d["s"] or sector in d["s"])]
    if not rel: return {"pct":50.0,"summary":"-"}
    bull=sum(1 for _,u in rel if u)
    pct =round(bull/len(rel)*100,1)
    summ=" | ".join(f"{'✅' if u else '⚠️'}{n}" for n,u in rel[:5])
    return {"pct":pct,"summary":summ}


# ── ANALYZE TICKER ─────────────────────────────────────────────────────────
def analyze(ticker, sector, ctx):
    df = get_ohlcv(ticker)
    if df is None or len(df) < LENGTH+2:
        print(f"    [skip] {ticker}"); return None

    cb = calc_cb(df)
    custom_score, adtv = calc_custom_score(df)
    tvs, tvl = calc_tv(df)
    comm = comm_sector(sector, ctx)
    close_now = float(df["Close"].iloc[-1])
    tgl = df.index[-1].strftime("%d-%b-%y")

    # Action Logic
    if cb["type"] == "ChBrkLE":
        if cb["bars"] <= 1: action = "BUY NOW"
        else: action = "HOLD"
    else:
        action = "SELL / WAIT"

    # Liquidity Filter (ADTV < 1 Miliar = Warning)
    ticker_display = ticker.replace(".JK", "")
    if adtv < 1.0:
        ticker_display = f"⚠️ {ticker_display}"

    # Batas Jual (SL) = downBound[t-1]
    down = df["Low"].rolling(LENGTH).min().shift(1)
    sl_price = down.iloc[-1] if not pd.isna(down.iloc[-1]) else 0

    return {
        "Ticker"              : ticker_display,
        "Sektor"              : sector,
        "Action"              : action,
        "Harga"               : int(close_now),
        "Batas Jual (SL)"     : int(sl_price),
        "ADTV (M)"            : adtv,
        "Tgl Data"            : tgl,
        "Channel Breakout"    : cb["label"],
        "Tgl Breakout"        : cb["date"],
        "Skor Tambahan"       : custom_score,
        "_type"               : cb["type"],
        "_bars"               : cb["bars"],
        "Skor TV"             : tvs,
        "Rek TV"              : tvl,
        "Commodity Bullish %" : comm["pct"],
        "Commodity Context"   : comm["summary"],
    }


# ── SECTOR ─────────────────────────────────────────────────────────────────
def run_sector(sector, tickers, ctx):
    print(f"\n📊 {sector}")
    rows=[]
    for i,t in enumerate(tickers,1):
        print(f"  [{i:>2}/{len(tickers)}] {t}...", end=" ", flush=True)
        r=analyze(t,sector,ctx)
        if r: rows.append(r); print(r["Channel Breakout"])
        else: print("skip")
        time.sleep(0.3)
    if not rows: return pd.DataFrame()
    df=pd.DataFrame(rows)
    df["_le"]=(df["_type"]=="ChBrkLE").astype(int)
    df=df.sort_values(["_le","_bars"],ascending=[False,True])
    df=df.drop(columns=["_type","_bars","_le"]).reset_index(drop=True)
    return df


# ── UPLOAD ─────────────────────────────────────────────────────────────────
def upload_sector(sector, df, ctx):
    ws=gsheet(sector)
    if not ws: return
    try:
        ws.clear()
        ts=datetime.now(WIB).strftime("%Y-%m-%d %H:%M WIB")
        cm=comm_sector(sector,ctx)
        ws.update("A1",[[f"📊 {sector} | ChannelBreakOut (Length={LENGTH}) | {ts}"]])
        ws.update("A2",[[f"Commodity Bullish: {cm['pct']}%"]])
        ws.update("A3",[[""]])
        avail=[c for c in DISPLAY_COLS if c in df.columns]
        set_with_dataframe(ws,df[avail],row=4,col=1,include_index=False)
        nc=len(avail)
        ws.format(f"A4:{chr(64+min(nc,26))}4",
                  {"textFormat":{"bold":True},
                   "backgroundColor":{"red":0.12,"green":0.20,"blue":0.50}})
        print(f"  ✅ {sector}: {len(df)} rows")
    except Exception as e: print(f"  ❌ {sector}: {e}")

def upload_summary(all_rows, ctx):
    print(f"\n📤 Summary → {SUMMARY_SHEET}")
    ws=gsheet(SUMMARY_SHEET)
    if not ws: return
    try:
        ws.clear()
        ts=datetime.now(WIB).strftime("%Y-%m-%d %H:%M WIB")
        ws.update("A1",[[
            f"🔍 IHSG Channel Breakout Screener v3.5 — {ts}","","",
            f"high>ta.highest(high,{LENGTH})[prev]→ChBrkLE | "
            f"low<ta.lowest(low,{LENGTH})[prev]→ChBrkSE | auto_adjust=False"]])

        crow=[["Commodity","Ticker","Close","MA","1W%","Status"]]
        for name,d in ctx.items():
            if d.get("close") is None:
                crow.append([name,d["ticker"],"-","-","-","❌"]); continue
            st=("✅ Weak(Bullish EM)" if (name=="DXY" and d["up"]) else
                "⚠️ Strong" if name=="DXY" else
                "✅ Bullish" if d["up"] else "⚠️ Bearish")
            crow.append([name,d["ticker"],d["close"],d["ma"],f"{d['chg']:+.2f}%",st])
        ws.update("A2",crow)

        data_row=len(crow)+4
        df=pd.DataFrame(all_rows)
        if df.empty: print("  ⚠️  No data"); return

        df["_le"]=(df.get("_type","")=="ChBrkLE").astype(int)
        df["_b"]=pd.to_numeric(df.get("_bars",999999),errors="coerce").fillna(999999)
        # Sort by: Long breakout first, shortest bars ago first, highest score TV
        df=df.sort_values(["_le","_b","Skor TV"],ascending=[False,True,False])
        df=df.drop(columns=["_le","_b","_type","_bars"],errors="ignore").reset_index(drop=True)

        avail=[c for c in DISPLAY_COLS if c in df.columns]
        set_with_dataframe(ws,df[avail],row=data_row,col=1,include_index=False)
        nc=len(avail)
        ws.format(f"A{data_row}:{chr(64+min(nc,26))}{data_row}",
                  {"textFormat":{"bold":True},
                   "backgroundColor":{"red":0.10,"green":0.35,"blue":0.22}})
        print(f"  ✅ {len(df)} rows")
    except Exception as e: print(f"  ❌ Summary: {e}")


# ── MAIN ───────────────────────────────────────────────────────────────────
def main():
    ts=datetime.now(WIB).strftime("%Y-%m-%d %H:%M WIB")
    print(f"\n{'═'*60}\n  IHSG Channel Breakout Screener v3.5   {ts}")
    print(f"  Length={LENGTH} | auto_adjust=False | {DOWNLOAD_DAYS}d history")
    print(f"{'═'*60}\n")

    ctx=fetch_commodities()
    all_rows=[]

    for sector,tickers in SECTORS.items():
        df_s=run_sector(sector,tickers,ctx)
        if df_s.empty: continue
        all_rows.extend(df_s.to_dict("records"))
        upload_sector(sector,df_s,ctx)
        time.sleep(2)

    if not all_rows: print("❌ No results"); sys.exit(1)
    upload_summary(all_rows,ctx)

    df_f=pd.DataFrame(all_rows)
    print(f"\n{'═'*60}")
    print(f"  Total   : {len(df_f)}")
    print(f"  ChBrkLE : {df_f['Channel Breakout'].str.contains('ChBrkLE',na=False).sum()}")
    print(f"  ChBrkSE : {df_f['Channel Breakout'].str.contains('ChBrkSE',na=False).sum()}")
    print(f"  Hari ini: {df_f['Channel Breakout'].str.contains('hari ini',na=False).sum()}")
    print(f"{'═'*60}\n")


if __name__ == "__main__":
    main()
