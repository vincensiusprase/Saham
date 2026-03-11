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
            if df.empty: continue
                
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [c[0] for c in df.columns]
                
            df.index = pd.to_datetime(df.index).tz_localize(None)
            
            # Normalisasi kolom harga
            if 'Close' not in df.columns and 'Adj Close' in df.columns:
                df['Close'] = df['Adj Close']
            
            # --- TAMBAHAN: HITUNG PERCENTAGE CHANGE ---
            # Menggunakan .pct_change() * 100 untuk mendapatkan angka dalam persen
            df['Pct_Change'] = df['Close'].pct_change() * 100
            
            # Indikator lainnya (EMA & Ichimoku tetap sama)
            c = df['Close']
            h = df['High'] if 'High' in df.columns else c
            l = df['Low'] if 'Low' in df.columns else c
            
            df['EMA9'] = c.ewm(span=9, adjust=False).mean()
            df['EMA20'] = c.ewm(span=20, adjust=False).mean()
            df['EMA50'] = c.ewm(span=50, adjust=False).mean()
            df['EMA200'] = c.ewm(span=200, adjust=False).mean()
            
            # Metadata
            df['Asset_Name'] = name
            df['Category'] = info["category"]
            df['Date'] = df.index
            
            all_data.append(df)
            print(f"✅ Berhasil memproses: {name}")
            
        except Exception as e:
            print(f"❌ Error memproses {name} ({ticker}): {e}")

    if not all_data: return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    full_df = pd.concat(all_data, ignore_index=True)
    
    # ── SHEET 1: WIDE FORMAT (Harga & % Change) ──
    # Pivot untuk harga Close
    price_pivot = full_df.pivot(index='Date', columns='Asset_Name', values='Close')
    
    # Pivot untuk Percentage Change
    pct_pivot = full_df.pivot(index='Date', columns='Asset_Name', values='Pct_Change')
    # Memberikan suffix agar kolom tidak tertukar (Contoh: IHSG_Price dan IHSG_%Chg)
    pct_pivot.columns = [f"{col}_%Chg" for col in pct_pivot.columns]
    
    # Gabungkan keduanya
    sheet1_df = pd.concat([price_pivot, pct_pivot], axis=1).reset_index()
    sheet1_df = sheet1_df.sort_values('Date').ffill().round(2)
    sheet1_df['Date'] = sheet1_df['Date'].dt.strftime('%Y-%m-%d')

    # ── SHEET 2: LONG FORMAT (Ditambah Pct_Change untuk Dashboard) ──
    sheet2_cols = ['Date', 'Category', 'Asset_Name', 'Close', 'Pct_Change', 'EMA9', 'EMA20', 'EMA50', 'EMA200']
    sheet2_df = full_df[sheet2_cols].copy()
    sheet2_df['Date'] = sheet2_df['Date'].dt.strftime('%Y-%m-%d')
    sheet2_df = sheet2_df.round(2)

    # Sheet 3 tetap sama
    sheet3_df = pd.DataFrame(currency_ref_list)

    return sheet1_df, sheet2_df, sheet3_df
