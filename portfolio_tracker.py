import streamlit as st
import pandas as pd
import yfinance as yf

st.set_page_config(layout="wide")

# -------------------------------
# HELPERS
# -------------------------------
def load_csv(file):
    import pandas as pd

    for skip in range(5):  # try skipping first few rows
        try:
            df = pd.read_csv(file, skiprows=skip)
            if df.shape[1] > 2:
                return df
        except:
            continue

    # fallback
    return pd.read_csv(file, engine="python", on_bad_lines="skip")


def clean_columns(df):
    df.columns = [c.strip().lower() for c in df.columns]
    return df


def merge_common(h, g):
    h, g = clean_columns(h), clean_columns(g)
    return pd.merge(h, g, on="isin", how="inner")


def apply_symbol_map(df, sm):
    sm.columns = ["isin", "symbol"]
    return df.merge(sm, on="isin", how="left")


def apply_sector_map(df, sector_map):
    sector_map.columns = ["symbol", "sector"]
    return df.merge(sector_map, on="symbol", how="left")


# -------------------------------
# PRICE FETCH
# -------------------------------
@st.cache_data(ttl=60)
def fetch_prices(symbols):
    prices = {}

    if not symbols:
        return prices

    data = yf.download(
        tickers=" ".join(symbols),
        period="1d",
        interval="1d",
        group_by="ticker",
        threads=True
    )

    for s in symbols:
        try:
            if len(symbols) == 1:
                price = data["Close"].iloc[-1]
            else:
                price = data[s]["Close"].iloc[-1]
            prices[s] = float(price)
        except:
            continue

    return prices


# -------------------------------
# CALCULATIONS
# -------------------------------
def compute(df, prices):
    df["ltp"] = df["symbol"].map(prices)
    df["current_value"] = df["quantity"] * df["ltp"]

    if "invested" in df.columns:
        df["invested_value"] = df["invested"]
    else:
        df["invested_value"] = df["avg_price"] * df["quantity"]

    df["pnl"] = df["current_value"] - df["invested_value"]
    df["return_pct"] = (df["pnl"] / df["invested_value"]) * 100

    return df


# -------------------------------
# SECTOR ANALYSIS
# -------------------------------
def sector_allocation(df):
    sector_df = df.groupby("sector")["current_value"].sum().reset_index()
    total = sector_df["current_value"].sum()
    sector_df["weight"] = (sector_df["current_value"] / total) * 100
    return sector_df


# -------------------------------
# COMPARISON
# -------------------------------
def compare(df1, df2):
    merged = pd.merge(df1, df2, on="isin", how="outer", suffixes=("_d1", "_d2")).fillna(0)

    merged["qty_change"] = merged["quantity_d2"] - merged["quantity_d1"]
    merged["pnl_change"] = merged["pnl_d2"] - merged["pnl_d1"]

    def classify(row):
        if row["quantity_d1"] == 0 and row["quantity_d2"] > 0:
            return "NEW"
        elif row["quantity_d1"] > 0 and row["quantity_d2"] == 0:
            return "EXITED"
        elif row["qty_change"] > 0:
            return "INCREASED"
        elif row["qty_change"] < 0:
            return "REDUCED"
        else:
            return "UNCHANGED"

    merged["status"] = merged.apply(classify, axis=1)

    return merged


# -------------------------------
# UI
# -------------------------------
st.title("📊 Portfolio Intelligence System")

# Sidebar uploads
h1_file = st.sidebar.file_uploader("Day 1 Holdings")
g1_file = st.sidebar.file_uploader("Day 1 Gain/Loss")
h2_file = st.sidebar.file_uploader("Day 2 Holdings (optional)")
g2_file = st.sidebar.file_uploader("Day 2 Gain/Loss (optional)")
symbol_map_file = st.sidebar.file_uploader("Symbol Map")
sector_map_file = st.sidebar.file_uploader("Sector Map")

if h1_file and g1_file and symbol_map_file:

    h1 = load_csv(h1_file)
    g1 = load_csv(g1_file)
    sm = load_csv(symbol_map_file)

    df1 = merge_common(h1, g1)
    df1 = apply_symbol_map(df1, sm)

    if sector_map_file:
        sector_map = load_csv(sector_map_file)
        df1 = apply_sector_map(df1, sector_map)

    symbols = df1["symbol"].dropna().unique().tolist()
    prices = fetch_prices(symbols)

    df1 = compute(df1, prices)

    # -------------------------------
    # DASHBOARD
    # -------------------------------
    st.header("📈 Day 1 Portfolio")

    col1, col2 = st.columns(2)
    col1.metric("Total Value", f"₹{df1['current_value'].sum():,.0f}")
    col2.metric("Total P&L", f"₹{df1['pnl'].sum():,.0f}")

    st.dataframe(df1)

    # -------------------------------
    # SECTOR VIEW
    # -------------------------------
    if "sector" in df1.columns:
        st.subheader("📊 Sector Allocation")
        sector_df = sector_allocation(df1)
        st.dataframe(sector_df)
        st.bar_chart(sector_df.set_index("sector")["weight"])

    # -------------------------------
    # TOP MOVERS
    # -------------------------------
    st.subheader("🔥 Top Movers")

    gainers = df1.sort_values("pnl", ascending=False).head(5)
    losers = df1.sort_values("pnl").head(5)

    st.write("Top Gainers")
    st.dataframe(gainers)

    st.write("Top Losers")
    st.dataframe(losers)

    # -------------------------------
    # COMPARISON
    # -------------------------------
    if h2_file and g2_file:

        h2 = load_csv(h2_file)
        g2 = load_csv(g2_file)

        df2 = merge_common(h2, g2)
        df2 = apply_symbol_map(df2, sm)

        if sector_map_file:
            df2 = apply_sector_map(df2, sector_map)

        df2 = compute(df2, prices)

        st.header("📊 Day 2 Portfolio")
        st.dataframe(df2)

        comp = compare(df1, df2)

        st.header("🔄 Portfolio Changes")
        st.dataframe(comp)

else:
    st.info("Upload required files to begin.")