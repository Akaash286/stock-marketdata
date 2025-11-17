import streamlit as st
import pandas as pd
from pathlib import Path

st.title("Stock Aggregates Dashboard")

# Folder where parquets live
DATA_DIR = Path("processed_data")

choice = st.selectbox("Choose aggregate", ["agg1", "agg2", "agg3"])

# Build the correct path
path = DATA_DIR / f"{choice}.parquet"

st.write("Loading:", path)

# Load parquet
df = pd.read_parquet(path)

st.write("Data preview:", df.head())

# Optional filters
if "date" in df.columns:
    dates = st.date_input("Date range", [])

if "ticker" in df.columns:
    tickers = st.multiselect("Ticker", sorted(df["ticker"].dropna().unique()))
    if tickers:
        df = df[df["ticker"].isin(tickers)]

# Chart
st.line_chart(df.select_dtypes(include="number"))
