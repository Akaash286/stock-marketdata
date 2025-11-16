import streamlit as st
import pandas as pd

st.title("Stock Aggregates Dashboard")

choice = st.selectbox("Choose aggregate", ["agg1", "agg2", "agg3"])
path = choice + ".parquet"
df = pd.read_parquet(path)

st.write("Data preview:", df.head())

if "date" in df.columns:
    dates = st.date_input("Date range", [])
    # skipping filtering logic for brevity

if "ticker" in df.columns:
    tickers = st.multiselect("Ticker", sorted(df["ticker"].dropna().unique()))
    if tickers:
        df = df[df["ticker"].isin(tickers)]

st.line_chart(df.select_dtypes(include="number"))
