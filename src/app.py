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

# ----------------------------------------------------------
# Filters
# ----------------------------------------------------------

# Date range filter
if "date" in df.columns:
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    min_d, max_d = df["date"].min(), df["date"].max()

    date_range = st.date_input(
        "Date range",
        [min_d, max_d]
    )

    if len(date_range) == 2:
        start, end = date_range
        df = df[(df["date"] >= pd.to_datetime(start)) &
                (df["date"] <= pd.to_datetime(end))]

# Ticker filter
if "ticker" in df.columns:
    tickers = st.multiselect("Ticker", sorted(df["ticker"].dropna().unique()))
    if tickers:
        df = df[df["ticker"].isin(tickers)]

# Sector filter (agg2)
if "sector" in df.columns:
    sectors = st.multiselect("Sector", sorted(df["sector"].dropna().unique()))
    if sectors:
        df = df[df["sector"].isin(sectors)]

# ----------------------------------------------------------
# Chart (auto chooses line or bar)
# ----------------------------------------------------------

st.subheader("Chart")

numeric_df = df.select_dtypes(include="number")

if numeric_df.empty:
    st.info("No numeric columns available for charting.")
else:
    y_col = st.selectbox("Choose numeric column to chart", numeric_df.columns)

    # If there is a date column → time series line chart
    if "date" in df.columns:
        chart_data = df.sort_values("date").set_index("date")[y_col]
        st.line_chart(chart_data)

    else:
        # No date column → use bar chart
        st.bar_chart(df[y_col])



