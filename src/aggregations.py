import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

INP = Path("processed_data/cleaned.parquet")

def main():
    df = pd.read_parquet(INP)

    # 1. Daily avg close by ticker
    if {"close", "ticker", "date"}.issubset(df.columns):
        agg1 = df.groupby(["date", "ticker"])["close"].mean().reset_index()
        pq.write_table(pa.Table.from_pandas(agg1), "processed_data/agg1.parquet")

    # 2. Avg volume by sector
     if {"volume", "sector", "ticker"}.issubset(df.columns):
        agg2 = df.groupby(["sector", "ticker"])["volume"].mean().reset_index()
        pq.write_table(pa.Table.from_pandas(agg2), "processed_data/agg2.parquet")

    # 3. Simple daily return
    if {"close", "ticker", "date"}.issubset(df.columns):
        df = df.sort_values(["ticker", "date"])
        df["prev_close"] = df.groupby("ticker")["close"].shift(1)
        df["return"] = (df["close"] - df["prev_close"]) / df["prev_close"]
        pq.write_table(pa.Table.from_pandas(df), "processed_data/agg3.parquet")

if __name__ == "__main__":
    main()

