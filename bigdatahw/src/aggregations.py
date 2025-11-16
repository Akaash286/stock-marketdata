import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

INP = "cleaned.parquet"

def main():
    df = pd.read_parquet(INP)

    if "close" in df.columns and "ticker" in df.columns and "date" in df.columns:
        agg1 = df.groupby(["date", "ticker"])["close"].mean().reset_index()
        pq.write_table(pa.Table.from_pandas(agg1), "agg1.parquet")

    if "volume" in df.columns and "sector" in df.columns:
        agg2 = df.groupby("sector")["volume"].mean().reset_index()
        pq.write_table(pa.Table.from_pandas(agg2), "agg2.parquet")

    if "close" in df.columns and "ticker" in df.columns and "date" in df.columns:
        df = df.sort_values(["ticker", "date"])
        df["prev_close"] = df.groupby("ticker")["close"].shift(1)
        df["return"] = (df["close"] - df["prev_close"]) / df["prev_close"]
        pq.write_table(pa.Table.from_pandas(df), "agg3.parquet")

if __name__ == "__main__":
    main()
