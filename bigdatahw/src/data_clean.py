import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

RAW = "stock_market.csv"
OUT = "cleaned.parquet"

def snake(s): return s.strip().lower().replace(" ", "_")

def main():
    df = pd.read_csv(RAW)

    df.columns = [snake(c) for c in df.columns]
    df = df.replace(["", "NA", "N/A", "null", "-", " "], pd.NA)

    # Fix date
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    df = df.drop_duplicates()

    # Save parquet
    table = pa.Table.from_pandas(df)
    pq.write_table(table, OUT)

if __name__ == "__main__":
    main()