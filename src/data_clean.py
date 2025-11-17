import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

RAW = Path("raw_data/stock_market.csv")
OUT = Path("processed_data/cleaned.parquet")

def snake(s): 
    return s.strip().lower().replace(" ", "_")

def to_numeric(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = (
                df[c]
                .astype(str)
                .str.replace(",", "", regex=False)
                .str.strip()
                .replace(["", "NA", "N/A", "null", "-", " "], pd.NA)
            )
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def main():
    df = pd.read_csv(RAW)

    # snake_case column names
    df.columns = [snake(c) for c in df.columns]

    # Rename columns into a standard schema
    rename_map = {
        "close_price": "close",
        "closing_price": "close",
        "adj_close": "close",
        "symbol": "ticker",
        "stock": "ticker",
        "stock_symbol": "ticker",
        "trade_date": "date",
        "volume_traded": "volume",
        "sector_name": "sector",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # normalize nulls
    df = df.replace(["", "NA", "N/A", "null", "-", " "], pd.NA)

    # Fix date format
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    # Convert numeric columns safely
    numeric_cols = ["open", "high", "low", "close", "volume"]
    df = to_numeric(df, numeric_cols)

    # Remove corrupted extreme volume values
    if "volume" in df.columns:
        df = df[df["volume"].fillna(0) < 1e14]   # Remove insane values

    df = df.drop_duplicates()

    # Save parquet
    table = pa.Table.from_pandas(df)
    pq.write_table(table, OUT)

if __name__ == "__main__":
    main()
