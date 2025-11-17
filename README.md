# 📊 Stock Market Data Pipeline & Streamlit Dashboard

This project performs **data cleaning**, **data transformations**, **Parquet aggregation**, and a **Streamlit dashboard** for visualizing stock market metrics.

The workflow includes:

1. Loading raw CSV stock data  
2. Cleaning and normalizing the schema  
3. Generating multiple aggregated Parquet files  
4. Viewing & filtering data via a Streamlit application  
5. Using **UV**, **VSCode**, and a structured Python project layout  

---

# ⚙️ 1. Setup (Using UV)

Install dependencies:

uv sync

# 🧼 2. Data Cleaning


data_clean.py loads the raw CSV, normalizes schema, fixes dates, converts numeric columns, removes corrupted values, and writes:

processed_data/cleaned.parquet


Run it:

uv run python src/data_clean.py

# 📦 3. Generate Aggregations

aggregations.py loads the cleaned parquet and generates:

✔ agg1.parquet – daily average close price per ticker

✔ agg2.parquet – average volume grouped by sector

✔ agg3.parquet – daily returns per ticker

Run it:

uv run python src/aggregations.py

# 📊 4. Streamlit Dashboard

The app loads any of the aggregation files and supports:

-Ticker filtering

-Sector filtering (agg2)

-Date range filtering

-Line charts (when date available)

-Bar charts (when date unavailable)

-Data previews

Run the dashboard:

uv run streamlit run src/app.py


## Features

### Data Cleaning
- Schema normalization (snake_case headers)
- Whitespace trimming and text standardization
- Missing value handling ("NA", "N/A", "null", "-" → standardized null)
- Date format standardization (yyyy-MM-dd)
- Deduplication and type enforcement

### Aggregations
- Daily average closing price by ticker
- Average volume by sector
- Simple daily returns by ticker
- Multiple aggregation levels for comprehensive analysis

### Interactive Dashboard
- Date range filtering
- Ticker selection
- Multiple chart types (line, bar, area)
- Real-time data exploration

## 🛠 Technologies Used

-Python 3.10+

-Pandas — data processing

-PyArrow — Parquet file support

-Streamlit — dashboard UI

-UV — package/dependency management

-VSCode — development environment


