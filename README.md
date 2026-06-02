# RT FINANCE API — ETL Pipeline

**Course:** FINANCE API
**Focus:** ETL Pipeline & Data Quality Engineering
**Database:** Supabase (PostgreSQL) via SQLAlchemy + psycopg2

---

## What This Project Does

A fully automated, five-stage ETL pipeline that:

1. **Extracts** IBM daily stock data from the Alpha Vantage REST API (falls back to a local CSV if the API is rate-limited)
2. **Cleans** the raw data — type casting, null removal, duplicate handling
3. **Transforms** the data by computing seven derived financial metrics
4. **Validates** data quality through hard and soft checks
5. **Loads** all results directly into a Supabase PostgreSQL table using an incremental upsert strategy

All output is stored in Supabase — no local CSV files are written.

---

## Project Structure

```
finance-etl-pipeline/
│
├── finance_etl_pipeline.py   # Main ETL script (the only file to submit)
├── daily_IBM.csv             # CSV fallback data (100 rows of IBM daily OHLCV)
│
├── requirements.txt          # Python dependencies
├── .env.example              # Environment variable template
├── .gitignore                # Files excluded from version control
├── README.md                 # This file
└── VALIDATION.md             # Data validation documentation
```

---

## Setup Instructions

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure environment variables

```bash
cp .env.example .env
```

Edit `.env` with your real values. Use the **Transaction pooler** connection parameters from your Supabase dashboard:

```env
ALPHA_VANTAGE_API_KEY=your_key_here

user=postgres.xxxxxxxxxxxxxxxxxxxx
password=your_supabase_password
host=aws-0-us-east-1.pooler.supabase.com
port=6543
dbname=postgres

STOCK_SYMBOL=IBM
CSV_FALLBACK_PATH=daily_IBM.csv
```

To find these values: Supabase Dashboard → **Settings → Database → Connection parameters** → switch toggle to **Transaction pooler**.

### 3. Place the CSV fallback file

Ensure `daily_IBM.csv` is in the same directory as the script. The pipeline uses it automatically if the Alpha Vantage API is unavailable.

### 4. Run the pipeline

```bash
python finance_etl_pipeline.py
```

A timestamped log is written to `logs/etl_YYYYMMDD_HHMMSS.log` on each run.

---

## Supabase Setup (One-Time)

1. Create a free project at [supabase.com](https://supabase.com)
2. Go to **Settings → Database → Connection parameters** and copy the Transaction pooler values into your `.env`
3. The pipeline creates the `public.stock_daily` table automatically on first run — no manual SQL required

---

## Pipeline Stages

| Stage | Description |
|---|---|
| 1. Extraction | Alpha Vantage REST API → CSV fallback if API unavailable |
| 2. Cleaning | Column normalisation, type casting, null drop, deduplication, sort |
| 3. Transformation | Computes daily_return, price_range, vwap_proxy, ma_7, ma_20, volatility_7, volume_zscore |
| 4. Validation | 6 hard checks (abort on failure) + 4 soft checks (warnings only) |
| 5. DB Loading | Supabase upsert via SQLAlchemy `INSERT … ON CONFLICT DO UPDATE` |

---

## Derived Metrics Reference

| Column | Formula | Purpose |
|---|---|---|
| `daily_return` | `(close / prev_close - 1) * 100` | Day-over-day % price change |
| `price_range` | `high - low` | Intraday volatility proxy |
| `vwap_proxy` | `(high + low + close) / 3` | Typical price approximation |
| `ma_7` | 7-day rolling mean of close | Short-term trend |
| `ma_20` | 20-day rolling mean of close | Medium-term trend |
| `volatility_7` | 7-day rolling std dev of daily_return | Short-term risk measure |
| `volume_zscore` | `(vol - rolling_mean) / rolling_std` | Volume anomaly detection |

---

## Supabase Table Schema

Table: `public.stock_daily` — created automatically on first run.

| Column | Type | Notes |
|---|---|---|
| `timestamp` | DATE | Primary key (composite) |
| `symbol` | VARCHAR(10) | Primary key (composite) |
| `open` | NUMERIC(12,4) | |
| `high` | NUMERIC(12,4) | |
| `low` | NUMERIC(12,4) | |
| `close` | NUMERIC(12,4) | |
| `volume` | BIGINT | |
| `daily_return` | NUMERIC(10,4) | Derived |
| `price_range` | NUMERIC(12,4) | Derived |
| `vwap_proxy` | NUMERIC(12,4) | Derived |
| `ma_7` | NUMERIC(12,4) | Derived |
| `ma_20` | NUMERIC(12,4) | Derived |
| `volatility_7` | NUMERIC(10,4) | Derived |
| `volume_zscore` | NUMERIC(10,4) | Derived |
| `loaded_at` | TIMESTAMP | Set at load time |

---

## Incremental Loading

The pipeline uses PostgreSQL's `INSERT … ON CONFLICT DO UPDATE` on the composite primary key `(timestamp, symbol)`:
- **New dates** are inserted
- **Existing dates** are updated with recalculated values
- **No duplicates** are ever created, regardless of how many times the pipeline runs
