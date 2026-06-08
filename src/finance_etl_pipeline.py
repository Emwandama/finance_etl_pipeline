"""
================================================================================
RT FINANCE API — Real-Time Financial Market Analytics ETL Pipeline
================================================================================
Course    : FINANCE API
Focus     : ETL Pipeline & Data Quality Engineering
Data      : Alpha Vantage TIME_SERIES_DAILY (IBM) via REST API + local CSV fallback
Database  : Supabase (PostgreSQL) via SQLAlchemy + psycopg2
Analytics : Power BI-ready finalized dataset exported as CSV

Pipeline Stages
---------------
1. Extraction     — Alpha Vantage REST API  (falls back to local CSV if rate-limited)
2. Cleaning       — Type casting, null handling, duplicate removal
3. Transformation — Derived metrics (daily_return, price_range, vwap_proxy, ma_7,
                    ma_20, volatility_7, volume_zscore)
4. Validation     — Schema checks, range guards, business rule assertions
5. Loading        — Incremental upsert into Supabase `stock_daily` table
                    (all output is stored in Supabase — no local CSV written)

Usage
-----
  pip install -r requirements.txt
  Copy .env.example to .env and fill in your Supabase credentials, then run:
  python finance_etl_pipeline.py

Supabase Connection (.env)
--------------------------
  ALPHA_VANTAGE_API_KEY=your_key_here

  # Supabase — find these in your Supabase project under
  # Settings > Database > Connection string (URI mode)
  SUPABASE_DB_HOST=db.<project-ref>.supabase.co
  SUPABASE_DB_PORT=5432
  SUPABASE_DB_NAME=postgres
  SUPABASE_DB_USER=postgres
  SUPABASE_DB_PASSWORD=your_supabase_db_password

  # Optional overrides
  STOCK_SYMBOL=IBM
  CSV_FALLBACK_PATH=daily_IBM.csv
================================================================================
"""

# ── Standard library ──────────────────────────────────────────────────────────
import os
import sys
import logging
import warnings
from datetime import datetime
from pathlib import Path

# ── Third-party ───────────────────────────────────────────────────────────────
try:
    import requests
    import pandas as pd
    import numpy as np
    from dotenv import load_dotenv
    from sqlalchemy import (
        create_engine, text, MetaData, Table, Column,
        Date, Numeric, BigInteger, String, DateTime, UniqueConstraint,
    )
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    from sqlalchemy.exc import SQLAlchemyError
except ImportError as exc:
    print(
        f"\n[SETUP ERROR] Missing dependency: {exc}\n"
        "Install with:  pip install -r requirements.txt\n"
    )
    sys.exit(1)

warnings.filterwarnings("ignore", category=FutureWarning)


# ==============================================================================
# 0.  CONFIGURATION & LOGGING
# ==============================================================================

load_dotenv()

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_FORMAT = "%(asctime)s  [%(levelname)-8s]  %(message)s"
LOG_DATE   = "%Y-%m-%d %H:%M:%S"

Path("logs").mkdir(exist_ok=True)
log_file = Path("logs") / f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    datefmt=LOG_DATE,
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("finance_etl")

# ── Pipeline parameters ───────────────────────────────────────────────────────
API_KEY      = os.getenv("ALPHA_VANTAGE_API_KEY", "demo")
SYMBOL       = os.getenv("STOCK_SYMBOL", "IBM")
CSV_FALLBACK = os.getenv("CSV_FALLBACK_PATH", "daily_IBM.csv")

# ── Supabase / PostgreSQL connection ─────────────────────────────────────────
# Credentials are found in your Supabase dashboard:
#   Project Settings → Database → Connection parameters
# Use these exact variable names in your .env file.
USER   = os.getenv("user")
PASS   = os.getenv("password")
HOST   = os.getenv("host")
PORT   = os.getenv("port")
DBNAME = os.getenv("dbname")

# sslmode=require is appended in the URL — Supabase mandates SSL.
DATABASE_URL = (
    f"postgresql+psycopg2://{USER}:{PASS}@{HOST}:{PORT}/{DBNAME}?sslmode=require"
)


# ==============================================================================
# 1.  EXTRACTION
# ==============================================================================

def extract_from_api(symbol: str, api_key: str) -> "pd.DataFrame | None":
    """
    Pull TIME_SERIES_DAILY (full output size) from the Alpha Vantage REST API.

    Parameters
    ----------
    symbol  : Ticker symbol, e.g. 'IBM'
    api_key : Alpha Vantage API key

    Returns
    -------
    Raw DataFrame with columns [timestamp, open, high, low, close, volume],
    or None if the request fails or the API returns no time-series data.
    """
    url = (
        "https://www.alphavantage.co/query"
        f"?function=TIME_SERIES_DAILY"
        f"&symbol={symbol}"
        f"&outputsize=full"
        f"&apikey={api_key}"
    )
    logger.info("EXTRACT | Calling Alpha Vantage API  symbol=%s", symbol)

    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        # Alpha Vantage signals rate-limit / key errors via Note / Information keys
        if "Time Series (Daily)" not in data:
            note = data.get("Note") or data.get("Information") or "Unknown API error"
            logger.warning("EXTRACT | API returned no time series. Reason: %s", note)
            return None

        records = [
            {
                "timestamp": date_str,
                "open":   values.get("1. open"),
                "high":   values.get("2. high"),
                "low":    values.get("3. low"),
                "close":  values.get("4. close"),
                "volume": values.get("5. volume"),
            }
            for date_str, values in data["Time Series (Daily)"].items()
        ]

        df = pd.DataFrame(records)
        logger.info("EXTRACT | API returned %d raw rows for %s", len(df), symbol)
        return df

    except requests.RequestException as exc:
        logger.warning("EXTRACT | HTTP request failed: %s", exc)
        return None


def extract_from_csv(path: str) -> "pd.DataFrame | None":
    """
    Load a local CSV file as a raw DataFrame fallback.

    Expected columns: timestamp, open, high, low, close, volume
    (matches the Alpha Vantage daily export format).
    """
    csv_path = Path(path)
    if not csv_path.exists():
        logger.error("EXTRACT | CSV fallback not found: %s", csv_path.resolve())
        return None

    logger.info("EXTRACT | Loading CSV fallback from %s", csv_path.resolve())
    df = pd.read_csv(csv_path)
    logger.info("EXTRACT | CSV returned %d raw rows", len(df))
    return df


def extract(symbol: str, api_key: str, csv_fallback: str) -> pd.DataFrame:
    """
    Orchestrate extraction: try Alpha Vantage API first, fall back to CSV.

    Raises RuntimeError if both sources fail.
    Adds provenance columns: _source, _extracted_at.
    """
    logger.info("=" * 60)
    logger.info("STAGE 1 — EXTRACTION")
    logger.info("=" * 60)

    df = extract_from_api(symbol, api_key)
    source = "API"

    if df is None or df.empty:
        logger.info("EXTRACT | Falling back to local CSV ...")
        df = extract_from_csv(csv_fallback)
        source = "CSV"

    if df is None or df.empty:
        raise RuntimeError(
            "Extraction failed: both API and CSV sources returned no data."
        )

    # Provenance metadata — useful for audit and incremental tracking
    df["_source"]       = source
    df["_extracted_at"] = datetime.now()

    logger.info(
        "EXTRACT | Complete — source=%s  rows=%d", source, len(df)
    )
    return df


# ==============================================================================
# 2.  CLEANING & NORMALISATION
# ==============================================================================

PRICE_COLS = ["open", "high", "low", "close"]
VOLUME_COL = "volume"
DATE_COL   = "timestamp"


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply cleaning and normalisation to the raw extracted DataFrame.

    Steps
    -----
    1. Normalise column names (strip whitespace, lower-case)
    2. Rename verbose Alpha Vantage field names if present
    3. Assert all required columns exist
    4. Parse timestamp to datetime
    5. Cast price columns to float; volume to Int64
    6. Drop rows with null OHLCV values
    7. Drop duplicate dates (keep last)
    8. Sort chronologically and reset index
    """
    logger.info("=" * 60)
    logger.info("STAGE 2 — CLEANING & NORMALISATION")
    logger.info("=" * 60)

    df = df.copy()
    original_count = len(df)

    # Step 1 — Normalise column names
    df.columns = [c.strip().lower() for c in df.columns]

    # Step 2 — Rename Alpha Vantage verbose names when present (API source)
    av_rename = {
        "1. open":   "open",
        "2. high":   "high",
        "3. low":    "low",
        "4. close":  "close",
        "5. volume": "volume",
    }
    df.rename(columns=av_rename, inplace=True)

    # Step 3 — Required column check
    required = {DATE_COL, "open", "high", "low", "close", "volume"}
    missing  = required - set(df.columns)
    if missing:
        raise ValueError(f"CLEAN | Missing required columns: {missing}")

    # Step 4 — Parse timestamp
    df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce")
    bad_dates = df[DATE_COL].isna().sum()
    if bad_dates:
        logger.warning("CLEAN | Dropping %d rows with unparseable dates", bad_dates)
        df.dropna(subset=[DATE_COL], inplace=True)

    # Step 5 — Cast numeric types
    for col in PRICE_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df[VOLUME_COL] = pd.to_numeric(df[VOLUME_COL], errors="coerce").astype("Int64")

    # Step 6 — Drop null OHLCV rows
    null_rows = df[PRICE_COLS + [VOLUME_COL]].isnull().any(axis=1).sum()
    if null_rows:
        logger.warning("CLEAN | Dropping %d rows with null OHLCV values", null_rows)
        df.dropna(subset=PRICE_COLS + [VOLUME_COL], inplace=True)

    # Step 7 — Remove duplicate dates
    dupes = df.duplicated(subset=[DATE_COL]).sum()
    if dupes:
        logger.warning("CLEAN | Dropping %d duplicate date rows", dupes)
        df.drop_duplicates(subset=[DATE_COL], keep="last", inplace=True)

    # Step 8 — Sort chronologically
    df.sort_values(DATE_COL, inplace=True)
    df.reset_index(drop=True, inplace=True)

    logger.info(
        "CLEAN | Rows in=%d  rows out=%d  dropped=%d",
        original_count, len(df), original_count - len(df),
    )
    return df


# ==============================================================================
# 3.  TRANSFORMATION & DERIVED METRICS
# ==============================================================================

def transform(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """
    Compute derived financial metrics and attach the ticker symbol.

    Derived columns
    ---------------
    daily_return  : % change in daily close price (day-over-day)
    price_range   : High − Low  (intraday spread)
    vwap_proxy    : (High + Low + Close) / 3  — typical price approximation
    ma_7          : 7-day simple moving average on close
    ma_20         : 20-day simple moving average on close
    volatility_7  : 7-day rolling standard deviation of daily_return
    volume_zscore : z-score of volume vs. 20-day rolling window
    symbol        : ticker identifier (uppercase)

    All derived numerics are rounded to 4 decimal places for storage efficiency.
    """
    logger.info("=" * 60)
    logger.info("STAGE 3 — TRANSFORMATION & DERIVED METRICS")
    logger.info("=" * 60)

    df = df.copy()

    # Daily percentage return
    df["daily_return"] = df["close"].pct_change() * 100

    # Intraday spread
    df["price_range"]  = df["high"] - df["low"]

    # VWAP proxy (typical price — no tick data available)
    df["vwap_proxy"]   = (df["high"] + df["low"] + df["close"]) / 3

    # Simple moving averages
    df["ma_7"]  = df["close"].rolling(window=7,  min_periods=1).mean()
    df["ma_20"] = df["close"].rolling(window=20, min_periods=1).mean()

    # Rolling volatility (std of daily returns over 7 trading days)
    df["volatility_7"] = (
        df["daily_return"].rolling(window=7, min_periods=2).std()
    )

    # Volume z-score: how unusual is today's volume vs. trailing 20-day window?
    vol_mean = df["volume"].rolling(window=20, min_periods=1).mean()
    vol_std  = df["volume"].rolling(window=20, min_periods=2).std()
    df["volume_zscore"] = (
        (df["volume"] - vol_mean) / vol_std.replace(0, np.nan)
    )

    # Ticker identifier
    df["symbol"] = symbol.upper()

    # Round all derived numeric columns
    round_cols = [
        "daily_return", "price_range", "vwap_proxy",
        "ma_7", "ma_20", "volatility_7", "volume_zscore",
    ]
    df[round_cols] = df[round_cols].round(4)

    logger.info("TRANSFORM | Derived metrics added: %s", round_cols)
    logger.info("TRANSFORM | Total columns: %d", len(df.columns))
    return df


# ==============================================================================
# 4.  DATA VALIDATION & QUALITY CHECKS
# ==============================================================================

class DataQualityError(Exception):
    """Raised when a critical (hard) data quality assertion fails."""


def validate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Run a suite of data quality checks against the transformed DataFrame.

    Hard checks  — raise DataQualityError, aborting the pipeline:
      1. No nulls in OHLCV columns
      2. High >= Low for every row
      3. Close is within [Low, High] for every row
      4. All prices are positive (> 0)
      5. Volume is positive (> 0)
      6. Row count is non-zero

    Soft checks  — logged as warnings, pipeline continues:
      7.  No future-dated rows
      8.  No extreme daily returns (|return| > 50%) — likely data error
      9.  Volume outlier flag (|z-score| > 3) — informational
      10. No date gaps larger than 10 calendar days
    """
    logger.info("=" * 60)
    logger.info("STAGE 4 — DATA VALIDATION & QUALITY CHECKS")
    logger.info("=" * 60)

    errors        = []
    warnings_seen = 0

    # ── Hard check 1: Nulls in OHLCV ─────────────────────────────────────────
    null_counts = df[PRICE_COLS + [VOLUME_COL]].isnull().sum()
    if null_counts.any():
        errors.append(
            f"Null values in OHLCV columns: "
            f"{null_counts[null_counts > 0].to_dict()}"
        )

    # ── Hard check 2: High >= Low ─────────────────────────────────────────────
    bad_hl = df[df["high"] < df["low"]]
    if not bad_hl.empty:
        errors.append(
            f"high < low on {len(bad_hl)} row(s): "
            f"{bad_hl[DATE_COL].dt.date.tolist()}"
        )

    # ── Hard check 3: Close within [Low, High] ────────────────────────────────
    bad_close = df[(df["close"] > df["high"]) | (df["close"] < df["low"])]
    if not bad_close.empty:
        errors.append(
            f"close outside [low, high] on {len(bad_close)} row(s): "
            f"{bad_close[DATE_COL].dt.date.tolist()}"
        )

    # ── Hard check 4: Prices positive ────────────────────────────────────────
    neg_prices = df[(df[PRICE_COLS] <= 0).any(axis=1)]
    if not neg_prices.empty:
        errors.append(f"Non-positive price(s) on {len(neg_prices)} row(s)")

    # ── Hard check 5: Volume positive ────────────────────────────────────────
    zero_vol = df[df[VOLUME_COL] <= 0]
    if not zero_vol.empty:
        errors.append(f"Non-positive volume on {len(zero_vol)} row(s)")

    # ── Hard check 6: Non-empty dataset ──────────────────────────────────────
    if len(df) == 0:
        errors.append("DataFrame is empty — nothing to load")

    # Raise on any hard failure
    if errors:
        msg = "DATA QUALITY HARD FAILURES:\n" + "\n".join(
            f"  * {e}" for e in errors
        )
        logger.error(msg)
        raise DataQualityError(msg)

    logger.info("VALIDATE | All hard quality checks PASSED (%d checks)", 6)

    # ── Soft check 7: Future dates ────────────────────────────────────────────
    future = df[df[DATE_COL] > pd.Timestamp.now().normalize()]
    if not future.empty:
        logger.warning(
            "VALIDATE | %d row(s) have future-dated timestamps", len(future)
        )
        warnings_seen += 1

    # ── Soft check 8: Extreme daily returns ──────────────────────────────────
    extreme = df[df["daily_return"].abs() > 50].dropna(subset=["daily_return"])
    if not extreme.empty:
        logger.warning(
            "VALIDATE | %d row(s) with |daily_return| > 50%% — possible bad data",
            len(extreme),
        )
        warnings_seen += 1

    # ── Soft check 9: Volume z-score outliers (informational) ─────────────────
    vol_out = df[df["volume_zscore"].abs() > 3].dropna(subset=["volume_zscore"])
    if not vol_out.empty:
        logger.info(
            "VALIDATE | %d row(s) flagged as volume outliers (|z| > 3)",
            len(vol_out),
        )

    # ── Soft check 10: Large date gaps ───────────────────────────────────────
    gaps = df[DATE_COL].sort_values().diff().dropna()
    large_gaps = gaps[gaps > pd.Timedelta(days=10)]
    if not large_gaps.empty:
        logger.warning(
            "VALIDATE | %d date gap(s) exceeding 10 calendar days detected",
            len(large_gaps),
        )
        warnings_seen += 1

    # ── Dataset summary ───────────────────────────────────────────────────────
    logger.info(
        "VALIDATE | Summary — rows: %d | date range: %s to %s | "
        "avg close: %.2f | avg volume: %s",
        len(df),
        df[DATE_COL].min().date(),
        df[DATE_COL].max().date(),
        df["close"].mean(),
        f"{df['volume'].mean():,.0f}",
    )
    logger.info("VALIDATE | Soft warnings raised: %d", warnings_seen)
    return df


# ==============================================================================
# 5.  DATABASE LOADING — Supabase (PostgreSQL) via SQLAlchemy
# ==============================================================================
# Schema: 4 related tables matching the schema visualizer diagram
#
#   symbols             — master ref   (one row per ticker)
#   api_ingestion_runs  — audit log    (one row per pipeline run)
#   daily_prices        — fact table   (raw OHLCV, one row per symbol+date)
#   daily_price_metrics — derived      (computed metrics, one row per symbol+date)
#
# Incremental strategy: INSERT … ON CONFLICT DO UPDATE (upsert) on
# (symbol_id, trade_date) for daily_prices and daily_price_metrics.
# Re-running the pipeline never creates duplicate rows.
# ==============================================================================

SCHEMA_NAME = "public"


def get_engine(database_url: str):
    """
    Build a SQLAlchemy engine pointed at Supabase and verify connectivity.

    Raises RuntimeError if the connection cannot be established.
    All pipeline output is stored in Supabase — a working connection
    is required for the pipeline to complete.

    sslmode=require is embedded in DATABASE_URL; no extra connect_args needed.
    """
    logger.info("DB | Connecting to Supabase PostgreSQL ...")

    if not HOST:
        raise RuntimeError(
            "'host' is not set in .env. "
            "Add your Supabase connection parameters and re-run the pipeline."
        )

    engine = create_engine(database_url, echo=False, pool_pre_ping=True)

    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("DB | Supabase connection successful")
        return engine
    except SQLAlchemyError as exc:
        raise RuntimeError(
            f"Could not connect to Supabase: {exc}"
        ) from exc


def ensure_tables(engine, metadata: MetaData) -> dict:
    """
    Declare all four tables and create them in Supabase if they do not exist.

    Returns a dict of {table_name: Table} for use in the load functions.
    Tables are created in dependency order:
      symbols → api_ingestion_runs → daily_prices → daily_price_metrics
    """
    # ── Table 1: symbols (master ref) ────────────────────────────────────────
    symbols = Table(
        "symbols", metadata,
        Column("symbol_id",    BigInteger, primary_key=True, autoincrement=True),
        Column("ticker",       String(10), nullable=False, unique=True),
        Column("company_name", String(255)),
        Column("exchange",     String(50)),
        Column("currency",     String(3)),
        Column("created_at",   DateTime),
        schema=SCHEMA_NAME,
        extend_existing=True,
    )

    # ── Table 2: api_ingestion_runs (audit) ───────────────────────────────────
    ingestion_runs = Table(
        "api_ingestion_runs", metadata,
        Column("run_id",           BigInteger, primary_key=True, autoincrement=True),
        Column("symbol_id",        BigInteger, nullable=False),
        Column("fetched_at",       DateTime),
        Column("records_inserted", BigInteger),
        Column("source_url",       String(500)),
        Column("status",           String(20)),
        Column("notes",            String(1000)),
        schema=SCHEMA_NAME,
        extend_existing=True,
    )

    # ── Table 3: daily_prices (fact table) ───────────────────────────────────
    daily_prices = Table(
        "daily_prices", metadata,
        Column("price_id",   BigInteger, primary_key=True, autoincrement=True),
        Column("symbol_id",  BigInteger, nullable=False),
        Column("run_id",     BigInteger, nullable=False),
        Column("trade_date", Date,       nullable=False),
        Column("open_price", Numeric(12, 4)),
        Column("high_price", Numeric(12, 4)),
        Column("low_price",  Numeric(12, 4)),
        Column("close_price",Numeric(12, 4)),
        Column("volume",     BigInteger),
        Column("ingested_at",DateTime),
        UniqueConstraint("symbol_id", "trade_date", name="uq_daily_prices_symbol_date"),
        schema=SCHEMA_NAME,
        extend_existing=True,
    )

    # ── Table 4: daily_price_metrics (derived) ────────────────────────────────
    daily_price_metrics = Table(
        "daily_price_metrics", metadata,
        Column("metric_id",        BigInteger, primary_key=True, autoincrement=True),
        Column("symbol_id",        BigInteger, nullable=False),
        Column("trade_date",       Date,       nullable=False),
        Column("daily_return_pct", Numeric(8,  4)),
        Column("price_range",      Numeric(12, 4)),
        Column("range_pct",        Numeric(8,  4)),
        Column("vwap_approx",      Numeric(12, 4)),
        Column("ma_7",             Numeric(12, 4)),
        Column("sma_20d",          Numeric(12, 4)),
        Column("volatility_7",     Numeric(10, 4)),
        Column("volume_zscore",    Numeric(10, 4)),
        Column("computed_at",      DateTime),
        UniqueConstraint("symbol_id", "trade_date", name="uq_daily_price_metrics_symbol_date"),
        schema=SCHEMA_NAME,
        extend_existing=True,
    )

    metadata.create_all(engine)
    logger.info("DB | All 4 tables ready: symbols, api_ingestion_runs, daily_prices, daily_price_metrics")

    return {
        "symbols":             symbols,
        "api_ingestion_runs":  ingestion_runs,
        "daily_prices":        daily_prices,
        "daily_price_metrics": daily_price_metrics,
    }


def upsert_symbol(engine, tables: dict, symbol: str) -> int:
    """
    Insert the ticker into the symbols table if it does not already exist.
    Returns the symbol_id for use in downstream inserts.
    """
    sym_table = tables["symbols"]

    with engine.begin() as conn:
        # Try to insert; if ticker already exists, do nothing and return existing id
        stmt = pg_insert(sym_table).values(
            ticker    = symbol.upper(),
            created_at= datetime.now(),
        ).on_conflict_do_nothing(index_elements=["ticker"])
        conn.execute(stmt)

        # Fetch the symbol_id (whether just inserted or pre-existing)
        row = conn.execute(
            text("SELECT symbol_id FROM public.symbols WHERE ticker = :t"),
            {"t": symbol.upper()}
        ).fetchone()

    symbol_id = row[0]
    logger.info("DB | symbols — symbol_id=%d for ticker=%s", symbol_id, symbol.upper())
    return symbol_id


def log_ingestion_run(engine, tables: dict, symbol_id: int,
                      source_url: str, status: str,
                      records_inserted: int = 0, notes: str = "") -> int:
    """
    Insert one row into api_ingestion_runs to audit this pipeline execution.
    Returns the run_id for linking daily_prices rows.
    """
    run_table = tables["api_ingestion_runs"]

    with engine.begin() as conn:
        result = conn.execute(
            pg_insert(run_table).values(
                symbol_id        = symbol_id,
                fetched_at       = datetime.now(),
                records_inserted = records_inserted,
                source_url       = source_url,
                status           = status,
                notes            = notes or None,
            ).returning(run_table.c.run_id)
        )
        run_id = result.fetchone()[0]

    logger.info("DB | api_ingestion_runs — run_id=%d  status=%s", run_id, status)
    return run_id


def load_daily_prices(df: pd.DataFrame, engine, tables: dict,
                      symbol_id: int, run_id: int) -> int:
    """
    Upsert raw OHLCV data into daily_prices.

    Conflict target: (symbol_id, trade_date)
    On conflict: update price columns and ingested_at timestamp.
    """
    tbl = tables["daily_prices"]

    records = [
        {
            "symbol_id":  symbol_id,
            "run_id":     run_id,
            "trade_date": row["timestamp"].date(),
            "open_price": row["open"],
            "high_price": row["high"],
            "low_price":  row["low"],
            "close_price":row["close"],
            "volume":     int(row["volume"]) if pd.notnull(row["volume"]) else None,
            "ingested_at":datetime.now(),
        }
        for _, row in df.iterrows()
    ]

    upserted = 0
    with engine.begin() as conn:
        for start in range(0, len(records), 500):
            batch = records[start: start + 500]
            stmt  = pg_insert(tbl).values(batch)
            stmt  = stmt.on_conflict_do_update(
                index_elements=["symbol_id", "trade_date"],
                set_={
                    "open_price":  stmt.excluded.open_price,
                    "high_price":  stmt.excluded.high_price,
                    "low_price":   stmt.excluded.low_price,
                    "close_price": stmt.excluded.close_price,
                    "volume":      stmt.excluded.volume,
                    "ingested_at": stmt.excluded.ingested_at,
                },
            )
            conn.execute(stmt)
            upserted += len(batch)
            logger.info("DB | daily_prices upserted rows %d–%d", start + 1, start + len(batch))

    logger.info("DB | daily_prices — total rows upserted: %d", upserted)
    return upserted


def load_daily_metrics(df: pd.DataFrame, engine, tables: dict, symbol_id: int) -> int:
    """
    Upsert derived financial metrics into daily_price_metrics.

    Conflict target: (symbol_id, trade_date)
    On conflict: update all metric columns and computed_at timestamp.
    """
    tbl = tables["daily_price_metrics"]

    def _safe(val):
        return None if pd.isnull(val) else float(val)

    records = [
        {
            "symbol_id":       symbol_id,
            "trade_date":      row["timestamp"].date(),
            "daily_return_pct":_safe(row["daily_return"]),
            "price_range":     _safe(row["price_range"]),
            "range_pct":       _safe(row["price_range"] / row["close"] * 100)
                               if pd.notnull(row["close"]) and row["close"] != 0 else None,
            "vwap_approx":     _safe(row["vwap_proxy"]),
            "ma_7":            _safe(row["ma_7"]),
            "sma_20d":         _safe(row["ma_20"]),
            "volatility_7":    _safe(row["volatility_7"]),
            "volume_zscore":   _safe(row["volume_zscore"]),
            "computed_at":     datetime.now(),
        }
        for _, row in df.iterrows()
    ]

    upserted = 0
    with engine.begin() as conn:
        for start in range(0, len(records), 500):
            batch = records[start: start + 500]
            stmt  = pg_insert(tbl).values(batch)
            stmt  = stmt.on_conflict_do_update(
                index_elements=["symbol_id", "trade_date"],
                set_={
                    "daily_return_pct": stmt.excluded.daily_return_pct,
                    "price_range":      stmt.excluded.price_range,
                    "range_pct":        stmt.excluded.range_pct,
                    "vwap_approx":      stmt.excluded.vwap_approx,
                    "ma_7":             stmt.excluded.ma_7,
                    "sma_20d":          stmt.excluded.sma_20d,
                    "volatility_7":     stmt.excluded.volatility_7,
                    "volume_zscore":    stmt.excluded.volume_zscore,
                    "computed_at":      stmt.excluded.computed_at,
                },
            )
            conn.execute(stmt)
            upserted += len(batch)
            logger.info("DB | daily_price_metrics upserted rows %d–%d", start + 1, start + len(batch))

    logger.info("DB | daily_price_metrics — total rows upserted: %d", upserted)
    return upserted


# ==============================================================================
# MAIN ORCHESTRATOR
# ==============================================================================

def run_pipeline() -> None:
    """
    Execute all five ETL stages end-to-end.

    Stage 5 loads data into 4 Supabase tables:
      symbols, api_ingestion_runs, daily_prices, daily_price_metrics

    The pipeline is idempotent — upserts on (symbol_id, trade_date) mean
    re-runs never create duplicate rows. DataQualityError in Stage 4 aborts
    before any DB write.
    """
    start_time = datetime.now()
    source_url = (
        f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY"
        f"&symbol={SYMBOL}&outputsize=full"
    )

    logger.info("=" * 60)
    logger.info("RT FINANCE API  —  ETL PIPELINE START")
    logger.info("Symbol : %s", SYMBOL)
    logger.info("Started: %s", start_time.strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("=" * 60)

    # Stage 1 — Extract
    raw_df = extract(SYMBOL, API_KEY, CSV_FALLBACK)

    # Stage 2 — Clean
    clean_df = clean(raw_df)

    # Stage 3 — Transform
    transformed_df = transform(clean_df, SYMBOL)

    # Stage 4 — Validate (aborts on hard failures before any DB write)
    validated_df = validate(transformed_df)

    # Stage 5 — Load to Supabase
    engine   = get_engine(DATABASE_URL)
    metadata = MetaData()
    tables   = ensure_tables(engine, metadata)

    # 5a — Upsert ticker into symbols master table
    symbol_id = upsert_symbol(engine, tables, SYMBOL)

    # 5b — Log this ingestion run (initial entry; updated after load)
    run_id = log_ingestion_run(
        engine, tables, symbol_id,
        source_url=source_url,
        status="in_progress",
    )

    # 5c — Upsert raw OHLCV into daily_prices
    price_rows = load_daily_prices(validated_df, engine, tables, symbol_id, run_id)

    # 5d — Upsert derived metrics into daily_price_metrics
    metric_rows = load_daily_metrics(validated_df, engine, tables, symbol_id)

    # 5e — Update ingestion run record with final counts and success status
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE public.api_ingestion_runs
                SET status = 'success', records_inserted = :n
                WHERE run_id = :rid
            """),
            {"n": price_rows, "rid": run_id}
        )
    logger.info("DB | api_ingestion_runs updated — run_id=%d  status=success", run_id)

    engine.dispose()

    # Summary
    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info("symbol_id      : %d", symbol_id)
    logger.info("run_id         : %d", run_id)
    logger.info("Prices upserted: %d", price_rows)
    logger.info("Metrics upserted: %d", metric_rows)
    logger.info("Elapsed time   : %.2f seconds", elapsed)
    logger.info("Log file       : %s", log_file)
    logger.info("=" * 60)


# ==============================================================================
# ENTRY POINT
# ==============================================================================

if __name__ == "__main__":
    try:
        run_pipeline()
    except DataQualityError as dqe:
        logger.critical("Pipeline aborted — data quality failure:\n%s", dqe)
        sys.exit(2)
    except Exception as exc:
        logger.critical("Pipeline aborted — unexpected error: %s", exc, exc_info=True)
        sys.exit(1)