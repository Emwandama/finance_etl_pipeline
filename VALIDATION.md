# Data Validation & Quality Checks

**Stage 4 of the RT FINANCE API ETL Pipeline**

Validation runs automatically inside `finance_etl_pipeline.py` after transformation and before any data is written to Supabase. It is implemented in the `validate()` function and uses a two-tier system: **hard checks** that abort the pipeline on failure, and **soft checks** that log warnings but allow the pipeline to continue.

---

## Hard Checks — Pipeline Aborts on Failure

If any hard check fails, a `DataQualityError` is raised, the pipeline exits with code `2`, and **nothing is written to Supabase**.

| # | Check | What it tests |
|---|---|---|
| 1 | **Null values in OHLCV** | No missing values in open, high, low, close, or volume |
| 2 | **High >= Low** | The high price is never less than the low price on any row |
| 3 | **Close within range** | The close price falls within [low, high] on every row |
| 4 | **Prices positive** | All four price columns are greater than zero |
| 5 | **Volume positive** | Volume is greater than zero on every row |
| 6 | **Non-empty dataset** | The DataFrame contains at least one row |

If all six pass, the pipeline logs:
```
VALIDATE | All hard quality checks PASSED (6 checks)
```

If any fail, the pipeline logs the specific rows and dates involved, then stops:
```
CRITICAL | Pipeline aborted — data quality failure:
DATA QUALITY HARD FAILURES:
  * high < low on 2 row(s): [datetime.date(2026, 3, 14), ...]
```

---

## Soft Checks — Warnings Only, Pipeline Continues

These checks flag potential data anomalies that don't necessarily mean the data is wrong, but are worth investigating. They are logged at WARNING or INFO level and do not prevent the Supabase load from proceeding.

| # | Check | Threshold | Log level |
|---|---|---|---|
| 7 | **Future-dated rows** | Any timestamp after today | WARNING |
| 8 | **Extreme daily returns** | \|return\| > 50% | WARNING |
| 9 | **Volume outliers** | \|z-score\| > 3 vs. 20-day window | INFO |
| 10 | **Date gaps** | Gap > 10 calendar days between trading days | WARNING |

Example log output for a soft warning:
```
VALIDATE | 4 row(s) flagged as volume outliers (|z| > 3)
VALIDATE | Soft warnings raised: 1
```

---

## Dataset Summary

After all checks complete, the pipeline logs a summary regardless of soft warning count:

```
VALIDATE | Summary — rows: 100 | date range: 2025-12-26 to 2026-05-20 | avg close: 259.21 | avg volume: 5,860,066
```

---

## Where Validation Sits in the Pipeline

```
Stage 1 — Extraction
Stage 2 — Cleaning
Stage 3 — Transformation
Stage 4 — Validation      ◄ runs here (validate() function)
Stage 5 — Supabase Load   ◄ only reached if all hard checks pass
```

Placing validation between transformation and loading ensures that derived metrics (`daily_return`, `volume_zscore`, etc.) are also checked before anything is written to the database.
