# pipedrive-reporting

Pipedrive data extraction and reporting pipeline for Blink / Mphasis.
Pulls deal data from the Pipedrive API, computes derived metrics, and exports
CSVs ready for Looker Studio or Google Sheets.

## Structure

```
pipedrive-reporting/
  src/
    extract.py      Pipedrive API → SQLite (deals, changelog, dimensions)
    transform.py    Derived fields: LMSD, stall flags, engagement segments, rate realisation
    load.py         SQLite → CSV exports (one file per report)
    run_all.py      Runs extract → transform → load in sequence
  config/
    fields.json     Field key mappings, pipeline/stage IDs, stall thresholds
  data/             SQLite DB and CSV exports (gitignored)
  requirements.txt
```

## Setup

**1. Install Python dependencies**
```bash
pip install -r requirements.txt
```

**2. Set your Pipedrive API token**
```bash
export PIPEDRIVE_API_TOKEN=your_token_here
```
Or create a `.env` file (never commit this):
```
PIPEDRIVE_API_TOKEN=your_token_here
```

**3. First run — full extraction**
```bash
python src/run_all.py --mode full
```
This fetches all open, won, and lost deals plus changelog history.
Takes a few minutes depending on your deal volume.

**4. Daily incremental run**
```bash
python src/run_all.py
```
Only fetches deals updated since the last run. Typically fast (< 1 minute).

**5. Find your exports**
CSV files are written to `data/exports/`. One file per report:

| File | Report |
|---|---|
| `01_pipeline_health.csv` | Pipeline Health by Stage |
| `02_stalling_deals.csv` | Stalling Deals |
| `03_won_lost.csv` | Won / Lost Analysis |
| `04_pre_sales.csv` | Pre-Sales Volume × Meetings |
| `05_engagement.csv` | Engagement |
| `06a_velocity_summary.csv` | Velocity — pipeline summary |
| `06b_stage_dwell.csv` | Velocity — stage dwell times |
| `07_commercial_fit.csv` | Commercial Fit |
| `08_forecast_confidence.csv` | Forecast Confidence |
| `dim_pipelines.csv` | Pipeline dimension table |
| `dim_stages.csv` | Stage dimension table |
| `dim_users.csv` | User dimension table |

## Connecting to Looker Studio

1. Upload each CSV to a Google Sheet (one sheet per report, or separate files).
2. In Looker Studio: Add data source → Google Sheets → select your sheet.
3. Build reports using the field definitions in `Pipedrive_Reporting_Specification_v1.docx`.

## Key derived fields

| Field | Definition |
|---|---|
| `lmsd` | Last Meaningful Signal Date = MAX(last_activity_date, last_outgoing_mail_time, last_incoming_mail_time) |
| `days_since_signal` | Days since LMSD |
| `stall_severity` | Healthy / Watch / At-Risk / Critical |
| `stall_signals_triggered` | Comma-separated list of active stall signals |
| `engagement_segment` | Active Two-Way / Active Outbound / One-Sided (Stalling) / Activity Only / Going Dark / No Signal |
| `rate_realisation_pct` | Price to Client ÷ RPH Sold × 100 |

## Notes

- Email signals (LMSD, engagement segments) require Pipedrive email sync to be active per BD user.
  Check: Pipedrive Admin → Users → Email sync status.
- The `data/` directory is gitignored. Never commit `pipedrive.db` or the CSV exports
  as they contain client and deal data.
- API token should be stored in an environment variable, never hardcoded or committed.
