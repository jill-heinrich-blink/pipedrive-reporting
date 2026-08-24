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

Note: this path works (verified 2026-08-24) even though Apps Script is disabled
org-wide — the block is specific to Apps Script/bound scripts, not to Looker
Studio or the Sheets connector itself. The catch is refresh: with Apps Script
off, nothing auto-updates the Sheet. For the beta, refresh means manually
re-importing the CSV (File → Import → Replace current sheet, not a new file —
replacing in place keeps Looker Studio's data source binding intact).

## Alternative: GitHub Pages dashboard (no Google dependency)

`docs/index.html` is a standalone dashboard (Report 9 — Owner Goals) that
needs no Google account, Apps Script, or Looker Studio. It fetches
`docs/data/*.csv` at page load and renders scorecards, a chart, and detail
tables per owner.

**One-time setup:** push this repo, then in GitHub: Settings → Pages →
Source: Deploy from branch → `main` / `docs`.

**Refresh cycle:**
```bash
python src/run_all.py            # updates data/exports/*.csv
python src/publish_to_pages.py    # copies the beta exports into docs/data
git add docs/data && git commit -m "Refresh beta dashboard data" && git push
```
GitHub Pages serves whatever's committed — there's no live link to the
database, so a push is required for the dashboard to show new numbers. This
sandbox has no GitHub push credentials, so pushes have to happen from a
machine that does.

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
