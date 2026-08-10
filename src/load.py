"""
load.py — Export transformed data to CSV files for Looker Studio
Blink / Mphasis  |  pipedrive-reporting

Writes one CSV per report view to data/exports/.
These CSVs can be:
  - Uploaded manually to Google Sheets (then connected to Looker Studio)
  - Synced automatically via Google Sheets API (future enhancement)
  - Imported to BigQuery for larger-scale use

Run after transform.py:
    python src/load.py
"""

import csv
import logging
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

BASE_DIR    = Path(__file__).resolve().parent.parent
DATA_DIR    = BASE_DIR / "data"
EXPORT_DIR  = DATA_DIR / "exports"
DB_PATH     = DATA_DIR / "pipedrive.db"

EXPORT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def write_csv(conn: sqlite3.Connection, query: str, filename: str, params=()) -> int:
    rows = conn.execute(query, params).fetchall()
    if not rows:
        log.warning(f"  {filename}: no rows returned")
        return 0
    path = EXPORT_DIR / filename
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows([dict(r) for r in rows])
    log.info(f"  {filename}: {len(rows)} rows → {path}")
    return len(rows)


# ── Export queries ─────────────────────────────────────────────────────────────

def export_pipeline_health(conn):
    """Report 1: Pipeline Health by Stage — one row per open deal with stage age."""
    write_csv(conn, """
        SELECT
            t.deal_id,
            t.title,
            t.status,
            p.name                          AS pipeline_name,
            s.name                          AS stage_name,
            s.stage_order,
            u.name                          AS owner_name,
            t.value,
            t.weighted_value,
            t.probability,
            t.expected_close_date,
            t.stage_change_time,
            t.days_in_stage,
            CASE
                WHEN t.days_in_stage IS NULL   THEN 'Unknown'
                WHEN t.days_in_stage <= 30     THEN '0-30 days'
                WHEN t.days_in_stage <= 60     THEN '31-60 days'
                WHEN t.days_in_stage <= 90     THEN '61-90 days'
                ELSE '90+ days'
            END                             AS age_bucket,
            t.lmsd,
            t.days_since_signal,
            t.add_time,
            t.industry,
            t.project_type,
            t.contract_type,
            t.lead_source
        FROM deals_transformed t
        LEFT JOIN dim_pipelines p ON t.pipeline_id = p.pipeline_id
        LEFT JOIN dim_stages    s ON t.stage_id    = s.stage_id
        LEFT JOIN dim_users     u ON t.owner_id    = u.user_id
        WHERE t.status = 'open'
        ORDER BY p.name, s.stage_order, t.value DESC
    """, "01_pipeline_health.csv")


def export_stalling_deals(conn):
    """Report 2: Stalling Deal Register — open stalling deals only."""
    write_csv(conn, """
        SELECT
            t.deal_id,
            t.title,
            p.name                          AS pipeline_name,
            s.name                          AS stage_name,
            s.stage_order,
            u.name                          AS owner_name,
            t.value,
            t.weighted_value,
            t.lmsd                          AS last_meaningful_signal_date,
            t.lmsd_source                   AS signal_type,
            t.days_since_signal,
            t.last_activity_date,
            t.last_outgoing_mail_time,
            t.last_incoming_mail_time,
            t.days_in_stage,
            t.expected_close_date,
            t.days_until_close,
            t.undone_activities_count,
            t.stall_severity,
            t.stall_score,
            t.stall_signals_triggered,
            t.signal_no_signal,
            t.signal_no_next_activity,
            t.signal_stage_unchanged,
            t.signal_proposal_ghost,
            t.signal_one_sided,
            t.signal_committed_no_close,
            t.signal_close_overdue,
            t.signal_low_prob_dark,
            t.position_to_win
        FROM deals_transformed t
        LEFT JOIN dim_pipelines p ON t.pipeline_id = p.pipeline_id
        LEFT JOIN dim_stages    s ON t.stage_id    = s.stage_id
        LEFT JOIN dim_users     u ON t.owner_id    = u.user_id
        WHERE t.status = 'open'
          AND t.is_stalling = 1
        ORDER BY
            CASE t.stall_severity
                WHEN 'Critical' THEN 1
                WHEN 'At-Risk'  THEN 2
                WHEN 'Watch'    THEN 3
                ELSE 4
            END,
            t.days_since_signal DESC NULLS LAST
    """, "02_stalling_deals.csv")


def export_won_lost(conn):
    """Report 3: Won / Lost deals with all segmentation dimensions."""
    write_csv(conn, """
        SELECT
            t.deal_id,
            t.title,
            t.status,
            p.name                          AS pipeline_name,
            s.name                          AS stage_name,
            u.name                          AS owner_name,
            t.value,
            t.add_time,
            t.won_time,
            t.lost_time,
            t.close_time,
            t.lost_reason,
            CASE
                WHEN t.won_time IS NOT NULL AND t.add_time IS NOT NULL
                THEN CAST(
                    (julianday(t.won_time) - julianday(t.add_time)) AS INTEGER
                ) END                       AS days_to_win,
            CASE
                WHEN t.lost_time IS NOT NULL AND t.add_time IS NOT NULL
                THEN CAST(
                    (julianday(t.lost_time) - julianday(t.add_time)) AS INTEGER
                ) END                       AS days_to_loss,
            t.industry,
            t.project_type,
            t.service_type,
            t.deal_type,
            t.contract_type,
            t.rfp,
            t.lead_source,
            t.has_discount,
            t.right_fit_for_blink,
            t.mphasis_engineering,
            t.mphasis_engineering_value,
            t.disqualification_reason,
            t.rate_realisation_pct
        FROM deals_transformed t
        LEFT JOIN dim_pipelines p ON t.pipeline_id = p.pipeline_id
        LEFT JOIN dim_stages    s ON t.stage_id    = s.stage_id
        LEFT JOIN dim_users     u ON t.owner_id    = u.user_id
        WHERE t.status IN ('won', 'lost')
        ORDER BY COALESCE(t.won_time, t.lost_time) DESC
    """, "03_won_lost.csv")


def export_pre_sales(conn):
    """Report 4: Pre-Sales pipeline — all deals in pipeline 10."""
    write_csv(conn, """
        SELECT
            t.deal_id,
            t.title,
            t.status,
            s.name                          AS stage_name,
            s.stage_order,
            u.name                          AS owner_name,
            t.add_time,
            t.stage_change_time,
            t.days_in_stage,
            t.lmsd,
            t.days_since_signal,
            t.outbound_meeting_date,
            t.touch_type,
            t.deal_outreach_tag,
            t.disqualification_reason,
            t.re_engagement_eligible,
            t.position_to_win,
            t.is_stalling,
            t.stall_severity,
            t.won_time,
            t.lost_time,
            t.lost_reason
        FROM deals_transformed t
        LEFT JOIN dim_stages s ON t.stage_id = s.stage_id
        LEFT JOIN dim_users  u ON t.owner_id = u.user_id
        WHERE t.pipeline_id = 10
        ORDER BY s.stage_order, t.days_in_stage DESC
    """, "04_pre_sales.csv")


def export_engagement(conn):
    """Report 5: Engagement — all open deals with email/activity signals."""
    write_csv(conn, """
        SELECT
            t.deal_id,
            t.title,
            p.name                          AS pipeline_name,
            s.name                          AS stage_name,
            u.name                          AS owner_name,
            t.value,
            t.lmsd                          AS last_meaningful_signal_date,
            t.lmsd_source                   AS signal_type,
            t.days_since_signal,
            t.last_activity_date,
            t.last_outgoing_mail_time,
            t.last_incoming_mail_time,
            CASE
                WHEN t.last_outgoing_mail_time IS NOT NULL
                 AND t.last_incoming_mail_time IS NOT NULL
                THEN CAST(
                    julianday(t.last_outgoing_mail_time) -
                    julianday(t.last_incoming_mail_time)
                    AS INTEGER)
                ELSE NULL
            END                             AS outbound_inbound_gap_days,
            t.email_messages_count,
            t.activities_count,
            t.done_activities_count,
            t.undone_activities_count,
            t.engagement_segment,
            t.signal_one_sided,
            -- Email sync indicator: NULL outgoing mail = likely no sync
            CASE
                WHEN t.last_outgoing_mail_time IS NULL
                 AND t.last_activity_date IS NULL THEN 'No sync detected'
                WHEN t.last_outgoing_mail_time IS NULL THEN 'Possible — activity only'
                ELSE 'Sync active'
            END                             AS email_sync_status
        FROM deals_transformed t
        LEFT JOIN dim_pipelines p ON t.pipeline_id = p.pipeline_id
        LEFT JOIN dim_stages    s ON t.stage_id    = s.stage_id
        LEFT JOIN dim_users     u ON t.owner_id    = u.user_id
        WHERE t.status = 'open'
        ORDER BY t.engagement_segment, t.days_since_signal DESC NULLS LAST
    """, "05_engagement.csv")


def export_velocity(conn):
    """Report 6: Velocity inputs — won/lost deals + stage dwell times."""
    # Won/lost summary for velocity formula
    write_csv(conn, """
        SELECT
            p.name                          AS pipeline_name,
            t.status,
            COUNT(*)                        AS deal_count,
            AVG(t.value)                    AS avg_value,
            AVG(CASE
                WHEN t.status = 'won' AND t.won_time IS NOT NULL AND t.add_time IS NOT NULL
                THEN julianday(t.won_time) - julianday(t.add_time)
                WHEN t.status = 'lost' AND t.lost_time IS NOT NULL AND t.add_time IS NOT NULL
                THEN julianday(t.lost_time) - julianday(t.add_time)
                ELSE NULL END)              AS avg_cycle_days,
            SUM(t.value)                    AS total_value
        FROM deals_transformed t
        LEFT JOIN dim_pipelines p ON t.pipeline_id = p.pipeline_id
        WHERE t.status IN ('won', 'lost')
          AND COALESCE(t.won_time, t.lost_time) >= date('now', '-90 days')
        GROUP BY p.name, t.status
    """, "06a_velocity_summary.csv")

    # Stage dwell times
    write_csv(conn, """
        SELECT
            h.deal_id,
            p.name                          AS pipeline_name,
            s.name                          AS stage_name,
            s.stage_order,
            h.entered_at,
            h.exited_at,
            h.dwell_days,
            h.is_current,
            t.status                        AS deal_status
        FROM deal_stage_history h
        LEFT JOIN deals_transformed t  ON h.deal_id  = t.deal_id
        LEFT JOIN dim_stages        s  ON h.stage_id = s.stage_id
        LEFT JOIN dim_pipelines     p  ON s.pipeline_id = p.pipeline_id
        WHERE h.dwell_days IS NOT NULL OR h.is_current = 1
        ORDER BY h.deal_id, h.entered_at
    """, "06b_stage_dwell.csv")


def export_commercial_fit(conn):
    """Report 7: Commercial Fit — rate realisation, discount, Mphasis attach."""
    write_csv(conn, """
        SELECT
            t.deal_id,
            t.title,
            t.status,
            p.name                          AS pipeline_name,
            u.name                          AS owner_name,
            t.value,
            t.rph_sold,
            t.price_to_client,
            t.rate_realisation_pct,
            t.has_discount,
            t.right_fit_for_blink,
            t.mphasis_engineering,
            t.mphasis_engineering_value,
            t.project_type,
            t.service_type,
            t.contract_type,
            t.deal_type,
            t.rfp,
            t.industry,
            t.won_time,
            t.add_time
        FROM deals_transformed t
        LEFT JOIN dim_pipelines p ON t.pipeline_id = p.pipeline_id
        LEFT JOIN dim_users     u ON t.owner_id    = u.user_id
        WHERE t.status IN ('open', 'won')
          AND (t.rph_sold IS NOT NULL OR t.price_to_client IS NOT NULL
               OR t.has_discount IS NOT NULL OR t.mphasis_engineering IS NOT NULL)
        ORDER BY t.status, t.rate_realisation_pct ASC NULLS LAST
    """, "07_commercial_fit.csv")


def export_forecast_confidence(conn):
    """Report 8: Forecast Confidence — position-to-win vs actual outcomes."""
    write_csv(conn, """
        SELECT
            t.deal_id,
            t.title,
            t.status,
            p.name                          AS pipeline_name,
            s.name                          AS stage_name,
            u.name                          AS owner_name,
            t.value,
            t.weighted_value,
            t.probability,
            t.position_to_win,
            t.expected_close_date,
            t.won_time,
            t.lost_time,
            -- Close date accuracy (won deals only)
            CASE
                WHEN t.status = 'won'
                 AND t.won_time IS NOT NULL
                 AND t.expected_close_date IS NOT NULL
                THEN CAST(
                    julianday(t.won_time) - julianday(t.expected_close_date)
                    AS INTEGER)
                ELSE NULL
            END                             AS close_date_slip_days,
            t.add_time,
            t.stage_change_time,
            t.days_until_close
        FROM deals_transformed t
        LEFT JOIN dim_pipelines p ON t.pipeline_id = p.pipeline_id
        LEFT JOIN dim_stages    s ON t.stage_id    = s.stage_id
        LEFT JOIN dim_users     u ON t.owner_id    = u.user_id
        WHERE t.position_to_win IS NOT NULL
           OR t.status IN ('won', 'lost')
        ORDER BY t.status, p.name, t.position_to_win
    """, "08_forecast_confidence.csv")


def export_metadata(conn):
    """Dimension tables for joins in Looker Studio."""
    write_csv(conn, "SELECT * FROM dim_pipelines", "dim_pipelines.csv")
    write_csv(conn, "SELECT * FROM dim_stages ORDER BY pipeline_id, stage_order", "dim_stages.csv")
    write_csv(conn, "SELECT user_id, name, email FROM dim_users", "dim_users.csv")


def write_manifest():
    """Write a manifest file with export timestamp."""
    manifest_path = EXPORT_DIR / "manifest.json"
    import json
    manifest = {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "files": sorted(str(p.name) for p in EXPORT_DIR.glob("*.csv")),
    }
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    log.info(f"  Manifest written: {manifest_path}")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    conn = get_conn()
    log.info("=== Exporting CSVs ===")

    export_pipeline_health(conn)
    export_stalling_deals(conn)
    export_won_lost(conn)
    export_pre_sales(conn)
    export_engagement(conn)
    export_velocity(conn)
    export_commercial_fit(conn)
    export_forecast_confidence(conn)
    export_metadata(conn)
    write_manifest()

    log.info(f"=== Load complete — exports in {EXPORT_DIR} ===")


if __name__ == "__main__":
    main()
