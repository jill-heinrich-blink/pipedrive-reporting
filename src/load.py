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
import json
import logging
import sqlite3
from pathlib import Path
from datetime import datetime, timezone, date

BASE_DIR    = Path(__file__).resolve().parent.parent
CONFIG_DIR  = BASE_DIR / "config"
DATA_DIR    = BASE_DIR / "data"
EXPORT_DIR  = DATA_DIR / "exports"
DB_PATH     = DATA_DIR / "pipedrive.db"

EXPORT_DIR.mkdir(parents=True, exist_ok=True)

with open(CONFIG_DIR / "targets.json") as f:
    TARGETS = json.load(f)

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
            t.stage_dwell_status,
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
            t.rfp,
            t.service_type,
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
            t.stage_dwell_status,
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


def export_owner_goals(conn):
    """
    Report 9: Owner Goals & Progress-to-Target — the 4 beta metrics
    (Sales, Pipeline Coverage, Reporting Tag deals, Elevated Buyer deals)
    per owner, driven by config/targets.json since Pipedrive has no native
    concept of an external target.
    """
    q_start = TARGETS["quarter_start"]
    q_end   = TARGETS["quarter_end"]
    mult    = TARGETS["pipeline_coverage_multiplier"]
    stage_zero_ids = TARGETS["stage_zero_exclusion"]["stage_ids"]
    tag_filter     = TARGETS["reporting_tag_filter"]
    label_filter   = TARGETS["elevated_buyer_label"]
    today = date.today().isoformat()
    stage_zero_clause = ",".join(str(s) for s in stage_zero_ids)

    rows = []
    for owner_id_str, cfg in TARGETS["owners"].items():
        if owner_id_str.startswith("_"):
            continue
        owner_id = int(owner_id_str)
        goal = cfg["sales_goal"]

        sales_actual = conn.execute(f"""
            SELECT COALESCE(SUM(value), 0) FROM deals_transformed
            WHERE owner_id = ? AND status = 'won'
              AND date(won_time) BETWEEN date(?) AND date(?)
        """, (owner_id, q_start, q_end)).fetchone()[0]

        open_all = conn.execute(f"""
            SELECT COALESCE(SUM(value), 0) FROM deals_transformed
            WHERE owner_id = ? AND status = 'open'
              AND stage_id NOT IN ({stage_zero_clause})
              AND pipeline_id IN (2, 7, 8)
        """, (owner_id,)).fetchone()[0]

        open_window = conn.execute(f"""
            SELECT COALESCE(SUM(value), 0) FROM deals_transformed
            WHERE owner_id = ? AND status = 'open'
              AND stage_id NOT IN ({stage_zero_clause})
              AND pipeline_id IN (2, 7, 8)
              AND expected_close_date IS NOT NULL AND expected_close_date != ''
              AND date(expected_close_date) BETWEEN date(?) AND date(?)
        """, (owner_id, today, q_end)).fetchone()[0]

        overdue = conn.execute(f"""
            SELECT COUNT(*), COALESCE(SUM(value), 0) FROM deals_transformed
            WHERE owner_id = ? AND status = 'open'
              AND stage_id NOT IN ({stage_zero_clause})
              AND expected_close_date IS NOT NULL AND expected_close_date != ''
              AND date(expected_close_date) < date(?)
        """, (owner_id, today)).fetchone()

        no_close_date = conn.execute(f"""
            SELECT COUNT(*) FROM deals_transformed
            WHERE owner_id = ? AND status = 'open'
              AND stage_id NOT IN ({stage_zero_clause})
              AND (expected_close_date IS NULL OR expected_close_date = '')
        """, (owner_id,)).fetchone()[0]

        no_person = conn.execute(f"""
            SELECT COUNT(*) FROM deals_transformed
            WHERE owner_id = ? AND status = 'open'
              AND stage_id NOT IN ({stage_zero_clause})
              AND (person_id IS NULL OR person_id = '')
        """, (owner_id,)).fetchone()[0]

        stage_zero_count = conn.execute(f"""
            SELECT COUNT(*) FROM deals_transformed
            WHERE owner_id = ? AND status = 'open' AND stage_id IN ({stage_zero_clause})
        """, (owner_id,)).fetchone()[0]

        tag_deals = conn.execute(f"""
            SELECT COUNT(*), COALESCE(SUM(t.value), 0) FROM deals_transformed t
            JOIN dim_persons p ON t.person_id = p.person_id
            WHERE t.owner_id = ? AND t.status = 'open'
              AND t.stage_id NOT IN ({stage_zero_clause})
              AND p.reporting_tag = ?
        """, (owner_id, tag_filter)).fetchone()

        elevated_engaged = conn.execute("""
            SELECT COUNT(*) FROM deals_transformed t
            JOIN dim_persons p ON t.person_id = p.person_id
            JOIN dim_stages s ON t.stage_id = s.stage_id
            WHERE t.owner_id = ? AND t.pipeline_id = 10
              AND p.label = ? AND s.name = 'Engaged'
        """, (owner_id, label_filter)).fetchone()[0]

        elevated_mtg = conn.execute("""
            SELECT COUNT(*) FROM deals_transformed t
            JOIN dim_persons p ON t.person_id = p.person_id
            JOIN dim_stages s ON t.stage_id = s.stage_id
            WHERE t.owner_id = ? AND t.pipeline_id = 10
              AND p.label = ? AND s.name = 'Meeting Scheduled'
              AND t.outbound_meeting_date IS NOT NULL AND t.outbound_meeting_date != ''
              AND date(t.outbound_meeting_date) BETWEEN date(?) AND date(?)
        """, (owner_id, label_filter, today, q_end)).fetchone()[0]

        coverage_goal = goal * mult
        win_rate = cfg.get("win_rate", 1 / mult if mult else 0.37)
        remaining_goal = max(goal - sales_actual, 0)
        pipeline_needed = round(remaining_goal / win_rate, 2) if win_rate else None
        new_pipeline_required = max(round(pipeline_needed - open_window, 2), 0) if pipeline_needed is not None else None

        rows.append({
            "owner_id": owner_id,
            "owner_name": cfg.get("name"),
            "sales_goal": goal,
            "sales_goal_source": cfg.get("source"),
            "sales_actual": sales_actual,
            "attainment_pct": round(sales_actual / goal, 4) if goal else None,
            "remaining_goal": remaining_goal,
            "win_rate": win_rate,
            "pipeline_needed": pipeline_needed,
            "new_pipeline_required": new_pipeline_required,
            "pipeline_coverage_multiplier": mult,
            "coverage_goal": coverage_goal,
            "open_pipeline_all": open_all,
            "open_pipeline_in_window": open_window,
            "coverage_pct_all": round(open_all / coverage_goal, 4) if coverage_goal else None,
            "coverage_pct_in_window": round(open_window / coverage_goal, 4) if coverage_goal else None,
            "coverage_pct_vs_pipeline_needed": round(open_window / pipeline_needed, 4) if pipeline_needed else None,
            "open_deals_overdue_count": overdue[0],
            "open_deals_overdue_value": overdue[1],
            "open_deals_no_close_date_count": no_close_date,
            "open_deals_no_person_count": no_person,
            "open_deals_stage_zero_count": stage_zero_count,
            "reporting_tag_deal_count": tag_deals[0],
            "reporting_tag_deal_value": tag_deals[1],
            "elevated_buyer_engaged_count": elevated_engaged,
            "elevated_buyer_meeting_scheduled_count": elevated_mtg,
            "elevated_buyer_total_count": elevated_engaged + elevated_mtg,
            "quarter_start": q_start,
            "quarter_end": q_end,
            "as_of_date": today,
        })

    if not rows:
        log.warning("  09_owner_goals.csv: no owners configured in targets.json")
        return 0

    path = EXPORT_DIR / "09_owner_goals.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    log.info(f"  09_owner_goals.csv: {len(rows)} rows → {path}")
    return len(rows)


def export_account_coverage(conn):
    """
    Report 12: Account Coverage — Open Pipeline vs. Closed Sales by
    Organization, across all owners configured in targets.json. One row
    per (owner, organization) so the same org can appear more than once
    if multiple reps touch it.
    """
    q_start = TARGETS["quarter_start"]
    q_end   = TARGETS["quarter_end"]
    today = date.today().isoformat()
    stage_zero_ids = TARGETS["stage_zero_exclusion"]["stage_ids"]
    stage_zero_clause = ",".join(str(s) for s in stage_zero_ids)
    owner_ids = [int(k) for k in TARGETS["owners"] if not k.startswith("_")]
    placeholders = ",".join("?" for _ in owner_ids)

    rows = conn.execute(f"""
        SELECT
            t.owner_id,
            COALESCE(o.name, '(no organization linked)') AS org_name,
            t.org_id,
            SUM(CASE WHEN t.status='won' AND date(t.won_time) BETWEEN date(?) AND date(?)
                     THEN t.value ELSE 0 END) AS won_value,
            SUM(CASE WHEN t.status='open' AND t.stage_id NOT IN ({stage_zero_clause})
                       AND t.pipeline_id IN (2,7,8)
                     THEN t.value ELSE 0 END) AS open_pipeline_value_all,
            SUM(CASE WHEN t.status='open' AND t.stage_id NOT IN ({stage_zero_clause})
                       AND t.pipeline_id IN (2,7,8)
                       AND t.expected_close_date IS NOT NULL AND t.expected_close_date != ''
                       AND date(t.expected_close_date) BETWEEN date(?) AND date(?)
                     THEN t.value ELSE 0 END) AS open_pipeline_value_in_window
        FROM deals_transformed t
        LEFT JOIN dim_organizations o ON t.org_id = o.org_id
        WHERE t.owner_id IN ({placeholders})
          AND t.org_id IS NOT NULL
        GROUP BY t.owner_id, t.org_id
        HAVING won_value > 0 OR open_pipeline_value_in_window > 0
        ORDER BY t.owner_id, (won_value + open_pipeline_value_in_window) DESC
    """, (q_start, q_end, today, q_end, *owner_ids)).fetchall()

    out_rows = []
    for r in rows:
        owner_cfg = TARGETS["owners"].get(str(r["owner_id"]), {})
        out_rows.append({
            "owner_name": owner_cfg.get("name", str(r["owner_id"])),
            "org_name": r["org_name"],
            "org_id": r["org_id"],
            "won_value": r["won_value"],
            "open_pipeline_value_in_window": r["open_pipeline_value_in_window"],
            "open_pipeline_value_all": r["open_pipeline_value_all"],
        })

    if not out_rows:
        log.warning("  12_account_coverage.csv: no rows")
        return 0
    path = EXPORT_DIR / "12_account_coverage.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=out_rows[0].keys())
        writer.writeheader()
        writer.writerows(out_rows)
    log.info(f"  12_account_coverage.csv: {len(out_rows)} rows → {path}")
    return len(out_rows)


def export_account_coverage_detail(conn):
    """
    Report 13: Account Coverage detail — the individual deals behind each
    org bar in the Open Pipeline vs. Closed Sales chart (Report 12). One row
    per deal, so the dashboard can show a click-through list: deal name,
    value, close date, and a link to open the deal in Pipedrive.
    """
    q_start = TARGETS["quarter_start"]
    q_end   = TARGETS["quarter_end"]
    today = date.today().isoformat()
    stage_zero_ids = TARGETS["stage_zero_exclusion"]["stage_ids"]
    stage_zero_clause = ",".join(str(s) for s in stage_zero_ids)
    owner_ids = [int(k) for k in TARGETS["owners"] if not k.startswith("_")]
    placeholders = ",".join("?" for _ in owner_ids)
    pipedrive_domain = TARGETS.get("pipedrive_domain", "blinkux.pipedrive.com")

    rows = conn.execute(f"""
        SELECT
            t.deal_id,
            t.title,
            t.owner_id,
            t.org_id,
            COALESCE(o.name, '(no organization linked)') AS org_name,
            t.value,
            t.status,
            t.expected_close_date,
            t.won_time
        FROM deals_transformed t
        LEFT JOIN dim_organizations o ON t.org_id = o.org_id
        WHERE t.owner_id IN ({placeholders})
          AND t.org_id IS NOT NULL
          AND (
                (t.status = 'won' AND date(t.won_time) BETWEEN date(?) AND date(?))
             OR (t.status = 'open' AND t.stage_id NOT IN ({stage_zero_clause})
                 AND t.pipeline_id IN (2,7,8)
                 AND t.expected_close_date IS NOT NULL AND t.expected_close_date != ''
                 AND date(t.expected_close_date) BETWEEN date(?) AND date(?))
              )
        ORDER BY t.owner_id, t.org_id, t.status, t.value DESC
    """, (*owner_ids, q_start, q_end, today, q_end)).fetchall()

    out_rows = []
    for r in rows:
        owner_cfg = TARGETS["owners"].get(str(r["owner_id"]), {})
        close_date = r["won_time"] if r["status"] == "won" else r["expected_close_date"]
        out_rows.append({
            "owner_name": owner_cfg.get("name", str(r["owner_id"])),
            "org_name": r["org_name"],
            "org_id": r["org_id"],
            "deal_id": r["deal_id"],
            "title": r["title"],
            "status": r["status"],
            "value": r["value"],
            "close_date": close_date,
            "pipedrive_url": f"https://{pipedrive_domain}/deal/{r['deal_id']}",
        })

    if not out_rows:
        log.warning("  13_account_coverage_detail.csv: no rows")
        return 0
    path = EXPORT_DIR / "13_account_coverage_detail.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=out_rows[0].keys())
        writer.writeheader()
        writer.writerows(out_rows)
    log.info(f"  13_account_coverage_detail.csv: {len(out_rows)} rows → {path}")
    return len(out_rows)


def export_ap_buyer_targets(conn):
    """
    Report 14: AP Buyer target vs. actual — per named buyer from each owner's
    account plan (config/targets.json → ap_buyer_targets), matched against
    actual Pipedrive activity by person name. Targets live in account-plan
    documents, not Pipedrive, so they're maintained by hand until a better
    capture mechanism exists.
    """
    q_start = TARGETS["quarter_start"]
    q_end   = TARGETS["quarter_end"]
    ap_targets = TARGETS.get("ap_buyer_targets", {})

    out_rows = []
    for owner_id_str, cfg in TARGETS["owners"].items():
        if owner_id_str.startswith("_"):
            continue
        owner_id = int(owner_id_str)
        for target in ap_targets.get(owner_id_str, []):
            buyer_name = target["buyer"].split(" (")[0]  # strip "(TEST FIXTURE)" etc. for matching

            actual = conn.execute("""
                SELECT
                    COALESCE(SUM(CASE WHEN t.status='open' THEN t.value ELSE 0 END), 0) AS actual_pipeline,
                    COALESCE(SUM(CASE WHEN t.status='won' AND date(t.won_time) BETWEEN date(?) AND date(?)
                                  THEN t.value ELSE 0 END), 0) AS actual_sales,
                    GROUP_CONCAT(DISTINCT s.name) AS stages,
                    COUNT(*) AS deal_count
                FROM deals_transformed t
                JOIN dim_persons p ON t.person_id = p.person_id
                LEFT JOIN dim_stages s ON t.stage_id = s.stage_id
                WHERE t.owner_id = ? AND p.name = ?
                  AND t.status IN ('open', 'won')
            """, (q_start, q_end, owner_id, buyer_name)).fetchone()

            out_rows.append({
                "owner_name": cfg.get("name"),
                "buyer": target["buyer"],
                "target_source": target.get("source", ""),
                "expected_pipeline": target["expected_pipeline"],
                "expected_sales": target["expected_sales"],
                "actual_pipeline": actual["actual_pipeline"],
                "actual_sales": actual["actual_sales"],
                "current_stages": actual["stages"] or "(no matching deals)",
                "matching_deal_count": actual["deal_count"],
            })

    if not out_rows:
        log.warning("  14_ap_buyer_targets.csv: no rows")
        return 0
    path = EXPORT_DIR / "14_ap_buyer_targets.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=out_rows[0].keys())
        writer.writeheader()
        writer.writerows(out_rows)
    log.info(f"  14_ap_buyer_targets.csv: {len(out_rows)} rows → {path}")
    return len(out_rows)


def export_reporting_tag_detail(conn):
    """Report 10 (detail): the actual open deals behind the Reporting Tag count."""
    tag_filter = TARGETS["reporting_tag_filter"]
    stage_zero_ids = TARGETS["stage_zero_exclusion"]["stage_ids"]
    stage_zero_clause = ",".join(str(s) for s in stage_zero_ids)
    write_csv(conn, f"""
        SELECT t.deal_id, t.title, u.name AS owner_name, pl.name AS pipeline_name,
               s.name AS stage_name, t.value, t.expected_close_date,
               p.person_id, p.name AS person_name, p.reporting_tag
        FROM deals_transformed t
        JOIN dim_persons p ON t.person_id = p.person_id
        LEFT JOIN dim_users u ON t.owner_id = u.user_id
        LEFT JOIN dim_pipelines pl ON t.pipeline_id = pl.pipeline_id
        LEFT JOIN dim_stages s ON t.stage_id = s.stage_id
        WHERE t.status = 'open'
          AND t.stage_id NOT IN ({stage_zero_clause})
          AND p.reporting_tag = ?
        ORDER BY u.name, t.value DESC
    """, "10_reporting_tag_detail.csv", (tag_filter,))


def export_elevated_buyer_detail(conn):
    """Report 11 (detail): the actual Pre-Sales deals behind the Elevated Buyer count."""
    label_filter = TARGETS["elevated_buyer_label"]
    q_end = TARGETS["quarter_end"]
    today = date.today().isoformat()
    write_csv(conn, """
        SELECT t.deal_id, t.title, u.name AS owner_name, s.name AS stage_name,
               t.outbound_meeting_date, p.person_id, p.name AS person_name, p.label,
               CASE
                   WHEN s.name = 'Engaged' THEN 1
                   WHEN s.name = 'Meeting Scheduled'
                    AND t.outbound_meeting_date IS NOT NULL AND t.outbound_meeting_date != ''
                    AND date(t.outbound_meeting_date) BETWEEN date(?) AND date(?)
                   THEN 1 ELSE 0
               END AS qualifies
        FROM deals_transformed t
        JOIN dim_persons p ON t.person_id = p.person_id
        LEFT JOIN dim_users u ON t.owner_id = u.user_id
        LEFT JOIN dim_stages s ON t.stage_id = s.stage_id
        WHERE t.pipeline_id = 10 AND p.label = ?
        ORDER BY qualifies DESC, u.name
    """, "11_elevated_buyer_detail.csv", (today, q_end, label_filter))


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
    export_owner_goals(conn)
    export_account_coverage(conn)
    export_account_coverage_detail(conn)
    export_ap_buyer_targets(conn)
    export_reporting_tag_detail(conn)
    export_elevated_buyer_detail(conn)
    export_metadata(conn)
    write_manifest()

    log.info(f"=== Load complete — exports in {EXPORT_DIR} ===")


if __name__ == "__main__":
    main()
