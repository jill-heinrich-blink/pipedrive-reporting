"""
transform.py — Derived fields, stall flags, engagement segments
Blink / Mphasis  |  pipedrive-reporting

Reads from: data/pipedrive.db  (deals_snapshot, deal_changelog, dim_stages)
Writes to:  data/pipedrive.db  (deals_transformed view + stage_history table)
            data/*.csv          (flat exports for Looker Studio)

Run after extract.py:
    python src/transform.py
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path

BASE_DIR   = Path(__file__).resolve().parent.parent
CONFIG_DIR = BASE_DIR / "config"
DATA_DIR   = BASE_DIR / "data"
DB_PATH    = DATA_DIR / "pipedrive.db"

with open(CONFIG_DIR / "fields.json") as f:
    FIELDS = json.load(f)

THRESHOLDS  = FIELDS["stall_thresholds"]
PIPELINE_IDS = FIELDS["pipelines"]          # name → id
STAGE_MAP    = FIELDS["stages"]             # pipeline_name → {stage_key → stage_id}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def days_since(date_str: str) -> int | None:
    """Return number of days since a date string (ISO format). None if blank."""
    if not date_str:
        return None
    try:
        d = datetime.fromisoformat(date_str.replace("Z", "+00:00")).date()
        return (date.today() - d).days
    except (ValueError, TypeError):
        return None


def max_date(*date_strs) -> str | None:
    """Return the most recent non-null date string from a list."""
    valid = []
    for ds in date_strs:
        if ds:
            try:
                valid.append(datetime.fromisoformat(ds.replace("Z", "+00:00")))
            except (ValueError, TypeError):
                pass
    return max(valid).isoformat() if valid else None


def pipeline_name_for_id(pipeline_id: int) -> str | None:
    for name, pid in PIPELINE_IDS.items():
        if pid == pipeline_id:
            return name
    return None


def stage_order_for_id(stage_id: int) -> int | None:
    """Look up the 0-based stage order from config. Falls back to None."""
    for pipeline_stages in STAGE_MAP.values():
        for order, (key, sid) in enumerate(pipeline_stages.items()):
            if sid == stage_id:
                return order
    return None


def get_thresholds(pipeline_id: int, stage_order: int) -> dict:
    """Return {no_signal, stage_unchanged, close_date_required} for a deal's stage."""
    pname = pipeline_name_for_id(pipeline_id)
    if not pname:
        return {"no_signal": 21, "stage_unchanged": 30, "close_date_required": False}
    t = THRESHOLDS.get(pname, {})
    if t == "mirror_blink":
        t = THRESHOLDS["blink"]
    stage_t = t.get(str(stage_order), t.get("0", {}))
    return {
        "no_signal":           stage_t.get("no_signal", 21),
        "stage_unchanged":     stage_t.get("stage_unchanged", 30),
        "close_date_required": stage_t.get("close_date_required", False),
    }


# ── Stall flag computation ────────────────────────────────────────────────────

def compute_stall_signals(row: sqlite3.Row) -> dict:
    """
    Returns a dict of individual signal booleans and a composite severity score.
    Uses LMSD (Last Meaningful Signal Date) as primary recency measure.
    """
    pipeline_id    = row["pipeline_id"]
    stage_id       = row["stage_id"]
    stage_order    = stage_order_for_id(stage_id) or 0
    thresh         = get_thresholds(pipeline_id, stage_order)

    # ── Last Meaningful Signal Date ──────────────────────────────────────────
    lmsd = max_date(
        row["last_activity_date"],
        row["last_outgoing_mail_time"],
        row["last_incoming_mail_time"],
    )
    days_since_signal  = days_since(lmsd)
    days_in_stage      = days_since(row["stage_change_time"])
    days_until_close   = (
        (datetime.fromisoformat(row["expected_close_date"]).date() - date.today()).days
        if row["expected_close_date"] else None
    )

    # ── Individual signals ───────────────────────────────────────────────────
    no_signal = (
        days_since_signal is None or
        days_since_signal > thresh["no_signal"]
    )
    no_next_activity = (
        (row["undone_activities_count"] or 0) == 0 and
        stage_order >= 1
    )
    stage_unchanged = (
        days_in_stage is not None and
        thresh["stage_unchanged"] is not None and
        days_in_stage > thresh["stage_unchanged"]
    )
    # Proposal ghost: in proposal stage and hasn't moved in 14 days
    proposal_stage_ids = [
        STAGE_MAP.get("blink", {}).get("3_proposal"),
        STAGE_MAP.get("synergy", {}).get("3_proposal"),
        STAGE_MAP.get("insight_space", {}).get("3_proposal"),
    ]
    proposal_ghost = (
        stage_id in [s for s in proposal_stage_ids if s] and
        days_in_stage is not None and
        days_in_stage > 14
    )
    # One-sided: outbound emails but no inbound in 21 days
    out_days = days_since(row["last_outgoing_mail_time"])
    in_days  = days_since(row["last_incoming_mail_time"])
    one_sided = (
        row["last_outgoing_mail_time"] is not None and
        out_days is not None and out_days <= 14 and
        (row["last_incoming_mail_time"] is None or (in_days is not None and in_days > 21))
    )
    # Committed with no close date
    committed_stage_ids = [
        STAGE_MAP.get("blink", {}).get("5_committed"),
        STAGE_MAP.get("synergy", {}).get("5_committed"),
        STAGE_MAP.get("insight_space", {}).get("5_committed"),
    ]
    committed_no_close = (
        stage_id in [s for s in committed_stage_ids if s] and
        not row["expected_close_date"]
    )
    # Close date in the past
    close_overdue = (
        days_until_close is not None and
        days_until_close < 0
    )
    # Low probability + no recent signal
    low_prob_dark = (
        row["position_to_win"] == "Low probability to win" and
        (days_since_signal is None or days_since_signal > 14)
    )

    signals = {
        "no_signal":          no_signal,
        "no_next_activity":   no_next_activity,
        "stage_unchanged":    stage_unchanged,
        "proposal_ghost":     proposal_ghost,
        "one_sided":          one_sided,
        "committed_no_close": committed_no_close,
        "close_overdue":      close_overdue,
        "low_prob_dark":      low_prob_dark,
    }

    # Single-signal exceptions that are always Critical regardless of score
    always_critical = close_overdue or committed_no_close or proposal_ghost

    score = sum(1 for v in signals.values() if v)
    if always_critical and score > 0:
        severity = "Critical"
    elif score >= 3:
        severity = "Critical"
    elif score == 2:
        severity = "At-Risk"
    elif score == 1:
        severity = "Watch"
    else:
        severity = "Healthy"

    triggered = [k for k, v in signals.items() if v]

    return {
        "lmsd":                    lmsd,
        "days_since_signal":       days_since_signal,
        "days_in_stage":           days_in_stage,
        "days_until_close":        days_until_close,
        "lmsd_source":             _lmsd_source(row, lmsd),
        **{f"signal_{k}": v for k, v in signals.items()},
        "stall_score":             score,
        "stall_severity":          severity,
        "stall_signals_triggered": ", ".join(triggered) if triggered else "none",
        "is_stalling":             score > 0,
    }


def _lmsd_source(row: sqlite3.Row, lmsd: str | None) -> str | None:
    """Which field produced the LMSD value."""
    if not lmsd:
        return None
    candidates = {
        "activity":       row["last_activity_date"],
        "outbound_email": row["last_outgoing_mail_time"],
        "inbound_email":  row["last_incoming_mail_time"],
    }
    for label, val in candidates.items():
        if val and val[:19] == lmsd[:19]:
            return label
    return "activity"


# ── Engagement segment ────────────────────────────────────────────────────────

def compute_engagement_segment(row: sqlite3.Row) -> str:
    out_days = days_since(row["last_outgoing_mail_time"])
    in_days  = days_since(row["last_incoming_mail_time"])
    act_days = days_since(row["last_activity_date"])

    lmsd = max_date(
        row["last_activity_date"],
        row["last_outgoing_mail_time"],
        row["last_incoming_mail_time"],
    )
    lmsd_days = days_since(lmsd)

    no_outbound = row["last_outgoing_mail_time"] is None
    no_inbound  = row["last_incoming_mail_time"] is None
    no_activity = row["done_activities_count"] == 0 if row["done_activities_count"] is not None else True

    if lmsd is None:
        return "No Signal"
    if lmsd_days is not None and lmsd_days > 21:
        return "Going Dark"
    # Both email directions active
    if (out_days is not None and out_days <= 14 and
            in_days is not None and in_days <= 14):
        return "Active Two-Way"
    # Outbound but no inbound reply recently
    if out_days is not None and out_days <= 14 and (no_inbound or (in_days and in_days > 14)):
        if no_inbound or (in_days and in_days > 21):
            return "One-Sided (Stalling)"
        return "Active Outbound"
    # Activities logged but no email sync
    if not no_activity and no_outbound and no_inbound:
        return "Activity Only"
    return "Active Outbound"


# ── Commercial fit ────────────────────────────────────────────────────────────

def compute_rate_realisation(row: sqlite3.Row) -> float | None:
    rph = row["rph_sold"]
    ptc = row["price_to_client"]
    if rph and ptc and float(rph) > 0:
        return round(float(ptc) / float(rph) * 100, 1)
    return None


# ── Stage history (from changelog) ───────────────────────────────────────────

def build_stage_history(conn: sqlite3.Connection):
    """
    Derive deal_stage_history from the changelog table.
    Each row = one deal's time in one stage: deal_id, stage_id, entered_at, exited_at, dwell_days.
    """
    log.info("Building stage history from changelog...")
    conn.execute("DROP TABLE IF EXISTS deal_stage_history")
    conn.execute("""
        CREATE TABLE deal_stage_history (
            history_id   INTEGER PRIMARY KEY AUTOINCREMENT,
            deal_id      INTEGER NOT NULL,
            stage_id     INTEGER,
            entered_at   TEXT,
            exited_at    TEXT,
            dwell_days   REAL,
            is_current   INTEGER DEFAULT 0
        )
    """)

    # Get all stage change events ordered by time
    rows = conn.execute("""
        SELECT deal_id, new_value AS stage_id, changed_at
        FROM deal_changelog
        WHERE field_key = 'stage_id'
        ORDER BY deal_id, changed_at
    """).fetchall()

    # Group by deal
    from itertools import groupby
    groups = groupby(rows, key=lambda r: r["deal_id"])

    history_rows = []
    for deal_id, events in groups:
        events = list(events)
        for i, ev in enumerate(events):
            entered = ev["changed_at"]
            exited  = events[i + 1]["changed_at"] if i + 1 < len(events) else None
            dwell   = None
            if entered and exited:
                try:
                    d_in  = datetime.fromisoformat(entered.replace("Z", "+00:00"))
                    d_out = datetime.fromisoformat(exited.replace("Z", "+00:00"))
                    dwell = round((d_out - d_in).total_seconds() / 86400, 2)
                except ValueError:
                    pass
            history_rows.append((
                deal_id,
                ev["stage_id"],
                entered,
                exited,
                dwell,
                1 if exited is None else 0,
            ))

    conn.executemany("""
        INSERT INTO deal_stage_history
        (deal_id, stage_id, entered_at, exited_at, dwell_days, is_current)
        VALUES (?,?,?,?,?,?)
    """, history_rows)
    conn.commit()
    log.info(f"  Stage history: {len(history_rows)} stage entries for {len(set(r[0] for r in history_rows))} deals")


# ── Main transform ────────────────────────────────────────────────────────────

def build_transformed_deals(conn: sqlite3.Connection):
    """
    Creates deals_transformed table: one row per deal per snapshot,
    enriched with all derived fields.
    """
    log.info("Building deals_transformed...")
    conn.execute("DROP TABLE IF EXISTS deals_transformed")
    conn.execute("""
        CREATE TABLE deals_transformed AS
        SELECT * FROM deals_snapshot WHERE 1=0
    """)
    # Add derived columns
    for col, typ in [
        ("lmsd", "TEXT"),
        ("lmsd_source", "TEXT"),
        ("days_since_signal", "INTEGER"),
        ("days_in_stage", "INTEGER"),
        ("days_until_close", "INTEGER"),
        ("engagement_segment", "TEXT"),
        ("rate_realisation_pct", "REAL"),
        ("is_stalling", "INTEGER"),
        ("stall_score", "INTEGER"),
        ("stall_severity", "TEXT"),
        ("stall_signals_triggered", "TEXT"),
        ("signal_no_signal", "INTEGER"),
        ("signal_no_next_activity", "INTEGER"),
        ("signal_stage_unchanged", "INTEGER"),
        ("signal_proposal_ghost", "INTEGER"),
        ("signal_one_sided", "INTEGER"),
        ("signal_committed_no_close", "INTEGER"),
        ("signal_close_overdue", "INTEGER"),
        ("signal_low_prob_dark", "INTEGER"),
    ]:
        try:
            conn.execute(f"ALTER TABLE deals_transformed ADD COLUMN {col} {typ}")
        except sqlite3.OperationalError:
            pass

    # Repopulate from snapshot
    conn.execute("DELETE FROM deals_transformed")

    # Get the latest snapshot per deal
    snapshots = conn.execute("""
        SELECT s.*
        FROM deals_snapshot s
        INNER JOIN (
            SELECT deal_id, MAX(extracted_at) AS max_ext
            FROM deals_snapshot
            GROUP BY deal_id
        ) latest ON s.deal_id = latest.deal_id AND s.extracted_at = latest.max_ext
    """).fetchall()

    log.info(f"  Transforming {len(snapshots)} deals...")
    for row in snapshots:
        stall  = compute_stall_signals(row)
        seg    = compute_engagement_segment(row) if row["status"] == "open" else None
        rr     = compute_rate_realisation(row)

        conn.execute("""
            INSERT INTO deals_transformed
            SELECT *,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
            FROM deals_snapshot WHERE snapshot_id = ?
        """, (
            stall["lmsd"],
            stall["lmsd_source"],
            stall["days_since_signal"],
            stall["days_in_stage"],
            stall["days_until_close"],
            seg,
            rr,
            1 if stall["is_stalling"] else 0,
            stall["stall_score"],
            stall["stall_severity"],
            stall["stall_signals_triggered"],
            1 if stall["signal_no_signal"] else 0,
            1 if stall["signal_no_next_activity"] else 0,
            1 if stall["signal_stage_unchanged"] else 0,
            1 if stall["signal_proposal_ghost"] else 0,
            1 if stall["signal_one_sided"] else 0,
            1 if stall["signal_committed_no_close"] else 0,
            1 if stall["signal_close_overdue"] else 0,
            1 if stall["signal_low_prob_dark"] else 0,
            row["snapshot_id"],
        ))

    conn.commit()
    log.info(f"  deals_transformed built: {len(snapshots)} rows")

    # Summary counts
    stalling = conn.execute(
        "SELECT COUNT(*) FROM deals_transformed WHERE is_stalling=1 AND status='open'"
    ).fetchone()[0]
    critical = conn.execute(
        "SELECT COUNT(*) FROM deals_transformed WHERE stall_severity='Critical' AND status='open'"
    ).fetchone()[0]
    log.info(f"  Open stalling: {stalling} total / {critical} critical")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    conn = get_conn()
    build_stage_history(conn)
    build_transformed_deals(conn)
    log.info("=== Transform complete ===")


if __name__ == "__main__":
    main()
