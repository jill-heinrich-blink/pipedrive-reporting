"""
extract.py — Pipedrive API extraction layer
Blink / Mphasis  |  pipedrive-reporting

Pulls:
  - Open deals (snapshot)
  - Won / lost deals (incremental by update_time)
  - Deal changelog (stage history + field history)
  - Stage, pipeline, user dimension tables

Stores everything in a local SQLite database (data/pipedrive.db)
and writes flat CSV snapshots to data/ for Looker Studio / Sheets import.

Usage:
    python src/extract.py --mode full        # first run — fetches everything
    python src/extract.py --mode incremental # daily run — only new/updated deals
    python src/extract.py --mode snapshot    # snapshot open pipeline only (fast)
"""

import os
import sys
import json
import time
import logging
import sqlite3
import argparse
import requests
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────

BASE_DIR   = Path(__file__).resolve().parent.parent
CONFIG_DIR = BASE_DIR / "config"
DATA_DIR   = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

DB_PATH    = DATA_DIR / "pipedrive.db"

with open(CONFIG_DIR / "fields.json") as f:
    FIELDS = json.load(f)

CUSTOM = FIELDS["custom_fields"]
PERSON_FIELDS = FIELDS["person_fields"]

# All custom field keys we want to include on every deal fetch
CUSTOM_KEYS = list(CUSTOM.values())

API_BASE  = "https://api.pipedrive.com/api"
PAGE_SIZE = 500   # max for v2 deals endpoint

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── API client ────────────────────────────────────────────────────────────────

class PipedriveClient:
    def __init__(self, api_token: str):
        self.token   = api_token
        self.session = requests.Session()
        self.session.params = {"api_token": api_token}
        self._remaining_tokens = None

    def _get(self, url: str, params: dict = None, retries: int = 3) -> dict:
        params = params or {}
        for attempt in range(retries):
            try:
                r = self.session.get(url, params=params, timeout=30)
                # Track token budget from response headers
                if "x-daily-requests-left" in r.headers:
                    self._remaining_tokens = r.headers["x-daily-requests-left"]
                if r.status_code == 429:
                    wait = int(r.headers.get("Retry-After", 10))
                    log.warning(f"Rate limited — waiting {wait}s")
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                return r.json()
            except requests.RequestException as e:
                if attempt == retries - 1:
                    raise
                log.warning(f"Request failed ({e}), retry {attempt + 1}/{retries}")
                time.sleep(2 ** attempt)

    def get_all_pages(self, url: str, params: dict = None) -> list:
        """Paginate a v2 cursor-based endpoint, return all items."""
        params  = dict(params or {})
        params["limit"] = PAGE_SIZE
        items   = []
        cursor  = None
        page    = 0
        while True:
            if cursor:
                params["cursor"] = cursor
            data = self._get(url, params)
            batch = data.get("data") or []
            items.extend(batch)
            page += 1
            log.debug(f"  page {page}: {len(batch)} items (total {len(items)})")
            next_cursor = (data.get("additional_data") or {}).get("next_cursor")
            if not next_cursor or not batch:
                break
            cursor = next_cursor
        return items

    def get_v1_paginated(self, url: str, params: dict = None) -> list:
        """Paginate a v1 start/limit endpoint, return all items."""
        params  = dict(params or {})
        params["limit"] = 500
        items   = []
        start   = 0
        while True:
            params["start"] = start
            data = self._get(url, params)
            batch = data.get("data") or []
            if not batch:
                break
            items.extend(batch)
            more = (data.get("additional_data") or {}).get("pagination", {}).get("more_items_in_collection", False)
            if not more:
                break
            start += len(batch)
        return items

    # ── Dimension tables ─────────────────────────────────────────────────────

    def fetch_pipelines(self) -> list:
        data = self._get(f"{API_BASE}/v2/pipelines")
        return data.get("data") or []

    def fetch_stages(self) -> list:
        return self.get_v1_paginated(f"{API_BASE}/v1/stages")

    def fetch_users(self) -> list:
        return self.get_v1_paginated(f"{API_BASE}/v1/users")

    def fetch_deal_fields(self) -> list:
        """Fetch all deal field definitions (for option label lookups)."""
        return self.get_v1_paginated(f"{API_BASE}/v1/dealFields")

    def fetch_person_fields(self) -> list:
        """Fetch all person field definitions (for Label / Reporting Tag option lookups)."""
        return self.get_v1_paginated(f"{API_BASE}/v1/personFields")

    def fetch_persons(self) -> list:
        """
        Bulk paginated pull of ALL persons — one call sequence total, not one
        call per person. Label and custom fields (incl. Reporting Tag) come
        back on the standard v1 list response with no extra params needed.
        """
        log.info("Fetching persons...")
        persons = self.get_v1_paginated(f"{API_BASE}/v1/persons")
        log.info(f"  → {len(persons)} persons")
        return persons

    # ── Deals ─────────────────────────────────────────────────────────────────

    def fetch_deals(self, status: str = "open", updated_after: str = None) -> list:
        """
        status: 'open' | 'won' | 'lost' | 'all_not_deleted'
        updated_after: ISO datetime string — only return deals updated after this
        """
        params = {"status": status}
        if updated_after:
            params["update_time"] = updated_after
        log.info(f"Fetching {status} deals...")
        deals = self.get_all_pages(f"{API_BASE}/v2/deals", params)
        log.info(f"  → {len(deals)} {status} deals")
        return deals

    def fetch_archived_deals(self) -> list:
        log.info("Fetching archived deals...")
        deals = self.get_all_pages(f"{API_BASE}/v2/deals/archived")
        log.info(f"  → {len(deals)} archived deals")
        return deals

    # ── Changelog ─────────────────────────────────────────────────────────────

    def fetch_deal_changelog(self, deal_id: int) -> list:
        """
        Returns all changelog entries for a deal.
        Filters to field changes only (not activity/note entries).
        """
        url    = f"{API_BASE}/v1/deals/{deal_id}/changelog"
        params = {"limit": 500}
        items  = []
        cursor = None
        while True:
            if cursor:
                params["cursor"] = cursor
            data = self._get(url, params)
            batch = data.get("data") or []
            items.extend(batch)
            next_cursor = (data.get("additional_data") or {}).get("next_cursor")
            if not next_cursor or not batch:
                break
            cursor = next_cursor
        return items


# ── Database ──────────────────────────────────────────────────────────────────

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(conn: sqlite3.Connection):
    conn.executescript("""
    -- Dimension tables
    CREATE TABLE IF NOT EXISTS dim_pipelines (
        pipeline_id   INTEGER PRIMARY KEY,
        name          TEXT,
        updated_at    TEXT
    );

    CREATE TABLE IF NOT EXISTS dim_stages (
        stage_id      INTEGER PRIMARY KEY,
        pipeline_id   INTEGER,
        name          TEXT,
        stage_order   INTEGER,
        rotten_flag   INTEGER,
        rotten_days   INTEGER,
        updated_at    TEXT
    );

    CREATE TABLE IF NOT EXISTS dim_users (
        user_id       INTEGER PRIMARY KEY,
        name          TEXT,
        email         TEXT,
        updated_at    TEXT
    );

    -- Persons — overwritten each run (latest Label / Reporting Tag state only,
    -- not historized like deals_snapshot). If tag-adoption trend is ever
    -- needed, this would need the same append-only treatment as deals.
    CREATE TABLE IF NOT EXISTS dim_persons (
        person_id       INTEGER PRIMARY KEY,
        name            TEXT,
        label           TEXT,
        reporting_tag   TEXT,
        updated_at      TEXT
    );

    -- Option label lookup for dropdown custom fields (option_id → label)
    CREATE TABLE IF NOT EXISTS dim_field_options (
        field_key     TEXT NOT NULL,
        field_name    TEXT,
        option_id     INTEGER NOT NULL,
        option_label  TEXT,
        updated_at    TEXT,
        PRIMARY KEY (field_key, option_id)
    );

    -- Deal snapshots (append-only — one row per deal per extraction run)
    CREATE TABLE IF NOT EXISTS deals_snapshot (
        snapshot_id         INTEGER PRIMARY KEY AUTOINCREMENT,
        extracted_at        TEXT NOT NULL,
        deal_id             INTEGER NOT NULL,
        title               TEXT,
        status              TEXT,
        pipeline_id         INTEGER,
        stage_id            INTEGER,
        owner_id            INTEGER,
        person_id           INTEGER,
        creator_id          INTEGER,
        value               REAL,
        weighted_value      REAL,
        currency            TEXT,
        probability         INTEGER,
        expected_close_date TEXT,
        add_time            TEXT,
        update_time         TEXT,
        stage_change_time   TEXT,
        won_time            TEXT,
        lost_time           TEXT,
        close_time          TEXT,
        lost_reason         TEXT,
        last_activity_date  TEXT,
        last_incoming_mail_time TEXT,
        last_outgoing_mail_time TEXT,
        email_messages_count    INTEGER,
        activities_count        INTEGER,
        done_activities_count   INTEGER,
        undone_activities_count INTEGER,
        -- custom fields
        rph_sold                REAL,
        price_to_client         REAL,
        position_to_win         TEXT,
        deal_outreach_tag       TEXT,
        outbound_meeting_date   TEXT,
        touch_type              TEXT,
        disqualification_reason TEXT,
        re_engagement_eligible  TEXT,
        project_type            TEXT,
        service_type            TEXT,
        business_unit           TEXT,
        industry                TEXT,
        deal_type               TEXT,
        contract_type           TEXT,
        rfp                     TEXT,
        has_discount            TEXT,
        right_fit_for_blink     TEXT,
        mphasis_engineering     TEXT,
        mphasis_engineering_value REAL,
        lead_source             TEXT,
        resourcing_label        TEXT,
        is_archived             INTEGER DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS idx_snap_deal_date ON deals_snapshot(deal_id, extracted_at);
    CREATE INDEX IF NOT EXISTS idx_snap_status    ON deals_snapshot(status, extracted_at);

    -- Changelog: one row per field change event
    CREATE TABLE IF NOT EXISTS deal_changelog (
        change_id       INTEGER PRIMARY KEY AUTOINCREMENT,
        deal_id         INTEGER NOT NULL,
        field_key       TEXT,
        field_name      TEXT,
        old_value       TEXT,
        new_value       TEXT,
        changed_at      TEXT,
        changed_by_id   INTEGER,
        is_bulk_update  INTEGER DEFAULT 0,
        fetched_at      TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_cl_deal       ON deal_changelog(deal_id);
    CREATE INDEX IF NOT EXISTS idx_cl_field_time ON deal_changelog(field_key, changed_at);

    -- Track which deals have had their changelog fetched
    CREATE TABLE IF NOT EXISTS changelog_fetch_log (
        deal_id       INTEGER PRIMARY KEY,
        last_fetched  TEXT NOT NULL,
        change_count  INTEGER DEFAULT 0
    );

    -- Extraction run log
    CREATE TABLE IF NOT EXISTS extraction_runs (
        run_id        INTEGER PRIMARY KEY AUTOINCREMENT,
        started_at    TEXT NOT NULL,
        finished_at   TEXT,
        mode          TEXT,
        deals_fetched INTEGER DEFAULT 0,
        changelogs_fetched INTEGER DEFAULT 0,
        status        TEXT DEFAULT 'running',
        notes         TEXT
    );
    """)
    conn.commit()

    # Migration: person_id was added after this DB was first created (2026-08-11).
    # CREATE TABLE IF NOT EXISTS above doesn't touch existing tables, so add it
    # explicitly. Existing historical rows get NULL person_id — expected, since
    # that data was never captured before this column existed.
    try:
        conn.execute("ALTER TABLE deals_snapshot ADD COLUMN person_id INTEGER")
        conn.commit()
        log.info("  Migration: added person_id column to deals_snapshot")
    except sqlite3.OperationalError:
        pass  # already exists

    log.info("Database schema initialised")


# ── Upsert helpers ────────────────────────────────────────────────────────────

def upsert_pipelines(conn, pipelines: list):
    now = datetime.now(timezone.utc).isoformat()
    conn.executemany(
        "INSERT OR REPLACE INTO dim_pipelines (pipeline_id, name, updated_at) VALUES (?,?,?)",
        [(p["id"], p["name"], now) for p in pipelines]
    )
    conn.commit()


def upsert_stages(conn, stages: list):
    now = datetime.now(timezone.utc).isoformat()
    conn.executemany(
        """INSERT OR REPLACE INTO dim_stages
           (stage_id, pipeline_id, name, stage_order, rotten_flag, rotten_days, updated_at)
           VALUES (?,?,?,?,?,?,?)""",
        [(s["id"], s["pipeline_id"], s["name"], s["order_nr"],
          1 if s.get("rotten_flag") else 0, s.get("rotten_days"), now)
         for s in stages]
    )
    conn.commit()


def upsert_field_options(conn, deal_fields: list):
    """Store option ID → label mappings for all dropdown/enum custom fields."""
    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for field in deal_fields:
        key  = field.get("key")
        name = field.get("name")
        options = field.get("options") or []
        for opt in options:
            rows.append((key, name, opt.get("id"), opt.get("label"), now))
    if rows:
        conn.executemany(
            """INSERT OR REPLACE INTO dim_field_options
               (field_key, field_name, option_id, option_label, updated_at)
               VALUES (?,?,?,?,?)""",
            rows
        )
        conn.commit()
        log.info(f"  Field options: {len(rows)} option labels stored")


def upsert_users(conn, users: list):
    now = datetime.now(timezone.utc).isoformat()
    conn.executemany(
        "INSERT OR REPLACE INTO dim_users (user_id, name, email, updated_at) VALUES (?,?,?,?)",
        [(u["id"], u["name"], u.get("email"), now) for u in users]
    )
    conn.commit()


def upsert_persons(conn, persons: list):
    """
    Overwrites dim_persons each run — latest Label / Reporting Tag state only.
    Raw option IDs are stored here (matching the deal custom-field pattern);
    transform.py resolves them to human-readable labels via dim_field_options.
    """
    now = datetime.now(timezone.utc).isoformat()
    label_key = PERSON_FIELDS["label"]
    tag_key   = PERSON_FIELDS["reporting_tag"]
    rows = []
    for p in persons:
        label_raw = p.get(label_key)
        tag_raw   = p.get(tag_key)
        # 'set' type custom fields (like Reporting Tag) can come back as a
        # comma-separated string of option IDs if multiple are selected.
        if isinstance(tag_raw, list):
            tag_raw = ",".join(str(v) for v in tag_raw) if tag_raw else None
        rows.append((p["id"], p.get("name"), label_raw, tag_raw, now))
    conn.execute("DELETE FROM dim_persons")
    conn.executemany(
        "INSERT OR REPLACE INTO dim_persons (person_id, name, label, reporting_tag, updated_at) VALUES (?,?,?,?,?)",
        rows
    )
    conn.commit()
    log.info(f"  Persons: {len(rows)} stored (label/reporting_tag latest state)")


def _cf(deal: dict, key: str):
    """Safely retrieve a custom field value from a deal dict.
    Pipedrive API v2 nests custom fields under deal['custom_fields'].
    Coerces unsupported types so sqlite3 never sees a dict or list.
    """
    custom = deal.get("custom_fields")
    if isinstance(custom, dict):
        val = custom.get(key)
    else:
        val = deal.get(key)  # fallback for v1-style flat responses

    # Monetary fields come back as {"amount": X, "currency": "Y"}
    if isinstance(val, dict):
        return val.get("amount")

    # Multi-select fields come back as a list of option IDs
    if isinstance(val, list):
        return ",".join(str(v) for v in val) if val else None

    return val


def insert_deal_snapshot(conn, deal: dict, extracted_at: str):
    c = CUSTOM
    conn.execute("""
        INSERT INTO deals_snapshot (
            extracted_at, deal_id, title, status, pipeline_id, stage_id,
            owner_id, person_id, creator_id, value, weighted_value, currency, probability,
            expected_close_date, add_time, update_time, stage_change_time,
            won_time, lost_time, close_time, lost_reason,
            last_activity_date, last_incoming_mail_time, last_outgoing_mail_time,
            email_messages_count, activities_count, done_activities_count,
            undone_activities_count,
            rph_sold, price_to_client, position_to_win, deal_outreach_tag,
            outbound_meeting_date, touch_type, disqualification_reason,
            re_engagement_eligible, project_type, service_type, business_unit,
            industry, deal_type, contract_type, rfp, has_discount,
            right_fit_for_blink, mphasis_engineering, mphasis_engineering_value,
            lead_source, resourcing_label, is_archived
        ) VALUES (
            ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
            ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
        )
    """, (
        extracted_at,
        deal.get("id"),
        deal.get("title"),
        deal.get("status"),
        deal.get("pipeline_id") or (deal.get("pipeline") if isinstance(deal.get("pipeline"), int) else None),
        deal.get("stage_id"),
        deal.get("owner_id") or ((deal.get("user_id") or {}).get("id") if isinstance(deal.get("user_id"), dict) else deal.get("user_id")),
        (deal.get("person_id") or {}).get("value") if isinstance(deal.get("person_id"), dict) else deal.get("person_id"),
        (deal.get("creator_user_id") or {}).get("id") if isinstance(deal.get("creator_user_id"), dict) else deal.get("creator_user_id"),
        deal.get("value"),
        # v2 doesn't return weighted_value — calculate from value × probability
        (deal.get("value") or 0) * (deal.get("probability") or 0) / 100
        if deal.get("value") is not None else None,
        deal.get("currency"),
        deal.get("probability"),
        deal.get("expected_close_date"),
        deal.get("add_time"),
        deal.get("update_time"),
        deal.get("stage_change_time"),
        deal.get("won_time"),
        deal.get("lost_time"),
        deal.get("close_time"),
        deal.get("lost_reason"),
        deal.get("last_activity_date"),
        deal.get("last_incoming_mail_time"),
        deal.get("last_outgoing_mail_time"),
        deal.get("email_messages_count"),
        deal.get("activities_count"),
        deal.get("done_activities_count"),
        deal.get("undone_activities_count"),
        _cf(deal, c["rph_sold"]),
        _cf(deal, c["price_to_client"]),
        _cf(deal, c["position_to_win"]),
        _cf(deal, c["deal_outreach_tag"]),
        _cf(deal, c["outbound_meeting_date"]),
        _cf(deal, c["touch_type"]),
        _cf(deal, c["disqualification_reason"]),
        _cf(deal, c["re_engagement_eligible"]),
        _cf(deal, c["project_type"]),
        _cf(deal, c["service_type"]),
        _cf(deal, c["business_unit"]),
        _cf(deal, c["industry"]),
        _cf(deal, c["deal_type"]),
        _cf(deal, c["contract_type"]),
        _cf(deal, c["rfp"]),
        _cf(deal, c["has_discount"]),
        _cf(deal, c["right_fit_for_blink"]),
        _cf(deal, c["mphasis_engineering"]),
        _cf(deal, c["mphasis_engineering_value"]),
        _cf(deal, c["lead_source"]),
        _cf(deal, c["resourcing_label"]),
        1 if deal.get("is_archived") else 0,
    ))


def insert_changelog(conn, deal_id: int, entries: list, fetched_at: str):
    rows = []
    for e in entries:
        rows.append((
            deal_id,
            e.get("field_key"),
            e.get("field_name"),
            str(e.get("old_value")) if e.get("old_value") is not None else None,
            str(e.get("new_value")) if e.get("new_value") is not None else None,
            e.get("time") or e.get("log_time"),
            (e.get("user_id") or {}).get("id") if isinstance(e.get("user_id"), dict) else e.get("user_id"),
            1 if e.get("is_bulk_update") else 0,
            fetched_at,
        ))
    if rows:
        conn.executemany("""
            INSERT INTO deal_changelog
            (deal_id, field_key, field_name, old_value, new_value,
             changed_at, changed_by_id, is_bulk_update, fetched_at)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, rows)
    conn.execute("""
        INSERT OR REPLACE INTO changelog_fetch_log (deal_id, last_fetched, change_count)
        VALUES (?, ?, ?)
    """, (deal_id, fetched_at, len(rows)))
    conn.commit()


# ── Changelog fetch logic ─────────────────────────────────────────────────────

def needs_changelog_refresh(conn, deal_id: int, deal_update_time: str) -> bool:
    """
    Only re-fetch a deal's changelog if:
      1. We've never fetched it before, OR
      2. The deal's update_time is more recent than our last fetch
    """
    row = conn.execute(
        "SELECT last_fetched FROM changelog_fetch_log WHERE deal_id = ?", (deal_id,)
    ).fetchone()
    if not row:
        return True
    last = row["last_fetched"]
    return (deal_update_time or "") > last


def fetch_changelogs_for_deals(client: PipedriveClient, conn: sqlite3.Connection,
                                deals: list, force: bool = False) -> int:
    """Fetch changelogs for deals that need updating. Returns count fetched."""
    fetched_at = datetime.now(timezone.utc).isoformat()
    count      = 0
    total      = len(deals)
    for i, deal in enumerate(deals, 1):
        deal_id     = deal["id"]
        update_time = deal.get("update_time", "")
        if not force and not needs_changelog_refresh(conn, deal_id, update_time):
            continue
        if i % 50 == 0:
            log.info(f"  Changelog: {i}/{total} deals processed ({count} fetched)")
        try:
            entries = client.fetch_deal_changelog(deal_id)
            insert_changelog(conn, deal_id, entries, fetched_at)
            count += 1
        except Exception as e:
            log.warning(f"  Changelog fetch failed for deal {deal_id}: {e}")
        # Small sleep to stay well within burst limits
        time.sleep(0.05)
    log.info(f"  Changelog: fetched/updated {count} of {total} deals")
    return count


# ── Main extraction modes ─────────────────────────────────────────────────────

def run_full(client: PipedriveClient, conn: sqlite3.Connection) -> dict:
    """Full extraction: all open + won + lost deals + all changelogs."""
    log.info("=== FULL extraction starting ===")
    extracted_at = datetime.now(timezone.utc).isoformat()
    stats = {"deals": 0, "changelogs": 0}

    # Dimensions
    log.info("Fetching dimension tables...")
    upsert_pipelines(conn, client.fetch_pipelines())
    upsert_stages(conn, client.fetch_stages())
    upsert_users(conn, client.fetch_users())
    upsert_field_options(conn, client.fetch_deal_fields())
    upsert_field_options(conn, client.fetch_person_fields())
    upsert_persons(conn, client.fetch_persons())

    # Deals
    all_deals = []
    for status in ("open", "won", "lost"):
        deals = client.fetch_deals(status)
        for d in deals:
            insert_deal_snapshot(conn, d, extracted_at)
        all_deals.extend(deals)
        stats["deals"] += len(deals)

    conn.commit()
    log.info(f"  Inserted {stats['deals']} deal snapshots")

    # Changelogs for all deals
    log.info("Fetching changelogs (this takes a while on first run)...")
    stats["changelogs"] = fetch_changelogs_for_deals(client, conn, all_deals, force=False)

    return stats


def run_incremental(client: PipedriveClient, conn: sqlite3.Connection) -> dict:
    """Incremental: only deals updated since last run."""
    log.info("=== INCREMENTAL extraction starting ===")

    # Find last successful run
    last_run = conn.execute(
        "SELECT finished_at FROM extraction_runs WHERE status='completed' ORDER BY finished_at DESC LIMIT 1"
    ).fetchone()
    cutoff = (last_run["finished_at"] if last_run else None)
    if cutoff:
        # Subtract 1 hour buffer to handle clock skew
        dt = datetime.fromisoformat(cutoff) - timedelta(hours=1)
        cutoff = dt.isoformat()
        log.info(f"  Incremental since: {cutoff}")
    else:
        log.info("  No previous run found — falling back to full extraction")
        return run_full(client, conn)

    extracted_at = datetime.now(timezone.utc).isoformat()
    stats = {"deals": 0, "changelogs": 0}

    # Persons are cheap to bulk-refresh (a handful of paginated calls
    # regardless of headcount) — always refresh latest state, every run.
    upsert_persons(conn, client.fetch_persons())

    all_deals = []
    for status in ("open", "won", "lost"):
        deals = client.fetch_deals(status, updated_after=cutoff)
        for d in deals:
            insert_deal_snapshot(conn, d, extracted_at)
        all_deals.extend(deals)
        stats["deals"] += len(deals)

    conn.commit()
    log.info(f"  Inserted {stats['deals']} updated deal snapshots")

    stats["changelogs"] = fetch_changelogs_for_deals(client, conn, all_deals)
    return stats


def run_snapshot(client: PipedriveClient, conn: sqlite3.Connection) -> dict:
    """Fast snapshot: open deals only, no changelog."""
    log.info("=== SNAPSHOT extraction starting ===")
    extracted_at = datetime.now(timezone.utc).isoformat()
    deals = client.fetch_deals("open")
    for d in deals:
        insert_deal_snapshot(conn, d, extracted_at)
    conn.commit()
    log.info(f"  Inserted {len(deals)} open deal snapshots")
    return {"deals": len(deals), "changelogs": 0}


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Pipedrive data extraction")
    parser.add_argument("--mode", choices=["full", "incremental", "snapshot"],
                        default="incremental")
    parser.add_argument("--token", help="Pipedrive API token (or set PIPEDRIVE_API_TOKEN env var)")
    args = parser.parse_args()

    api_token = args.token or os.environ.get("PIPEDRIVE_API_TOKEN")
    if not api_token:
        log.error("No API token provided. Set PIPEDRIVE_API_TOKEN or pass --token")
        sys.exit(1)

    client = PipedriveClient(api_token)
    conn   = get_conn()
    init_db(conn)

    started_at = datetime.now(timezone.utc).isoformat()
    run_id = conn.execute(
        "INSERT INTO extraction_runs (started_at, mode) VALUES (?,?)",
        (started_at, args.mode)
    ).lastrowid
    conn.commit()

    try:
        if args.mode == "full":
            stats = run_full(client, conn)
        elif args.mode == "incremental":
            stats = run_incremental(client, conn)
        else:
            stats = run_snapshot(client, conn)

        finished_at = datetime.now(timezone.utc).isoformat()
        conn.execute("""
            UPDATE extraction_runs
            SET finished_at=?, deals_fetched=?, changelogs_fetched=?, status='completed'
            WHERE run_id=?
        """, (finished_at, stats["deals"], stats["changelogs"], run_id))
        conn.commit()
        log.info(f"=== Extraction complete: {stats} ===")

    except Exception as e:
        conn.execute(
            "UPDATE extraction_runs SET status='failed', notes=? WHERE run_id=?",
            (str(e), run_id)
        )
        conn.commit()
        log.error(f"Extraction failed: {e}")
        raise


if __name__ == "__main__":
    main()
