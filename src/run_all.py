"""
run_all.py — Run the full ETL pipeline in sequence
Usage:
    python src/run_all.py                    # incremental (default)
    python src/run_all.py --mode full        # first-time full extraction
    python src/run_all.py --mode snapshot    # fast open-pipeline snapshot only
"""

import argparse
import logging
import os
import sys
from pathlib import Path

# Load .env file if present (so PIPEDRIVE_API_TOKEN is set automatically)
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
except ImportError:
    pass  # dotenv not installed — rely on environment variable

sys.path.insert(0, str(Path(__file__).parent))
import extract
import transform
import load

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Run full Pipedrive ETL pipeline")
    parser.add_argument("--mode", choices=["full", "incremental", "snapshot"],
                        default="incremental")
    parser.add_argument("--skip-extract", action="store_true",
                        help="Skip API extraction and re-run only transform + load against existing DB")
    parser.add_argument("--token", help="Pipedrive API token (or set PIPEDRIVE_API_TOKEN env var)")
    args = parser.parse_args()

    if args.skip_extract:
        log.info("Skipping extraction — re-running transform + load against existing DB")
        conn = extract.get_conn()
    else:
        api_token = args.token or os.environ.get("PIPEDRIVE_API_TOKEN")
        if not api_token:
            log.error("No API token. Set PIPEDRIVE_API_TOKEN env var or pass --token")
            sys.exit(1)

        log.info(f"Starting ETL pipeline — mode: {args.mode}")

        # 1. Extract
        client = extract.PipedriveClient(api_token)
        conn   = extract.get_conn()
        extract.init_db(conn)
        from datetime import datetime, timezone
        started_at = datetime.now(timezone.utc).isoformat()
        run_id = conn.execute(
            "INSERT INTO extraction_runs (started_at, mode) VALUES (?,?)",
            (started_at, args.mode)
        ).lastrowid
        conn.commit()

        try:
            if args.mode == "full":
                stats = extract.run_full(client, conn)
            elif args.mode == "snapshot":
                stats = extract.run_snapshot(client, conn)
            else:
                stats = extract.run_incremental(client, conn)

            finished_at = datetime.now(timezone.utc).isoformat()
            conn.execute(
                "UPDATE extraction_runs SET finished_at=?, deals_fetched=?, changelogs_fetched=?, status='completed' WHERE run_id=?",
                (finished_at, stats["deals"], stats["changelogs"], run_id)
            )
            conn.commit()
        except Exception as e:
            conn.execute("UPDATE extraction_runs SET status='failed', notes=? WHERE run_id=?",
                         (str(e), run_id))
            conn.commit()
            raise

    # 2. Transform
    transform.build_stage_history(conn)
    transform.build_transformed_deals(conn)
    transform.resolve_person_labels(conn)

    # 3. Load
    conn2 = load.get_conn()
    load.export_pipeline_health(conn2)
    load.export_stalling_deals(conn2)
    load.export_won_lost(conn2)
    load.export_pre_sales(conn2)
    load.export_engagement(conn2)
    load.export_velocity(conn2)
    load.export_commercial_fit(conn2)
    load.export_forecast_confidence(conn2)
    load.export_owner_goals(conn2)
    load.export_account_coverage(conn2)
    load.export_account_coverage_detail(conn2)
    load.export_ap_buyer_targets(conn2)
    load.export_reporting_tag_detail(conn2)
    load.export_elevated_buyer_detail(conn2)
    load.export_metadata(conn2)
    load.write_manifest()

    log.info("=== ETL pipeline complete ===")


if __name__ == "__main__":
    main()
