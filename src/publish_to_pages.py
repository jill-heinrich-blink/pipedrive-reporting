"""
publish_to_pages.py — Copy the latest beta exports into docs/data for GitHub Pages

The dashboard at docs/index.html fetches its data from docs/data/*.csv at page
load. Those files are a manual copy of data/exports/*.csv, not a live link —
GitHub Pages only serves whatever's committed, so "refresh" here means:

    1. python src/run_all.py            (updates data/exports/*.csv)
    2. python src/publish_to_pages.py   (this script — copies into docs/data)
    3. git add docs/data && git commit -m "Refresh beta dashboard data" && git push

Run after run_all.py, before pushing.
"""

import shutil
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
EXPORT_DIR = BASE_DIR / "data" / "exports"
PAGES_DATA_DIR = BASE_DIR / "docs" / "data"

FILES = [
    "09_owner_goals.csv",
    "10_reporting_tag_detail.csv",
    "11_elevated_buyer_detail.csv",
    "12_account_coverage.csv",
]


def main():
    PAGES_DATA_DIR.mkdir(parents=True, exist_ok=True)
    copied = []
    missing = []
    for name in FILES:
        src = EXPORT_DIR / name
        if not src.exists():
            missing.append(name)
            continue
        shutil.copy2(src, PAGES_DATA_DIR / name)
        copied.append(name)

    print(f"Copied {len(copied)} file(s) to {PAGES_DATA_DIR}:")
    for name in copied:
        print(f"  - {name}")
    if missing:
        print(f"\nMissing (not copied — did you run src/run_all.py first?):")
        for name in missing:
            print(f"  - {name}")
    print("\nNext: git add docs/data && git commit -m 'Refresh beta dashboard data' && git push")


if __name__ == "__main__":
    main()
