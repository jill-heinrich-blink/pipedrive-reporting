"""
diagnose_api.py — Print raw Pipedrive API v2 response for one open deal
Run from the pipedrive-reporting directory:
    python3 diagnose_api.py

Shows exactly what field names and values the API returns,
so we can fix any field mapping issues in extract.py.
"""

import os, json, sys
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass

import requests

token = os.environ.get("PIPEDRIVE_API_TOKEN")
if not token:
    print("ERROR: PIPEDRIVE_API_TOKEN not set")
    sys.exit(1)

# Fetch one open deal from v2
r = requests.get(
    "https://api.pipedrive.com/api/v2/deals",
    params={"status": "open", "limit": 1},
    headers={"x-api-token": token},
)
r.raise_for_status()
deals = r.json().get("data", [])
if not deals:
    print("No open deals returned")
    sys.exit(1)

deal = deals[0]

print("=" * 60)
print(f"Deal ID: {deal.get('id')}  Title: {deal.get('title')}")
print("=" * 60)
print("\n--- All keys returned by v2 ---")
for k, v in sorted(deal.items()):
    if v not in (None, "", [], {}):
        print(f"  {k}: {repr(v)[:80]}")

print("\n--- Fields we need (check these specifically) ---")
check = [
    "owner_id", "user_id", "weighted_value", "probability",
    "last_activity_date", "activities_count", "done_activities_count",
    "undone_activities_count", "last_incoming_mail_time", "last_outgoing_mail_time",
    "email_messages_count",
]
for f in check:
    print(f"  {f}: {repr(deal.get(f))}")

print("\n--- Custom fields (long hash keys) ---")
custom_keys = [k for k in deal.keys() if len(k) == 40]
if custom_keys:
    for k in custom_keys:
        print(f"  {k}: {repr(deal[k])[:60]}")
else:
    print("  None found — custom fields may need a different API parameter")

print("\n--- Raw JSON (first deal) ---")
print(json.dumps(deal, indent=2, default=str)[:3000])
