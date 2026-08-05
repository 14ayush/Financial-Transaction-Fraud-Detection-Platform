"""
seed_metadata.py

ONE-TIME USE ONLY.

Run this ONCE, before your first run of the fixed load_to_neon_postgres.py,
to tell the metadata tracker "these files are already in the database, don't
reload them." It marks every CSV currently found in raw_data/ as already
processed, based on the assumption that your previous script run already
uploaded everything present in that folder as of right now.

After running this once, DELETE this file (or just never run it again) —
running it a second time later would incorrectly mark newer files as
"already loaded" too, and they'd get skipped by mistake.

Usage:
    python seed_metadata.py
"""

import os
import glob
import json
from datetime import datetime, timezone

data_dir = r"E:\Financial Transaction Fraud Detection Platform\DataGeneration"
RAW_DATA_DIR = os.path.join(data_dir, "raw_data")
METADATA_PATH = os.path.join("Data", "metadata.json")

csv_pattern = os.path.join(RAW_DATA_DIR, "**", "*.csv")
existing_files = sorted(glob.glob(csv_pattern, recursive=True))

if not existing_files:
    print(f"No CSV files found under {RAW_DATA_DIR}. Nothing to seed.")
    raise SystemExit(0)

metadata = {"processed_files": {}}
now = datetime.now(timezone.utc).isoformat()

for f in existing_files:
    abs_path = os.path.abspath(f)
    metadata["processed_files"][abs_path] = {
        "rows_loaded": None,  # unknown - seeded, not loaded by this script
        "loaded_at": now,
        "note": "seeded as pre-existing; not actually loaded by this run"
    }

os.makedirs(os.path.dirname(METADATA_PATH), exist_ok=True)
with open(METADATA_PATH, "w", encoding="utf-8") as f:
    json.dump(metadata, f, indent=2)

print(f"Seeded {len(existing_files)} file(s) into {METADATA_PATH}.")
print("These will be SKIPPED by load_to_neon_postgres.py from now on.")
print("Any CSV added to raw_data/ AFTER this point will be treated as new.")