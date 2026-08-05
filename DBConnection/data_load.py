"""
load_to_neon_postgres.py

Walks through Data/raw_data/<year>/<year_month>/transactions_<year_month>.csv
and INCREMENTALLY loads only *new* files into a single 'raw_transactions'
table in Neon Postgres. Already-loaded files are tracked in a local metadata
JSON file so re-running the script never reprocesses or wipes old data.

Usage:
    python load_to_neon_postgres.py

Credentials are loaded automatically from a `.env` file. This script will
search upward from its own location (through parent directories) until it
finds `.env`, so it works no matter which subdirectory the script itself
lives in, as long as `.env` sits at or above it (e.g. at the project root).

.env should contain:
    PG_HOST=ep-xxxx-xxxx.us-east-2.aws.neon.tech
    PG_PORT=5432
    PG_DB=neondb
    PG_USER=neondb_owner
    PG_PASSWORD=your_password_here
    PG_TABLE=raw_transactions

Required packages:
    pip install pandas sqlalchemy psycopg2-binary python-dotenv
"""

import os
import json
import glob
import logging
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv, find_dotenv

# ---------------------------------------------------------------------------
# LOAD .env AUTOMATICALLY (searches upward from this file's location)
# ---------------------------------------------------------------------------
# find_dotenv() with usecwd=False starts the search at this script's own
# directory and walks up through parent folders until it finds a `.env`.
# That means it works whether this script sits directly in the main
# directory or several levels down in a subdirectory, as long as `.env`
# lives at or above it.
dotenv_path = find_dotenv(filename=".env", raise_error_if_not_found=False, usecwd=False)

if dotenv_path:
    load_dotenv(dotenv_path)
else:
    # Fall back to searching from the current working directory, in case
    # the script is being invoked in an unusual way (e.g. via -c or a
    # frozen exe) where __file__-based search doesn't apply.
    load_dotenv(find_dotenv(filename=".env", raise_error_if_not_found=False, usecwd=True))

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
PG_HOST = os.environ.get("PG_HOST", "")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DB = os.environ.get("PG_DB", "neondb")
PG_USER = os.environ.get("PG_USER", "")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "")
TABLE_NAME = os.environ.get("PG_TABLE", "raw_transactions")

data_dir = r"E:\\Financial Transaction Fraud Detection Platform\\DataGeneration"

RAW_DATA_DIR = os.path.join(data_dir, "raw_data")
LOGS_DIR = os.path.join("Data", "logs")
os.makedirs(LOGS_DIR, exist_ok=True)

# Metadata file that tracks which CSVs have already been uploaded.
# Since the source data generation doesn't tag rows with any unique key or
# timestamp column, the filename itself (e.g. transactions_2026_08.csv) is
# used as the unit of "new vs already loaded".
METADATA_PATH = os.path.join("Data", "metadata.json")

# ---------------------------------------------------------------------------
# LOGGING SETUP
# ---------------------------------------------------------------------------
run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file_path = os.path.join(LOGS_DIR, f"load_to_neon_{run_timestamp}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

logger.info("=== Neon Postgres incremental load run started ===")

if dotenv_path:
    logger.info(f".env loaded from: {dotenv_path}")
else:
    logger.warning("No .env file found via upward search; relying on existing environment variables.")

if not PG_HOST or not PG_USER or not PG_PASSWORD:
    logger.error("PG_HOST / PG_USER / PG_PASSWORD are not fully set. Check your .env file.")
    raise SystemExit(1)

# ---------------------------------------------------------------------------
# METADATA HELPERS
# ---------------------------------------------------------------------------
def load_metadata(path):
    """Return the metadata dict, creating a fresh structure if none exists yet."""
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Could not read metadata file ({e}); starting a fresh one.")
    return {"processed_files": {}}


def save_metadata(path, metadata):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)
    os.replace(tmp_path, path)  # atomic-ish swap so a crash mid-write can't corrupt metadata.json


metadata = load_metadata(METADATA_PATH)
processed_files = metadata.setdefault("processed_files", {})

# ---------------------------------------------------------------------------
# BUILD CONNECTION
# sslmode=require -> mandatory for Neon
# channel_binding=require -> Neon-recommended extra protection against MITM
# ---------------------------------------------------------------------------
connection_string = (
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    f"?sslmode=require&channel_binding=require"
)

try:
    engine = create_engine(connection_string)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    logger.info(f"Connected successfully to {PG_HOST}/{PG_DB}")
except Exception as e:
    logger.error(f"Failed to connect to Neon Postgres: {e}")
    raise

# ---------------------------------------------------------------------------
# FIND ALL CSV FILES, THEN FILTER OUT ONES ALREADY LOADED
# ---------------------------------------------------------------------------
csv_pattern = os.path.join(RAW_DATA_DIR, "**", "*.csv")
all_csv_files = sorted(glob.glob(csv_pattern, recursive=True))

if not all_csv_files:
    logger.error(f"No CSV files found matching pattern: {csv_pattern}")
    raise SystemExit(1)

new_csv_files = [f for f in all_csv_files if os.path.abspath(f) not in processed_files]
already_loaded_count = len(all_csv_files) - len(new_csv_files)

logger.info(
    f"Found {len(all_csv_files)} CSV files total "
    f"({already_loaded_count} already loaded, {len(new_csv_files)} new)."
)

if not new_csv_files:
    logger.info("Nothing new to load. Exiting.")
    print("Done. No new files to load — database already up to date.")
    raise SystemExit(0)

# ---------------------------------------------------------------------------
# LOAD ONLY THE NEW FILES — always append, never replace the table
# ---------------------------------------------------------------------------
total_rows_loaded = 0
newly_processed = []

for file_path in new_csv_files:
    abs_path = os.path.abspath(file_path)
    try:
        df = pd.read_csv(file_path)

        # if_exists="append": creates the table on the very first-ever run
        # (if it doesn't exist yet) and simply appends rows on every run
        # after that. This never drops or replaces existing data.
        df.to_sql(
            TABLE_NAME,
            engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000
        )

        total_rows_loaded += len(df)
        logger.info(f"Loaded {len(df)} rows from {file_path}")

        # Only mark as processed AFTER a successful upload, so a crash
        # mid-run leaves the file eligible for retry next time.
        processed_files[abs_path] = {
            "rows_loaded": len(df),
            "loaded_at": datetime.now(timezone.utc).isoformat(),
        }
        newly_processed.append(file_path)

        # Persist metadata incrementally (after each file) rather than only
        # at the very end, so partial progress survives a crash.
        save_metadata(METADATA_PATH, metadata)

    except Exception as e:
        logger.error(f"Failed to load {file_path}: {e}")
        # Do not add to processed_files — it will be retried next run.

logger.info(
    f"=== Load complete: {total_rows_loaded} total rows loaded from "
    f"{len(newly_processed)} new file(s) into '{TABLE_NAME}' ==="
)
print(
    f"Done. Loaded {total_rows_loaded} rows from {len(newly_processed)} new file(s) "
    f"into table '{TABLE_NAME}' on {PG_HOST}. Log file: {log_file_path}"
)