"""
load_to_neon_postgres.py

Walks through Data/raw_data/<year>/<year_month>/transactions_<year_month>.csv
and bulk-loads every file into a single 'transactions' table in Neon Postgres.

Usage:
    python load_to_neon_postgres.py

Configure connection details via environment variables (recommended) or by
editing the DEFAULT_* constants below.

Get your connection details from the Neon console -> Project -> Connection Details.
PG_HOST will look like: ep-xxxx-xxxx.us-east-2.aws.neon.tech
PG_USER is often in the form: neondb_owner
PG_DB defaults to: neondb

Required packages:
    pip install pandas sqlalchemy psycopg2-binary
"""

import os
import glob
import logging
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# CONFIG — set these as environment variables, or hardcode for local testing
# ---------------------------------------------------------------------------
PG_HOST = os.environ.get("PG_HOST", "")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DB = os.environ.get("PG_DB", "neondb")
PG_USER = os.environ.get("PG_USER", "neondb_owner")
PG_PASSWORD = os.environ.get("PG_PASSWORD","")  # do NOT hardcode in real use
TABLE_NAME = os.environ.get("PG_TABLE", "raw_transactions")

data_dir=r"E:\\Financial Transaction Fraud Detection Platform\\DataGeneration"


RAW_DATA_DIR = os.path.join(data_dir , "raw_data")
LOGS_DIR = os.path.join("Data", "logs")
os.makedirs(LOGS_DIR, exist_ok=True)

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

logger.info("=== Neon Postgres load run started ===")

if not PG_PASSWORD:
    logger.error("PG_PASSWORD is not set. Set it as an environment variable before running.")
    raise SystemExit(1)

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
# FIND ALL CSV FILES UNDER Data/raw_data/<year>/<year_month>/
# ---------------------------------------------------------------------------
csv_pattern = os.path.join(RAW_DATA_DIR, "**", "*.csv")
csv_files = glob.glob(csv_pattern, recursive=True)

if not csv_files:
    logger.error(f"No CSV files found matching pattern: {csv_pattern}")
    raise SystemExit(1)

logger.info(f"Found {len(csv_files)} CSV files to load.")

# ---------------------------------------------------------------------------
# LOAD EACH FILE — first file creates/replaces the table, rest append
# ---------------------------------------------------------------------------
total_rows_loaded = 0

for idx, file_path in enumerate(csv_files):
    try:
        df = pd.read_csv(file_path)
        write_mode = "replace" if idx == 0 else "append"

        df.to_sql(
            TABLE_NAME,
            engine,
            if_exists=write_mode,
            index=False,
            method="multi",
            chunksize=1000
        )

        total_rows_loaded += len(df)
        logger.info(f"Loaded {len(df)} rows from {file_path} (mode={write_mode})")
    except Exception as e:
        logger.error(f"Failed to load {file_path}: {e}")

logger.info(f"=== Load complete: {total_rows_loaded} total rows loaded into '{TABLE_NAME}' ===")
print(f"Done. Loaded {total_rows_loaded} rows into table '{TABLE_NAME}' on {PG_HOST}. Log file: {log_file_path}")