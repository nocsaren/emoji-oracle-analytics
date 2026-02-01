"""Project configuration.

These are intentionally simple constants so a non-data developer can quickly
see what drives inputs/outputs.

Where these values are used:
- `DATA_DIR`: local parquet cache for downloaded BigQuery tables
- `LOG_PATH`: a text file listing which BigQuery tables were downloaded
- `CSV_DIR`: pipeline outputs written as CSV for inspection/sharing
- `REPORT_PATH`: HTML report output folder (served as static pages)

Notes:
- `DATASET` is a BigQuery dataset (GA4 export-style) that contains tables like
	`events_YYYYMMDD`.
- `START_DATE` controls the earliest date to pull/process.
"""

from datetime import date

DATA_DIR = "./parquet-store"
LOG_PATH = "./parquet-store/log.txt"

# LOG_PATH = "./logs/downloaded_tables.log"
# DATA_DIR = "./data/parquet"
CSV_DIR = "./data/csv"
REPORT_PATH = "./docs"


DATASET = "emoji-oracle-74368.analytics_501671751"

START_DATE = date(2025, 11, 1)

COUNTRY = []

# List of user IDs to exclude from analysis (e.g., internal test devices).
NOT_USER: list[str] = []


# Keep only events where the app version is >= this version.
VERSION_FILTER = "1.0.4"  # >=
