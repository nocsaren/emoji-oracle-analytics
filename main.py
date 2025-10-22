print("[INFO]    Initializing pipeline...")
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import date

import os
import json

from pipeline.utils.main_functions import ensure_directories
from pipeline.utils.staging import (run_pipeline, PIPELINE_STAGES)
from pipeline.utils.reporting import generate_report
from config import settings

print("[INFO]    Module imports successful.")

# --- Ensure directories exist
print("[INFO]    Validating folder structure...")

dirs = ["data", "logs", "report"]

ensure_directories(directories=dirs)

# Set Paths
print("[INFO]    Setting configuration parameters...")
LOG_PATH = "./logs/downloaded_tables.log"
DATA_DIR = "./data"
REPORT_PATH = "./docs/index.html"

DATASET = "emoji-oracle-74368.analytics_501671751"
VERSION = "1.0.0"

print("[INFO]    Initializing BigQuery client...")

# for local dev, use key file

KEY_PATH = "./keys/key.json"
credentials = service_account.Credentials.from_service_account_file(KEY_PATH)

# for GitHub Actions

# creds_dict = json.loads(os.environ["BQ_SERVICE_ACCOUNT"])
# credentials = service_account.Credentials.from_service_account_info(creds_dict)


print("[INFO]    BigQuery client initialized.")

print("[INFO]    Starting data pull...")
if __name__ == "__main__":
    client = bigquery.Client(credentials=credentials)

    context = {
        "client": client,
        "log_path": settings.LOG_PATH,
        "data_dir": settings.DATA_DIR,
        "dataset": settings.DATASET
    }

    df = run_pipeline(df=None, context=context)
    generate_report(df, output_path=REPORT_PATH)




