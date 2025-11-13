from config import settings
from config.logging import get_logger

from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import date

import os
import json
import pandas as pd

from pipeline.utils.main_functions import ensure_directories
from pipeline.utils.staging import (run_pipeline, PIPELINE_STAGES)
from pipeline.utils.dataframes import create_dataframes

from analtytics.calculate_kpis import calculate_kpis

from report.reporting import generate_report


logger = get_logger(__name__)

logger.info('Starting pipeline...')

# --- Ensure directories exist
logger.info("Validating folder structure...")

dirs = ["data", "logs", "report"]

ensure_directories(directories=dirs)

# Set Paths
logger.info("Setting configuration parameters...")
LOG_PATH = "./logs/downloaded_tables.log"
DATA_DIR = "./data"
CSV_DIR = "./data/csv"
REPORT_PATH = "./docs/index.html"

DATASET = "emoji-oracle-74368.analytics_501671751"


logger.info("Initializing BigQuery client...")

# for local dev, use key file

KEY_PATH = "./keys/key.json"
credentials = service_account.Credentials.from_service_account_file(KEY_PATH)

# for GitHub Actions

# creds_dict = json.loads(os.environ["BQ_SERVICE_ACCOUNT"])
# credentials = service_account.Credentials.from_service_account_info(creds_dict)


logger.info("BigQuery client initialized.")


df = pd.DataFrame()
df_sessions = pd.DataFrame()


logger.info("Starting data pull...")
if __name__ == "__main__":
    client = bigquery.Client(credentials=credentials)

    context = {
        "client": client,
        "log_path": settings.LOG_PATH,
        "data_dir": settings.DATA_DIR,
        'csv_dir': settings.CSV_DIR,
        "dataset": settings.DATASET,
        'start_date': settings.START_DATE,
        'report_path': settings.REPORT_PATH
    }

    # pull data and run through pipeline
    df = run_pipeline(df=df, context=context)
    logger.info("Data pipeline executed successfully.")
    

    logger.info("Generating dataframes...")
    dfs = create_dataframes(df=df)
    logger.info("Dataframes generated successfully.")

    logger.info("Calculating KPIs...")

    kpis = calculate_kpis(df=df, dict=dfs)
    df.to_csv(os.path.join(settings.CSV_DIR, f"processed_data.csv"), index=False)

    for name, dataframe in dfs.items():
        dataframe.to_csv(os.path.join(settings.CSV_DIR, f"{name}_data.csv"), index=False)
    
    logger.info("Data pipeline complete. Processed data saved.")
    generate_report(df=df, dict = dfs, kpis = kpis, context = context)




