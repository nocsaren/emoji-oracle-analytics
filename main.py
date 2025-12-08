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

from pipeline.utils.calculate_kpis import calculate_kpis

from pipeline.utils.reporting import generate_report


logger = get_logger(__name__)

logger.info('Starting pipeline...')


# --- Ensure directories exist
logger.info("Validating folder structure...")

folders_to_create = []

for name in dir(settings):
    if name.isupper():
        value = getattr(settings, name)
        if isinstance(value, str) and (value.startswith("./") or value.startswith("/")):
            # If value ends with a known file extension, take dirname; else, take the path itself
            if os.path.splitext(value)[1]:  # has extension → file path
                folders_to_create.append(os.path.dirname(value))
            else:  # likely already a folder
                folders_to_create.append(value)
    
ensure_directories(folders_to_create)

logger.info("Initializing BigQuery client...")



def load_credentials():
    # GitHub Actions way
    creds_env = os.environ.get("BQ_SERVICE_ACCOUNT")
    if creds_env:
        creds_dict = json.loads(creds_env)
        return service_account.Credentials.from_service_account_info(creds_dict)

    # Local Dev way
    key_path = "./keys/key.json"
    if os.path.exists(key_path):
        return service_account.Credentials.from_service_account_file(key_path)

    raise RuntimeError("No Google service account credentials found.")

credentials = load_credentials()

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
        'report_path': settings.REPORT_PATH,
        'country': settings.COUNTRY,
        'not_user': settings.NOT_USER
    }
    

    # pull data and run through pipeline
    df = run_pipeline(df=df, context=context)
    logger.info("Data pipeline executed successfully.")
    

    logger.info("Generating dataframes...")
    dfs = create_dataframes(df=df)
    logger.info("Dataframes generated successfully.")

    logger.info("Calculating KPIs...")

    kpis = calculate_kpis(df=df, dict=dfs)
    
#    sliced_data = df[df['user_pseudo_id'] == 'a6bdeeb9060751b4b3a2c29d71b5e049'].copy()

#    sliced_data.to_csv(os.path.join(settings.CSV_DIR, "sliced_data.csv"), index=False)

#    df.to_csv(os.path.join(settings.CSV_DIR, "processed_data.csv"), index=False)

    for name, dataframe in dfs.items():
        dataframe.to_csv(os.path.join(settings.CSV_DIR, f"{name}_data.csv"), index=False)
    
    logger.info("Data pipeline complete. Processed data saved.")
    generate_report(df=df, dfs_dict = dfs, kpis = kpis, context = context)
