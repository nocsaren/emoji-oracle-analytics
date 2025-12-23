from __future__ import annotations

import json
import os
from typing import Any

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

from emoji_oracle_analytics.config import settings
from emoji_oracle_analytics.config.logging import get_logger
from emoji_oracle_analytics.pipeline.utils.calculate_kpis import calculate_kpis
from emoji_oracle_analytics.pipeline.utils.dataframes import create_dataframes
from emoji_oracle_analytics.pipeline.utils.main_functions import ensure_directories
from emoji_oracle_analytics.pipeline.utils.reporting import generate_report
from emoji_oracle_analytics.pipeline.utils.staging import run_pipeline

logger = get_logger(__name__)


def load_credentials():
    """Load BigQuery service account credentials.

    Prefers env var (CI) then local keys file.
    """

    creds_env = os.environ.get("BQ_SERVICE_ACCOUNT")
    if creds_env:
        creds_dict: Any = json.loads(creds_env)
        return service_account.Credentials.from_service_account_info(creds_dict)

    key_path = "./keys/key.json"
    if os.path.exists(key_path):
        return service_account.Credentials.from_service_account_file(key_path)

    raise RuntimeError("No Google service account credentials found.")


def main() -> None:
    logger.info("Starting pipeline...")

    # --- Ensure directories exist
    logger.info("Validating folder structure...")

    folders_to_create: list[str] = []
    for name in dir(settings):
        if not name.isupper():
            continue
        value = getattr(settings, name)
        if not (isinstance(value, str) and (value.startswith("./") or value.startswith("/"))):
            continue

        # If value ends with a known file extension, take dirname; else, take the path itself
        if os.path.splitext(value)[1]:
            folders_to_create.append(os.path.dirname(value))
        else:
            folders_to_create.append(value)

    ensure_directories(folders_to_create)

    logger.info("Initializing BigQuery client...")
    credentials = load_credentials()
    client = bigquery.Client(credentials=credentials)
    logger.info("BigQuery client initialized.")

    context = {
        "client": client,
        "log_path": settings.LOG_PATH,
        "data_dir": settings.DATA_DIR,
        "csv_dir": settings.CSV_DIR,
        "dataset": settings.DATASET,
        "start_date": settings.START_DATE,
        "report_path": settings.REPORT_PATH,
        "country": settings.COUNTRY,
        "not_user": settings.NOT_USER,
        "version_filter": settings.VERSION_FILTER,
    }

    df = run_pipeline(df=pd.DataFrame(), context=context)
    logger.info("Data pipeline executed successfully.")

    logger.info("Generating dataframes...")
    dfs = create_dataframes(df=df)
    logger.info("Dataframes generated successfully.")

    logger.info("Calculating KPIs...")
    kpis = calculate_kpis(df=df, dict=dfs)

    sliced_data = df[df["user_pseudo_id"] == "00edf42bee4cb1b14a6ce0e90f9ad3f9"].copy()
    sliced_data.to_csv(os.path.join(settings.CSV_DIR, "sliced_data.csv"), index=False)

    df.to_csv(os.path.join(settings.CSV_DIR, "processed_data.csv"), index=False)

    for name, dataframe in dfs.items():
        dataframe.to_csv(os.path.join(settings.CSV_DIR, f"{name}_data.csv"), index=False)

    logger.info("Data pipeline complete. Processed data saved.")
    generate_report(df=df, dfs_dict=dfs, kpis=kpis, context=context)


if __name__ == "__main__":
    main()
