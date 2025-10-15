from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import date, timedelta
import os

# Setup credentials from env (in GitHub Actions)
import json
creds_dict = json.loads(os.environ["BQ_SERVICE_ACCOUNT"])
credentials = service_account.Credentials.from_service_account_info(creds_dict)
client = bigquery.Client(credentials=credentials)

# Paths
LOG_PATH = "logs/downloaded_tables.txt"
DATA_DIR = "data_new"
REPORT_PATH = "report/index.html"
DATASET = "emoji-oracle-74368.analytics_501671751"

# --- Ensure directories exist
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs("logs", exist_ok=True)
os.makedirs("report", exist_ok=True)

# --- Load downloaded tables log
if os.path.exists(LOG_PATH):
    with open(LOG_PATH) as f:
        downloaded = {line.strip() for line in f if line.strip()}
else:
    downloaded = set()

# --- Get all existing tables
query = f"""
SELECT table_id
FROM `{DATASET}.__TABLES__`
WHERE table_id LIKE 'events_%'
"""
tables = [row.table_id for row in client.query(query)]

# --- Determine which tables are new
new_tables = [t for t in tables if t not in downloaded]

if not new_tables:
    print("No new tables to process.")
    exit(0)

for table_name in new_tables:
    print(f"Processing {table_name}...")

    df = client.query(f"SELECT * FROM `{DATASET}.{table_name}`").to_dataframe()

    # --- Example transformation (replace with yours)
    df["source_table"] = table_name

    # --- Save as parquet
    path = os.path.join(DATA_DIR, f"{table_name}.parquet")
    table = pa.Table.from_pandas(df)
    pq.write_table(table, path)

    # --- Update log
    with open(LOG_PATH, "a") as f:
        f.write(table_name + "\n")

# --- Combine all Parquets for report
dfs = [pd.read_parquet(os.path.join(DATA_DIR, f)) for f in os.listdir(DATA_DIR) if f.endswith(".parquet")]
all_data = pd.concat(dfs, ignore_index=True)

# --- Simple HTML summary
summary = all_data.groupby("source_table").size().reset_index(name="rows")
html = summary.to_html(index=False)
with open(REPORT_PATH, "w") as f:
    f.write(f"<h2>ETL Summary as of {date.today()}</h2>")
    f.write(html)

print("ETL completed successfully.")
