# --- Google Cloud Auth + APIs ---
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPICallError, RetryError

import os
import pandas as pd

import traceback

def pull_and_append(credentials, project_id, dataset_id, data_path, backup_path):
    """
    Ensures all available BigQuery tables are backed up as JSON files in backup_path.
    Appends only new data (based on event_date) to data_path JSON.

    Parameters:
        credentials: GCP service account credentials
        project_id (str): GCP project ID
        dataset_id (str): BigQuery dataset ID
        data_path (str): Path to merged JSON (e.g., ./data/data.json)
        backup_path (str): Directory to store individual table JSON backups
    """
    os.makedirs(backup_path, exist_ok=True)

    # Load existing merged data
    if os.path.exists(data_path):
        try:
            df_existing = pd.read_json(data_path)
            print("Loaded existing data.")
        except ValueError:
            df_existing = pd.DataFrame()
            print("Invalid or empty data file, starting fresh.")
    else:
        df_existing = pd.DataFrame()
        print("No data.json found, starting fresh.")

    latest_date = df_existing["event_date"].max() if not df_existing.empty else 0
    print(f"Latest event_date in merged data: {latest_date}")

    client = bigquery.Client(credentials=credentials, project=project_id)
    new_dataframes = []

    for table in client.list_tables(dataset_id):
        name = table.table_id
        if not name.startswith("events_"):
            continue

        try:
            event_date = int(name.split("_")[-1])
        except ValueError:
            print(f"Skipping invalid table name: {name}")
            continue

        backup_file = os.path.join(backup_path, f"{name}.json")

        # If no backup, fetch from BigQuery and write backup
        if not os.path.exists(backup_file):
            print(f"Backing up missing table: {name}")
            full_table_id = f"{project_id}.{dataset_id}.{name}"
            df = client.list_rows(full_table_id).to_dataframe()
            df["event_date"] = event_date
            df.to_json(backup_file, orient='records')
        else:
            print(f"Backup already exists: {name}")
            df = pd.read_json(backup_file)

        # Only append to data.json if this is newer than current data
        if event_date > latest_date:
            new_dataframes.append(df)

    # Append to data_path if needed
    if new_dataframes:
        df_new = pd.concat(new_dataframes, ignore_index=True)
        df_all = pd.concat([df_existing, df_new], ignore_index=True)
        df_all.to_json(data_path, orient='records')
        print(f"Appended {len(df_new)} new rows to data.json.")
        return df_all
    else:
        print("No new data to append.")
        return df_existing







def rebuild_data_json_from_backups(backup_path, data_path):
    """
    Reconstructs a single merged data.json file from all JSON table backups.

    Parameters:
        backup_path (str): Directory containing JSON files named like events_YYYYMMDD.json
        data_path (str): Path to write merged data (e.g., ./data/data.json)
    """
    if not os.path.isdir(backup_path):
        raise FileNotFoundError(f"Backup path '{backup_path}' does not exist.")

    all_dataframes = []

    for fname in sorted(os.listdir(backup_path)):
        if fname.startswith("events_") and fname.endswith(".json"):
            fpath = os.path.join(backup_path, fname)
            try:
                df = pd.read_json(fpath)
                all_dataframes.append(df)
                print(f"Loaded {fname}")
            except Exception as e:
                print(f"Failed to load {fname}: {e}")

    if all_dataframes:
        df_all = pd.concat(all_dataframes, ignore_index=True)
        df_all.to_json(data_path, orient='records')
        print(f"Rebuilt data.json with {len(df_all)} rows.")
        return df_all
    else:
        print("No valid backup files found.")
        return pd.DataFrame()



def upload_named_dataframes_to_bq(dataframes, dataset_id, project_id, bq_client):
    """
    Uploads multiple dataframes to BigQuery using their variable names as table names.

    Parameters:
        dataframes_dict (dict): Keys are dataframe names (as strings), values are the actual DataFrame objects.
    """
    for name, df in dataframes.items():
        table_id = f"{project_id}.{dataset_id}.{name}"
        try:
            job = bq_client.load_table_from_dataframe(
                df,
                table_id,
                job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            )
            job.result()
            print(f"DataFrame '{name}' uploaded successfully to {table_id}")
        except GoogleAPICallError as api_error:
            print(f"API error uploading '{name}' to {table_id}: {api_error}")
        except RetryError as retry_error:
            print(f"Retry error uploading '{name}' to {table_id}: {retry_error}")
        except Exception as e:
            print(f"Unexpected error uploading '{name}' to {table_id}: {e}")
            print(f"Exception type: {type(e)}")
            traceback.print_exc()
