import os
import pandas as pd
from google.cloud import bigquery

def pull_and_append(credentials, project_id, dataset_id, data_path):
    """
    supposed to update the LOCAL json formatted data_path file with new REMOTE event data from BigQuery Table IDs

    BigQuery Table IDs look like this: 
        
        project_id.dataset_id.events_YYYYMMDD

     1- loads LOCAL, (if not exists or corrput, creates new)
     2- gets the latest event date from LOCAL
     3- gets the latest event date from REMOTE table name
     4- compares dates
     5- if new found, appends to LOCAL

    Parameters:
        credientials (str:): service_account.Credentials.from_service_account_file from IAM
        project_id (str): The Google Cloud project ID
        dataset_id (str): The BigQuery dataset ID
        data_path (str): The path to the data file in Google Drive (normally ./data/data.json)
    """

    # Check JSON exists
    if os.path.exists(data_path):
        try:
            df_existing = pd.read_json(data_path)
            print("Loaded existing data.")
        except ValueError:
            df_existing = pd.DataFrame()  # JSON is invalid, start fresh
            print("Invalid or empty data file, starting fresh.")
    else:
        df_existing = pd.DataFrame()  # no JSON, start fresh
        print("No data.json found, starting fresh.")

    # Extract the latest event date in existing data (only if file exists and has data)
    if not df_existing.empty:
        latest_date = df_existing["event_date"].max()
        print(f"Latest event date in existing data: {latest_date}")
    else:
        latest_date = 0  # Start with 0 if there's no existing data or file is empty
        print("No existing data, fetching all available data.")

    # BigQuery client
    client = bigquery.Client(credentials=credentials, project=project_id)

    # look for new BigQuery tables
    new_dataframes = []

    for table in client.list_tables(dataset_id):
        name = table.table_id  # e.g., 'events_20250405'

        if name.startswith("events_"):
            date_str = name.split("_")[-1]
            try:
                event_date = int(date_str)
            except ValueError:
                print(f'Invalid date format:{date_str}')
                continue  # Skip if date format is invalid

            # import all if no existing data, else import new tables
            if latest_date == 0 or event_date > latest_date:
                print(f"Fetching table: {name}")
                full_table_id = f"{project_id}.{dataset_id}.{name}"
                df = client.list_rows(full_table_id).to_dataframe()
                df["event_date"] = event_date  # Tag it with the event date
                new_dataframes.append(df)

    # Append new data to existing data and save
    if new_dataframes:
        df_new = pd.concat(new_dataframes, ignore_index=True)
        df_all = pd.concat([df_existing, df_new], ignore_index=True)

        # Save the updated DataFrame to data.json
        df_all.to_json(data_path, orient='records')
        print(f"Updated data.json with {len(df_new)} new rows.")
    else:
        print("No new data found or no tables to import.")