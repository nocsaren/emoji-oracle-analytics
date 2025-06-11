# --- Google Cloud Auth + APIs ---
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import GoogleAPICallError, RetryError






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
