import os
import pandas as pd

def backup_as_csv(df, backup_path):
    if os.path.exists(backup_path):
        try:
            df_existing_backup = pd.read_csv(backup_path)
            df.to_csv(backup_path)
            print(f"Updated data.json with {len(df_new)} new rows.")
        except ValueError:
            df_existing_backup = pd.DataFrame()  # csv is invalid, start fresh
            print("Invalid or empty backup file, starting fresh.")