print("Starting data update process...")

# --- Standard Library ---

import os
import sys
import json

# --- Google Cloud Auth + APIs ---

from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import GoogleAPICallError, RetryError

from gspread_dataframe import set_with_dataframe

# --- Data & Visualization ---

import pandas as pd
import numpy as np
import openpyxl

# --- Local Modules ---

from modules.utilities import (
    pull_and_append,
    rebuild_data_json_from_backups,
    upload_named_dataframes_to_bq
)

from modules.flattening import (
    flatten_extract_params, 
    flatten_row,
    flatten_nested_column
)

from modules.cleaning import (
    apply_value_maps,
    safe_select_and_rename
)
# --- Lists and Maps ---

from modules.lists_and_maps import (
    df_column_names_map, 
    columns_to_drop,
    map_of_maps
    )


print("Imports completed successfully.")

# --- Path Setup ---
SERVICE_ACCOUNT_KEY = './keys/key.json'
DATA_PATH = './data/data.json'
PROJECT_ID = "emojioracle-342f1"
DATASET_ID = "analytics_481352676"
BACKUP_PATH = './backup/'

# Ensure service account key exists
if not os.path.exists(SERVICE_ACCOUNT_KEY):
    print(f"Service account key not found at {SERVICE_ACCOUNT_KEY}. Please check the path, or download a new json key file.")
    sys.exit(1)
    

print("Paths set up successfully.")
# --- BigQuery Setup ---
SCOPES = [
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_KEY,
    scopes = SCOPES
)
bq_client = bigquery.Client(credentials = credentials, project = PROJECT_ID)


print("BigQuery client initialized successfully.")

# --- Main Execution ---

raw_data = pd.DataFrame(pull_and_append(credentials = credentials, 
                                  project_id = PROJECT_ID, 
                                  dataset_id = DATASET_ID, 
                                  data_path = DATA_PATH, 
                                  backup_path = BACKUP_PATH))

print(f"Data loaded with {len(raw_data)} rows and {len(raw_data.columns)} columns.")



# Load the JSON data into a DataFrame
df = pd.read_json(DATA_PATH)

print(f"Data loaded into DataFrame with {df.shape[0]} rows and {df.shape[1]} columns.")

# --- Flatten the DataFrame ---
df = pd.DataFrame([flatten_row(row) for _, row in df.iterrows()]) # for wtfs refer to ./modules/flattening_json.py


print(f"Data flattened to {df.shape[0]} rows and {df.shape[1]} columns.")

df.columns = df.columns.str.replace('.', '__')

print(f"Column names updated to use '__' instead of '.' - now {df.shape[1]} columns.")

# --- Date and Time Cleanup and Transformation ---
df = df.drop(columns=['event_date'], errors='ignore') # built in case event_date may not be the same as the one in the event_timestamp

df['time_delta'] = pd.to_datetime(df['event_timestamp'], unit='us', utc=True) - pd.to_datetime(df['event_previous_timestamp'], unit='us', utc=True)
df['time_delta'] = df['time_delta'].dt.total_seconds() # convert to seconds

df['event_datetime'] = pd.to_datetime(df['event_timestamp'], unit='us', utc=True) 
df['event_previous_datetime'] = pd.to_datetime(df['event_previous_timestamp'], unit='us', utc=True)
df['event_first_touch_datetime'] = pd.to_datetime(df['user_first_touch_timestamp'], unit='us', utc=True)
df['user__first_open_datetime'] = pd.to_datetime(df['user__first_open_time'], unit='ms', utc=True)


df['event_date'] = df['event_datetime'].dt.normalize()
df['event_time'] = df['event_datetime'].dt.time

df['event_previous_date'] = df['event_previous_datetime'].dt.normalize()
df['event_previous_time'] = df['event_previous_datetime'].dt.time

df['event_first_touch_date'] = df['event_first_touch_datetime'].dt.normalize()
df['event_first_touch_time'] = df['event_first_touch_datetime'].dt.time

df['user__first_open_date'] = df['user__first_open_datetime'].dt.normalize()
df['user__first_open_time'] = df['user__first_open_datetime'].dt.time

df['device__time_zone_offset_hours'] = df['device__time_zone_offset_seconds'] / 3600 # seconds to hours
df['event_params__engagement_time_seconds'] = df['event_params__engagement_time_msec'] / 1000 # ms to seconds
df['event_server_delay_seconds'] = df['event_server_timestamp_offset'] / 1000 # ms to seconds 
df['event_params__time_spent_seconds'] = df['event_params__time_spent'] # just renaming for clarity


print("Date and time cleanup and transformation completed successfully.")


# --- Add Time-Based Features ---

df['ts_weekday'] = df['event_datetime'].dt.day_name() # weekday name
df['ts_weekday'] = pd.Categorical(df['ts_weekday'], 
                                  categories=['Monday', 'Tuesday', 'Wednesday', 
                                              'Thursday', 'Friday', 'Saturday', 
                                              'Sunday'],
                                  ordered=True) # order the weekdays

df['ts_local_time'] = df['event_datetime'] + pd.to_timedelta(df['device__time_zone_offset_hours'].fillna(0), unit='h') # local time
df['ts_hour'] = df['ts_local_time'].dt.hour # local hour
df['ts_daytime_named'] = df['ts_hour'].apply(lambda x: 
                                             'Gece' if (x < 6 or x > 22) else 
                                             'Sabah' if x < 11 else 
                                             'Öğle' if x < 14 else 
                                             'Öğleden Sonra' if x < 17 else 
                                             'Akşam') # time group of day
df['ts_is_weekend'] = df['ts_weekday'].apply(lambda x: 
                                             'Hafta Sonu' if x in ['Saturday', 'Sunday'] else
                                             'Hafta İçi') 
df['ts_weekday'] = df['ts_weekday'].astype(str) # convert to string for consistency


print("Time-based features added successfully.")
# --- Question Index Clean-up ---
"""
Tier 1: 16 Questions, Except t: 12
Tier 2: 12 Questions
Tier 3: 12 Questions
Tier 4: 10 Questions
"""

df['event_params__current_question_index'] = pd.NA

df['event_params__current_tier'] = pd.to_numeric(df['event_params__current_tier'], errors='coerce').astype("Int64")
df['event_params__current_qi'] = pd.to_numeric(df['event_params__current_qi'], errors='coerce').astype("Int64")

notna_mask = df['event_params__character_name'].notna() & df['event_params__current_tier'].notna() & df['event_params__current_qi'].notna()

# Tier 1
tier_1_mask = notna_mask & (df['event_params__current_tier'] == 1)
t_char_mask = tier_1_mask & (df['event_params__character_name'] == 't')

df.loc[t_char_mask, 'event_params__current_question_index'] = 13 - df.loc[t_char_mask, 'event_params__current_qi']
df.loc[~t_char_mask & tier_1_mask, 'event_params__current_question_index'] = 17 - df.loc[(~t_char_mask) & tier_1_mask, 'event_params__current_qi']

# Tier 2 & 3
tier_2_3_mask = notna_mask & df['event_params__current_tier'].isin([2, 3])
df.loc[tier_2_3_mask, 'event_params__current_question_index'] = 13 - df.loc[tier_2_3_mask, 'event_params__current_qi']

# Tier 4
tier_4_mask = notna_mask & (df['event_params__current_tier'] == 4)
df.loc[tier_4_mask, 'event_params__current_question_index'] = 11 - df.loc[tier_4_mask, 'event_params__current_qi']

# Hiccups
problems_mask = notna_mask & ~df['event_params__current_tier'].isin([1, 2, 3, 4])
if df[problems_mask].shape[0] > 0:
    print("Something wrong in:")
    print(df.loc[problems_mask, ['event_params__character_name', 'event_params__current_tier', 'event_params__current_qi']])

print(f"Question index cleaned up for {df['event_params__current_question_index'].notna().sum()} rows.")

# Calculate cumulative question index

df['cumulative_question_index'] = df['event_params__current_question_index']


# Tier 2
df.loc[(df['event_params__current_tier'] == 2) & (df['event_params__character_name'] == 't'), 'cumulative_question_index'] += 12
df.loc[(df['event_params__current_tier'] == 2) & (df['event_params__character_name'] != 't'), 'cumulative_question_index'] += 16

# Tier 3
df.loc[(df['event_params__current_tier'] == 3) & (df['event_params__character_name'] == 't'), 'cumulative_question_index'] += 24
df.loc[(df['event_params__current_tier'] == 3) & (df['event_params__character_name'] != 't'), 'cumulative_question_index'] += 28

# Tier 4
df.loc[(df['event_params__current_tier'] == 4) & (df['event_params__character_name'] == 't'), 'cumulative_question_index'] += 36
df.loc[(df['event_params__current_tier'] == 4) & (df['event_params__character_name'] != 't'), 'cumulative_question_index'] += 40

# NaNs
df.loc[df['event_params__current_tier'].isna(), 'cumulative_question_index'] = pd.NA

print(f"Cumulative question index calculated for {df['cumulative_question_index'].notna().sum()} rows.")


# --- Session Definition and Duration Calculation ---

''' 

Create a calculated session times dataframe from the events dataframe.
This will infer session times based on the time gaps between events for each user.

This is done by:
1. Sorting events by user and timestamp.
2. Calculating the time difference between consecutive events for each user.
3. Defining a session timeout (6 minutes).
4. Assigning session IDs based on the time gaps.

'''

# Ensure events are sorted per user
df_sorted = df.sort_values(by=['user_pseudo_id', 'event_datetime'])

# Compute time gap between events per user
df_sorted['time_diff'] = df_sorted.groupby('user_pseudo_id')['event_datetime'].diff()

# Use 6-minute timeout
SESSION_TIMEOUT = pd.Timedelta(minutes=6)

# Define inferred session ID using 6-minute gaps
df_sorted['inferred_session_id'] = (
    (df_sorted['time_diff'] > SESSION_TIMEOUT) | df_sorted['time_diff'].isna()
).cumsum()

# Assign session IDs to the original DataFrame
df['inferred_session_id'] = df_sorted['inferred_session_id'].loc[df.index]

# Calculate session duration
df['session_duration_seconds'] = df.groupby(['user_pseudo_id', 'inferred_session_id'])['event_datetime'].transform(
    lambda x: (x.max() - x.min()).total_seconds()
)

# Session start and end times
df['session_start_time'] = df.groupby(['user_pseudo_id', 'inferred_session_id'])['event_datetime'].transform('min')
df['session_end_time'] = df.groupby(['user_pseudo_id', 'inferred_session_id'])['event_datetime'].transform('max')

print(f"Session IDs assigned and durations calculated for {df['inferred_session_id'].nunique()} unique sessions.")


# Infer and forward-fill the character name, current tier, and current question index within each session

# Step 1: Sort chronologically within sessions
df_sorted = df.sort_values(by=['user_pseudo_id', 'inferred_session_id', 'event_datetime'])

# Step 2: Forward-fill the relevant columns per user-session group
cols_to_fill = [
    'event_params__character_name',
    'event_params__current_tier',
    'event_params__current_question_index'
]

df_sorted[cols_to_fill] = (
    df_sorted
    .groupby(['user_pseudo_id', 'inferred_session_id'])[cols_to_fill]
    .ffill()
)

df.loc[df_sorted.index, cols_to_fill] = df_sorted[cols_to_fill]

print(f"Character names, tiers, and question indices forward-filled for {df['inferred_session_id'].nunique()} unique sessions.")


# Split 'event_params_mini_game_ri' maze_hand_* into columns
# e.g 'maze_hand_WomanHandTwo_maze_level_3'

# Column to process
col = 'event_params__mini_game_ri'

# Filter rows starting with 'maze_hand'
mask = df[col].str.startswith('maze_hand', na=False)

# Split the matching rows by underscore
parts = df.loc[mask, col].str.split('_', expand=True)

# Extract Gender and Hand using the updated regex
gender_hand = parts[2].str.extract(r'(?P<Gender>Woman|Man)Hand(?P<Hand>\w+)')

# Extract Level (assumed to be in the last part)
levels = parts[5]

# Create new columns with extracted data
df.loc[mask, 'maze_gender'] = gender_hand['Gender']
df.loc[mask, 'maze_hand'] = gender_hand['Hand']
df.loc[mask, 'maze_level'] = levels

print(f"Extracted maze hand data for {mask.sum()} rows.")
# Split event_params_mini_game_ri buff_* into columns
# e.g. 'buff_IncreaseXEnergy_gift_True_gold_False'


# Column to process
col = 'event_params__mini_game_ri'

# Filter rows starting with 'buff'
mask = df[col].str.startswith('buff', na=False)

# Split the matching rows by underscore
parts = df.loc[mask, col].str.split('_', expand=True)

# Extract Buff Type and Level
buff_type = parts[2].str.extract(r'(?P<BuffType>\w+)')

# Extract Buff Gift and Gold status
buff_gift = parts[3].str.extract(r'(?P<BuffGift>\w+)')
buff_gold = parts[5].str.extract(r'(?P<BuffGold>\w+)')

# Create new columns with extracted data
df.loc[mask, 'buff_type'] = buff_type['BuffType']
df.loc[mask, 'buff_gift'] = buff_gift['BuffGift'].str.lower() == 'true'
df.loc[mask, 'buff_gold'] = buff_gold['BuffGold'].str.lower() == 'true'

print(f"Extracted buff data for {mask.sum()} rows.")
# Split event_params_mini_game_ri earned_buff_* into columns
# e.g. 'earned_buff_GiveXCharacter'

# Column to process
col = 'event_params__mini_game_ri'

# Filter rows starting with 'earned_buff'
mask = df[col].str.startswith('earned_buff', na=False)

# Split the matching rows by underscore
parts = df.loc[mask, col].str.split('_', expand=True)

# Extract Buff Type
buff_type = parts[2].str.extract(r'(?P<BuffType>\w+)')

# Create new columns with extracted data
df.loc[mask, 'earned_buff_type'] = buff_type['BuffType']

print(f"Extracted earned buff data for {mask.sum()} rows.")

# Split event_params__spent_to doll values into columns
# e.g. 'erjohndoll'

# Column to process
col = 'event_params__spent_to'

# Filter rows including string 'doll'
mask = df[col].str.contains('doll', na=False)

# Split the string by name and doll
parts = df.loc[mask, col].str.split('doll', expand=True)

# Extract the doll name
df.loc[mask, 'doll_name'] = parts[0].str.strip()  # Get the name before 'doll'

# Rewrite the 'event_params__spent_to' column to just the doll name
df.loc[mask, col] = 'Doll'

print(f"Extracted doll data for {mask.sum()} rows.")


# Split event_params__spent_to crystal values into columns
# list of possible values: cauldron_item, aliginn_item, coffee_item

# Column to process
col = 'event_params__spent_to'

# Filter rows including values from the list
mask = df[col].str.contains('cauldron_item|aliginn_item|coffee_item', na=False)

# Split the string by name and item
parts = df.loc[mask, col].str.split('_', expand=True)

# Extract the item name
df.loc[mask, 'spent_in_crystal'] = parts[0].str.strip()  # Get the name before '_item'

# Rewrite the 'event_params__spent_to' column to just the item name
df.loc[mask, col] = 'Crystal Ball'

print(f"Extracted crystal ball data for {mask.sum()} rows.")

# Write event_params_spent_to permanent shop item values into shop_permanent_item
# list of possible values: dreamcatcher, catcollar, library1, library2, bugspray, schedule

# Column to process
col = 'event_params__spent_to'

# Filter rows including values from the list
mask = df[col].str.contains('dreamcatcher|catcollar|library1|library2|bugspray|schedule|crystal|horseshoe', na=False)

# Create a new column for the shop permanent item
df.loc[mask, 'shop_permanent_item'] = df.loc[mask, col].str.extract(r'(dreamcatcher|catcollar|library1|library2|bugspray|schedule|crystal|horseshoe)')[0]

# Rewrite the 'event_params__spent_to' column to just the item name

df.loc[mask, col] = 'Permanent Item'

print(f"Extracted permanent shop item data for {mask.sum()} rows.")


# Write event_params_spent_to consumable shop item values into shop_consumable_item
# list of possible values: potion, ıncense, amulet, incense

# Column to process
col = 'event_params__spent_to'

# Filter rows including values from the list
mask = df[col].str.contains('potion|ıncense|amulet|incense', na=False)

# Create a new column for the shop consumable item
df.loc[mask, 'shop_consumable_item'] = df.loc[mask, col].str.extract(r'(potion|ıncense|amulet|incense)')[0]

# Rewrite the 'event_params__spent_to' column to just the item name
df.loc[mask, col] = 'Consumable Item'

print(f"Extracted consumable shop item data for {mask.sum()} rows.")

# Write event_params_spent_to mini_game remainin item values into board_item
# everything except: ['Doll', 'Crystal Ball', 'Permanent Item', 'Consumable Item']

# Column to process
col = 'event_params__spent_to'

# Filter rows that are not in the known categories
mask = (~df[col].isin(['Doll', 'Crystal Ball', 'Permanent Item', 'Consumable Item'])) & \
    (df['event_params__where_its_spent'].isin(['board', 'board_item']))

# Create a new column for the board item
df.loc[mask, 'board_item'] = df.loc[mask, col]

# Rewrite the 'event_params__spent_to' column to just the item name
df.loc[mask, col] = 'Board Item'
print(f"Extracted board item data for {mask.sum()} rows.")

df = df.drop(columns=columns_to_drop)

print(f"Dropped {len(columns_to_drop)} columns: {columns_to_drop}.")


# Apply value maps to the DataFrame
print("Applying value maps to the DataFrame...")

df = apply_value_maps(df, map_of_maps, keep_unmapped=True)

print(f"Value maps applied. DataFrame now has {df.shape[1]} columns.")
# Create adressable question index
df['question_address'] = df['event_params__character_name'] + ' - T: ' + df['event_params__current_tier'].astype(str) + ' - Q: ' + df['event_params__current_question_index'].astype(str)

print(f"Question address created for {df['question_address'].notna().sum()} rows.")
# Create user_metrics


df['event_datetime'] = pd.to_datetime(df['event_datetime'], errors='coerce')

# Group by user and calculate user-level metrics
user_metrics = df.groupby('user_pseudo_id').agg(
    first_seen=('event_datetime', 'min'),
    last_seen=('event_datetime', 'max'),
    total_sessions=('inferred_session_id', pd.Series.nunique),
    total_events=('event_name', 'count')
).reset_index()

# Compute user lifetime in days
user_metrics['lifetime_days'] = np.ceil((user_metrics['last_seen'] - 
                                         user_metrics['first_seen']).dt.total_seconds() / 86400
).astype('Int64')  # nullable int type for BQ/LS compatibility

# Churn flag based on 80th percentile of user lifetime in days
reference_date = df['event_datetime'].max()


threshold = user_metrics['lifetime_days'].quantile(0.80)
user_metrics['is_churned'] = user_metrics['lifetime_days'] > threshold


# Retention buckets (for visualization or filtering in LS)
user_metrics['retention_bucket'] = pd.cut(
    user_metrics['lifetime_days'],
    bins=[-1, 0, 1, 3, 7, 14, 30, 90, float('inf')],
    labels=[
            '0_0d',
            '1_1d',
            '2_1-3d',
            '3_4-7d',
            '4_8-14d',
            '5_15-30d',
            '6_31-90d',
            '7_90+d'
    ]
)

# Active/returning user flags
user_metrics['is_retained_1d'] = user_metrics['lifetime_days'] >= 1
user_metrics['is_retained_7d'] = user_metrics['lifetime_days'] >= 7
user_metrics['is_retained_30d'] = user_metrics['lifetime_days'] >= 30

print(f"User metrics calculated for {user_metrics.shape[0]} users.")

df = safe_select_and_rename(df, df_column_names_map)

print(f"DataFrame columns renamed and selected according to the map. Now has {df.shape[1]} columns.")

print("Data cleaning and transformation completed successfully.")
print("Saving cleaned data to CSV files...")

df.to_csv('./data/cleaned_data.csv', index=False, chunksize=100000)
user_metrics.to_csv('./data/user_metrics.csv', index=False)

print("Cleaned data saved to CSV files successfully.")