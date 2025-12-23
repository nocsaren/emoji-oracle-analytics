from datetime import date

DATA_DIR = "./parquet-store"
LOG_PATH = "./parquet-store/log.txt"

# LOG_PATH = "./logs/downloaded_tables.log"
# DATA_DIR = "./data/parquet"
CSV_DIR = "./data/csv"
REPORT_PATH = "./docs"


DATASET = "emoji-oracle-74368.analytics_501671751"

START_DATE = date(2025, 11, 27)

<<<<<<< Updated upstream:emoji_oracle_analytics/config/settings.py
COUNTRY= ['United States', 'Türkiye'] 
NOT_USER=[]


VERSION_FILTER = "1.0.5" # >=
=======
COUNTRY= ['United States'] # SHOULD ALWAYS BE A LIST, I.E. []
NOT_USER=['a6bdeeb9060751b4b3a2c29d71b5e049', 'd927559ab16657cec3256525a16c9238']


VERSION_FILTER = "1.0.5"
>>>>>>> Stashed changes:config/settings.py
