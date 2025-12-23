import os
import pandas as pd
from config.logging import get_logger
from pathlib import Path

logger = get_logger(__name__)

def ensure_directories(directories):
    """
    Ensure that the specified directories exist.
    Paths are resolved relative to the current working directory (repo root).
    """
    try:
        for d in directories:
            path = Path(d).resolve()
            path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Directory exists or created: {path}")
        logger.info("All folder structures validated.")
    except Exception:
        logger.exception("Error creating directories")
        raise SystemExit(1)


def filter_events_by_date(df, context):
    """Filter events in the DataFrame to only include those on or after start_date (UTC)."""
    # Convert date to pandas Timestamp (assumed UTC)

    start_date = context["start_date"]
    start_dt = pd.Timestamp(start_date, tz='UTC')

    # Convert to UNIX microseconds
    start_ts = int(start_dt.value // 10**3)

    # Filter
    filtered_df = df[df['event_timestamp'] >= start_ts]

    logger.info(
        f"Filtered events from {len(df)} to {len(filtered_df)} based on start date {start_dt}."
    )
    return filtered_df

def filter_events_by_country(df, context):
    
    # Convert date to pandas Timestamp (assumed UTC)
    country = context['country']
    filtered_df = df[df['geo__country'].isin(country)]

    logger.info(
        f"Filtered events from {len(df)} to {len(filtered_df)} based on countries: {country}."
    )
    return filtered_df

def filter_events_by_user(df, context):
    
    # Convert date to pandas Timestamp (assumed UTC)
    not_user = context['not_user']
    filtered_df = df[~df['user_pseudo_id'].isin(not_user)]

    logger.info(
        f"Filtered events from {len(df)} to {len(filtered_df)} based on users: {not_user}."
    )
    
    return filtered_df


def vers(v1, v2):
    """
    Compare two version strings like '1.0.3' and '1.0.12'.
    Returns:
        -1 if v1 < v2
         0 if v1 == v2
         1 if v1 > v2
    """
    # Convert version strings to tuples of integers
    t1 = tuple(int(x) for x in str(v1).split('.'))
    t2 = tuple(int(x) for x in str(v2).split('.'))
    
    # Compare tuples
    if t1 < t2:
        return -1
    elif t1 > t2:
        return 1
    else:
        return 0
    
def filter_events_by_version(df, context):
    """
    Filter events in the DataFrame to only include those with app_version >= specified version.
    """
    version_filter = context['version_filter']
    
    # Apply version comparison
    filtered_df = df[df['app_info__version'].apply(lambda v: vers(v, version_filter) >= 0)]

    logger.info(
        f"Filtered events from {len(df)} to {len(filtered_df)} based on app version >= {version_filter}."
    )
    return filtered_df