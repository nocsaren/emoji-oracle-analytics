import pandas as pd
import numpy as np
import datetime as dt

from emoji_oracle_analytics.config.logging import get_logger

logger = get_logger(__name__)

def transform_datetime_fields(df: pd.DataFrame, context=None) -> pd.DataFrame:
    """Clean and transform timestamp and date/time-related columns."""
    
    # Drop redundant or conflicting columns
    df = df.drop(columns=['event_date'], errors='ignore')
    
    # Define timestamp conversions (unit in microseconds unless noted)
    time_fields = {
        'event_datetime': ('event_timestamp', 'us'),
        'event_previous_datetime': ('event_previous_timestamp', 'us'),
        'event_first_touch_datetime': ('user_first_touch_timestamp', 'us'),
        'user__first_open_datetime': ('user__first_open_time', 'ms'),
        
    }
    
    for new_col, (src_col, unit) in time_fields.items():
        df[new_col] = pd.to_datetime(df[src_col], unit=unit, utc=True)
    
    # Time delta in seconds
    df['time_delta'] = (
        df['event_datetime'] - df['event_previous_datetime']
    ).dt.total_seconds()
    
    # Derive normalized date/time components
    for base in ['event', 'event_previous', 'event_first_touch', 'user__first_open']:
        # Use floor("D") instead of normalize() to keep behavior equivalent and
        # avoid Pylance/stub false-positives about DatetimeProperties.normalize.
        df[f'{base}_date'] = df[f'{base}_datetime'].dt.floor('D')
        df[f'{base}_time'] = df[f'{base}_datetime'].dt.time

    # Unit conversions and renames
    df['device__time_zone_offset_hours'] = df['device__time_zone_offset_seconds'] / 3600
    df['event_params__engagement_time_seconds'] = df['event_params__engagement_time_msec'] / 1000
    df['event_server_delay_seconds'] = df['event_server_timestamp_offset'] / 1000
    df['event_params__time_spent_seconds'] = df.get('event_params__time_spent')

    logger.info("Date/time cleanup and transformation complete.")
    return df




def add_time_based_features(df: pd.DataFrame, context=None) -> pd.DataFrame:
    """Add time-based features derived from event_datetime and time zone offset."""
    
    # Ensure datetime and offset fields exist
    if 'event_datetime' not in df or 'device__time_zone_offset_hours' not in df:
        raise KeyError("Required columns 'event_datetime' or 'device__time_zone_offset_hours' missing.")
    
    # Weekday name (ordered categorical)
    df['ts_weekday'] = pd.Categorical(
        df['event_datetime'].dt.day_name(),
        categories=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'],
        ordered=True
    )

    # Local timestamp and derived hour
    df['ts_local_time'] = df['event_datetime'] + pd.to_timedelta(
        df['device__time_zone_offset_hours'].fillna(0), unit='h'
    )
    df['ts_hour'] = df['ts_local_time'].dt.hour

    # Named daypart (Turkish)
    def _daytime_label(hour: int) -> str:
        if hour < 6 or hour > 22:
            return 'Gece'
        elif hour < 11:
            return 'Sabah'
        elif hour < 14:
            return 'Öğle'
        elif hour < 17:
            return 'Öğleden Sonra'
        else:
            return 'Akşam'

    df['ts_daytime_named'] = df['ts_hour'].apply(_daytime_label)

    # Weekend / weekday flag (Turkish)
    df['ts_is_weekend'] = df['ts_weekday'].astype(str).apply(
        lambda x: 'Hafta Sonu' if x in ['Saturday', 'Sunday'] else 'Hafta İçi'
    )

    # Convert weekday to string for consistency
    df['ts_weekday'] = df['ts_weekday'].astype(str)

    logger.info("Time-based features added successfully.")
    return df


def add_durations(df: pd.DataFrame, context=None) -> pd.DataFrame:

    def _squeeze_1d(value, *, label: str) -> pd.Series:
        """Coerce a groupby/apply result into a 1D Series.

        Local environments can sometimes end up with duplicate column names or
        pandas returning a single-column DataFrame from apply/reindex.
        """
        if isinstance(value, pd.Series):
            return value
        if isinstance(value, pd.DataFrame):
            if value.shape[1] == 1:
                return value.iloc[:, 0]
            raise ValueError(f"Expected 1D result for {label}, got DataFrame with shape={value.shape}")
        # Fall back to Series constructor for array-likes/scalars
        return pd.Series(value)

    # If upstream filters removed all events, keep the pipeline moving with empty outputs.
    if df.empty:
        df['session_duration_seconds'] = pd.Series(dtype='float64')
        df['session_duration_minutes'] = pd.Series(dtype='float64')
        df['session_duration_hours'] = pd.Series(dtype='float64')
        df['session_start_time'] = pd.Series(dtype='datetime64[ns]')
        df['session_end_time'] = pd.Series(dtype='datetime64[ns]')
        df['event_duration_seconds'] = pd.Series(dtype='float64')
        logger.info("add_durations: input DataFrame is empty; skipping duration calculations.")
        return df
    
    # Ensure event_datetime is in datetime format
    df['event_datetime'] = pd.to_datetime(df['event_datetime'])

    # Firebase assigns these events at the last session's end, hence the messy logic
    exclude_session_end_events = ['app_remove', 'app_update', 'app_clear_data']

    # Calculate session duration (ignoring session-end events)
    grouped = df.groupby(['user_pseudo_id', 'event_params__ga_session_id'])
    try:
        durations = grouped.apply(
            lambda g: (
                g.loc[~g['event_name'].isin(exclude_session_end_events), 'event_datetime'].max()
                - g.loc[g['event_name'] == 'session_start', 'event_datetime'].min()
            ).total_seconds()
            if not g.loc[g['event_name'] == 'session_start'].empty else np.nan,
            include_groups=False,
        )
    except TypeError:
        durations = grouped.apply(
            lambda g: (
                g.loc[~g['event_name'].isin(exclude_session_end_events), 'event_datetime'].max()
                - g.loc[g['event_name'] == 'session_start', 'event_datetime'].min()
            ).total_seconds()
            if not g.loc[g['event_name'] == 'session_start'].empty else np.nan
        )

    if not df.columns.is_unique:
        dupes = df.columns[df.columns.duplicated()].unique().tolist()
        logger.warning("DataFrame has duplicate columns (showing up to 20): %s", dupes[:20])

    durations = _squeeze_1d(durations, label="durations")

    session_index = df.set_index(['user_pseudo_id', 'event_params__ga_session_id']).index
    aligned = _squeeze_1d(durations.reindex(session_index), label="durations.reindex(session_index)")

    # Ensure a flat 1D numpy array for assignment.
    df['session_duration_seconds'] = pd.to_numeric(aligned, errors='coerce').round(3).to_numpy()
    df['session_duration_minutes'] = (df['session_duration_seconds'] / 60).round(2)
    df['session_duration_hours'] = (df['session_duration_seconds'] / 3600).round(3)
    # Session start and end times
    df['session_start_time'] = df.groupby(['user_pseudo_id', 'event_params__ga_session_id'])['event_datetime'].transform('min')
    df['session_end_time'] = df.groupby(['user_pseudo_id', 'event_params__ga_session_id'])['event_datetime'].transform('max')
    logger.info(f"Session IDs assigned and durations calculated for {df['event_params__ga_session_id'].nunique()} unique sessions.")

    # Calculate event duration within sessions
    df = df.sort_values(by=['user_pseudo_id', 'event_params__ga_session_id', 'event_datetime'])
    df['event_duration_seconds'] = df.groupby(['user_pseudo_id', 'event_params__ga_session_id'])['event_datetime'].shift(-1) - df['event_datetime']
    df['event_duration_seconds'] = df['event_duration_seconds'].dt.total_seconds().fillna(0).round(3)
    logger.info("Event durations within sessions calculated successfully.")
    return df


