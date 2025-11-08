from config.logging import get_logger

import pandas as pd
import numpy as np

from analtytics.utils.kpi_functions import retention_rate

logger = get_logger(__name__)

def calculate_kpis (df: pd.DataFrame, session: pd.DataFrame) -> dict:
    
    session_start_date = pd.DataFrame()
    session_start_date['date'] = df['session_start_time'].dt.normalize()
    kpis = {
        'most_recent_date' : df['event_date'].max(),
        'total_users': df['user_pseudo_id'].nunique(),
        'total_sessions': session['event_params__ga_session_id'].nunique(),
        'sessions_per_day': session['event_params__ga_session_id'].nunique() / session_start_date['date'].nunique(),
        'sessions_per_user': session['event_params__ga_session_id'].nunique() / df['user_pseudo_id'].nunique(),
        'average_session_duration': session['session_duration_seconds'].mean(),
        'one_day_retention': retention_rate(df=df, days=1),
        'seven_day_retention': retention_rate(df=df, days=7),
        'thirty_day_retention': retention_rate(df=df, days=30),
        }
    

    return kpis
