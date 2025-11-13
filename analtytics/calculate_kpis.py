from config.logging import get_logger

import pandas as pd
import numpy as np

from analtytics.utils.kpi_functions import retention_rate

logger = get_logger(__name__)

def calculate_kpis (df: pd.DataFrame, dict) -> dict:
    
    df_by_ads = dict['by_ads']
    df_by_sessions = dict['by_sessions']
    df_by_users = dict['by_users']
    df_by_questions = dict['by_questions']
    df_by_date = dict['by_date']
    df_technical_events = dict['technical_events']

    session_start_date = pd.DataFrame()
    session_start_date['date'] = df['session_start_time'].dt.normalize()
    kpis = {
        'From': df['event_date'].min().strftime('%d.%m.%Y'),
        'To': df['event_date'].max().strftime('%d.%m.%Y'),
        'Total Days': (df_by_date['event_date'].max() - df['event_date'].min()).days,
        'Total Users': df_by_users['user_pseudo_id'].nunique(),
        'Users per Day': round(df_by_users['user_pseudo_id'].nunique() / df['event_date'].nunique(), 2),
        'Total Sessions': df_by_sessions['event_params__ga_session_id'].nunique(),
        'Sessions per Day': round(df_by_sessions['event_params__ga_session_id'].nunique() / session_start_date['date'].nunique(), 2),
        'Sessions per User': round(df_by_sessions['event_params__ga_session_id'].nunique() / df['user_pseudo_id'].nunique(), 2),
        'Average Session Duration': round(df_by_sessions['session_duration_seconds'].mean(skipna=True) / 60, 2) if not df_by_sessions.empty else 0,
        'Total Ads Viewed': df_by_ads['total_impressions'].sum() if 'total_impressions' in df_by_ads.columns else 0,
        '1-Day Retention %': retention_rate(df=df, days=1),
        '7-Day Retention %': retention_rate(df=df, days=7),
        '30-Day Retention %': retention_rate(df=df, days=30),
        
        }
    logger.info("✅ KPIs calculated.")
    logger.info(f"KPIs: {kpis}")

    return kpis
