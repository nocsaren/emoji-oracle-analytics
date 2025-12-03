from config.logging import get_logger
from pipeline.utils.split_functions import (create_df_by_sessions,
                                            create_df_by_users,
                                            create_df_by_questions,
                                            create_df_by_ads,
                                            create_df_by_date,
                                            create_df_technical_events,
                                            )
import pandas as pd

logger = get_logger(__name__)

def create_dataframes(df: pd.DataFrame):
    """Generate actual dataframes from a single source df."""
    dataframes = {
        "by_sessions": create_df_by_sessions(df),
        "by_users": create_df_by_users(df),
        "by_questions": create_df_by_questions(df),
        "by_ads": create_df_by_ads(df),
        "by_date": create_df_by_date(df),
        "technical_events": create_df_technical_events(df),
    }
    logger.info("All split dataframes successfully created.")
    return dataframes
