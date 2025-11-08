from config.logging import get_logger

logger = get_logger(__name__)

import pandas as pd
import numpy as np

from pipeline.utils.split_functions import df_by_sessions


df_sessions = df_by_sessions(df=pd.DataFrame()) 
df_users = pd.DataFrame()
df_questions = pd.DataFrame()
df_ads = pd.DataFrame()