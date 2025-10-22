import pandas as pd

def question_index_cleanup(df: pd.DataFrame, context=None) -> pd.DataFrame:
    """
    Cleans up the current question index based on character name and
    # --- Question Index Clean-up ---
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
        print("[FAIL]        Something wrong in:")
        print(df.loc[problems_mask, ['event_params__character_name', 'event_params__current_tier', 'event_params__current_qi']])
    print(f"[INFO]    Question index cleaned up for {df['event_params__current_question_index'].notna().sum()} rows.")
    return df

def dots_to_underscores(df: pd.DataFrame, context=None) -> pd.DataFrame:

    df.columns = df.columns.str.replace('.', '__') 
    print(f"[INFO]    Column names updated to use '__' instead of '.' successfully.")
    
    return df
