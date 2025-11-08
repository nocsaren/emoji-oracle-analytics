import pandas as pd

from pipeline.utils.utils import summarize_gold # summarize_energy
from config.logging import get_logger

logger = get_logger(__name__)

def df_by_sessions(df: pd.DataFrame) -> pd.DataFrame:
    session_groups = ['event_params__ga_session_id', 'user_pseudo_id']

    session_duration = (
        df.groupby(session_groups)['session_duration_seconds']
        .mean()
        .round(2)
        .reset_index()
        .query('session_duration_seconds > 10')
    )

    session_start = (
        df[df['event_name'] == 'Session Started']
        .groupby(session_groups)['session_start_time']
        .min()
        .reset_index()
    )

    # --- Character counts (for customer calculation) ---
    session_df = (
        df[df['event_name'] == 'Question Started']
        .groupby(session_groups)['event_params__character_name']
        .nunique()
        .reset_index(name='customer_character_count')
    )

    # ---Character list per session --- 

    session_df['character_list'] = (
        df[df['event_name'] == 'Question Started']
        .groupby(session_groups)['event_params__character_name']
        .apply(lambda x: x.dropna().tolist())
        .reset_index(drop=True)
    )

    # --- Average tier per session ---
    session_df['average_tier'] = (
        df[df['event_name'] == 'Question Started']
        .groupby(session_groups)['event_params__current_tier']
        .mean()
        .reset_index(drop=True)
        .round(2)
    )
    # --- Average wrong answers per session ---

    session_df['average_wrong_answers'] = (
        df[df['event_name'] == 'Question Completed']
        .groupby(session_groups)['event_params__answered_wrong']
        .mean()
        .reset_index(drop=True)
        .round(2)
    )
    # --- Wheel metrics ---

    wheel = (
        df.groupby(session_groups)['event_params__mini_game_ri']
        .agg(
            Wheel_Impression=lambda x: (x == 'Daily Spin').sum(),
            Wheel_Skips=lambda x: (x == 'spin_skipped').sum()
        )
        .reset_index()
        .assign(Wheel_Spins=lambda df: df['Wheel_Impression'] - df['Wheel_Skips'])
    )

    # --- Ads watched ---
    ads = (
        df.groupby(session_groups)['event_name']
        .apply(lambda x: (x == 'ad_impression').sum())
        .reset_index(name='Ads_Watched_Count')
    )

    # --- In-game currency ---
    gold = (
        df.groupby(session_groups).apply(summarize_gold).reset_index()
    )
    # Bought items calculation ---

    consumable_items = (
        df[df['event_params__spent_to'] == 'Consumable Item']
        .groupby(session_groups)['shop_consumable_item']
        .agg(
            Potions_Bought=lambda x: (x == 'Potion').sum(),
            Incenses_Bought=lambda x: (x == 'Incense').sum(),
            Amulets_Bought=lambda x: (x == 'Amulet').sum()
        )
        .reset_index()
    )
    # Energy metrics

    energy_spent = (
        df[df['event_params__spent_to'].isin(['Cauldron', 'AliCin', 'Coffee'])]
        .groupby(session_groups)['event_params__spent_to']
        .agg(
            AliCin_Used=lambda x: (x == 'AliCin').sum(), # costs 2 energy
            Cauldron_Used=lambda x: (x == 'Cauldron').sum(), # costs 1 energy
            Coffee_Used=lambda x: (x == 'Coffee').sum() # costs 1 energy
        )
        .reset_index()
    )

    # --- Merge all session-level metrics ---
    result = (
        session_df
        .merge(session_duration, on=session_groups, how='left')
        .merge(session_start, on=session_groups, how='left')
        .merge(wheel, on=session_groups, how='left')
        .merge(ads, on=session_groups, how='left')
        .merge(gold, on=session_groups, how='left')
        .merge(consumable_items, on=session_groups, how='left')
        .merge(energy_spent, on=session_groups, how='left')
    )

    # --- New customer calculation ---
    result['bought_new_customer'] = result['customer_character_count'] // 3

    logger.info(f"Session-level dataframe created with {result.shape[0]} records and {result.shape[1]} columns.")

    return result


def df_by_users(df: pd.DataFrame) -> pd.DataFrame:
    user_groups = ['user_pseudo_id']

    # these events fu session end calculation
    exclude_last_events = ['App Removed', 'App Data Cleared', 'App Updated']

    # filter for last_event_date computation
    df_no_end = df[~df['event_name'].isin(exclude_last_events)]

    user_df = (
        df.groupby(user_groups)
        .agg(
            first_event_date=('event_date', 'min'),
            total_sessions=('event_params__ga_session_id', 'nunique'),
            total_characters_opened=('event_params__character_name', 'nunique'),
            total_gold_earned=('event_params__gold_earned', 'sum'),
            total_gold_spent=('event_params__gold_spent', 'sum'),
            total_ads_watched=('event_name', lambda x: (x == 'Ad Impression').sum()),
            total_questions_answered=('event_name', lambda x: (x == 'Question Completed').sum()),
            country=('geo__country', 'first'),
            install_source=('app_info__install_source', 'first'),
            operating_system=('device__operating_system', 'first'),
            operating_system_version=('device__operating_system_version', 'first'),
            is_limited_ad_tracking=('device__is_limited_ad_tracking', 'first'),
            device__language=('device__language', 'first'),
            version=('app_info__version', 'last')

        )
        .join(
            df_no_end.groupby(user_groups)['event_date'].max().rename('last_event_date'),
            on=user_groups
        )   
        .reset_index()
    )

    logger.info(f"User-level dataframe created with {user_df.shape[0]} records and {user_df.shape[1]} columns.")

    return user_df

def df_by_questions(df: pd.DataFrame) -> pd.DataFrame:
    question_groups = ['question_address', 'user_pseudo_id', 'session_id', 'character_name', 'tier']

    def question_metrics(g):
        return pd.Series({
            'potions_bought': (g['shop_consumable_item'] == 'Potion').sum(),
            'incense_bought': (g['shop_consumable_item'] == 'Incense').sum(),
            'amulet_bought': (g['shop_consumable_item'] == 'Amulet').sum(),
            'alicin_used':(g['event_params__spent_to'] == 'AliCin').sum(),
            'coffee_used':(g['event_params__spent_to'] == 'Coffee').sum(),
            'cauldron_used':(g['event_params__spent_to'] == 'Cauldron').sum(),
            'scroll_opened': ((g['event_name'] == 'Menu Opened') &
                              (g['event_params__menu_name'] == 'Scroll Menu')).sum()
        })

    question_df = (
        df.groupby(question_groups)
          .apply(question_metrics)
          .reset_index()
    )

    logger.info(
        f"Question-level dataframe created with {question_df.shape[0]} records and "
        f"{question_df.shape[1]} columns."
    )

    return question_df

def df_by_date(df: pd.DataFrame) -> pd.DataFrame:
    
    date_df = (
        df.groupby(['event_date'])
        .agg(
            weekday = ('ts_weekday', 'first'),
            unique_users = ('user_pseudo_id', 'nunique'),
            new_users = ('event_name', lambda x: (x == 'First Open').sum()),
            uninstall_count = ('event_name', lambda x: (x == 'App Removed').sum()),

            unique_sessions = ('event_params__ga_session_id', 'nunique').sum(),
            ads_watched=('event_name', lambda x: (x == 'Ad Impression').sum())

        )

    
    date_df = (
        d
          .apply(date_metrics)
          .reset_index()
    )
    )