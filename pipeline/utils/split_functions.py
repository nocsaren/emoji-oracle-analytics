import pandas as pd

from pipeline.utils.utils import summarize_gold # summarize_energy
from config.logging import get_logger

logger = get_logger(__name__)

def create_df_by_sessions(df: pd.DataFrame) -> pd.DataFrame:
    try:
        session_groups = ['event_params__ga_session_id', 'user_pseudo_id']

        # --- Ensure required columns exist ---
        required_cols = ['session_duration_seconds', 'event_name', 'event_datetime']
        if not all(col in df.columns for col in required_cols + session_groups):
            logger.warning("Missing required columns for df_by_sessions.")
            return pd.DataFrame()

        # --- Filter sessions ---
        df = df[df['session_duration_seconds'] > 15]

        # --- Base sessions ---
        base_sessions = df[session_groups].drop_duplicates().reset_index(drop=True)

        # --- Session duration ---
        session_duration = (
            df.groupby(session_groups, as_index=False)['session_duration_seconds']
              .mean()
              .round(2)
        )
        session_duration['passed_10_min'] = session_duration['session_duration_seconds'] >= 600

        # --- Session start time ---
        if 'session_start_time' in df.columns:
            session_start = (
                df.loc[df['event_name'] == 'Session Started']
                  .groupby(session_groups, as_index=False)['session_start_time']
                  .min()
            )
        else:
            session_start = pd.DataFrame(columns=session_groups + ['session_start_time'])

        # --- Question Started metrics ---
        q_started = df.loc[df['event_name'] == 'Question Started']
        qs_metrics = (
            q_started.groupby(session_groups, as_index=False)
            .agg(
                customer_character_count=('event_params__character_name', 'nunique'),
                character_list=('event_params__character_name',
                                lambda x: [v for v in x.dropna().tolist()]),
                average_tier=('event_params__current_tier', 'mean'),
            )
        )

        # --- Question Completed metrics ---
        q_completed = df.loc[df['event_name'] == 'Question Completed']
        qc_metrics = (
            q_completed.groupby(session_groups, as_index=False)
            .agg(average_wrong_answers=('event_params__answered_wrong', 'mean'))
        )

        # --- Wheel metrics ---
        wheel = (
            df.groupby(session_groups, as_index=False)['event_params__mini_game_ri']
              .agg(
                  Wheel_Impression=lambda x: (x == 'Daily Spin').sum(),
                  Wheel_Skips=lambda x: (x == 'spin_skipped').sum(),
              )
              .assign(Wheel_Spins=lambda d: d['Wheel_Impression'] - d['Wheel_Skips'])
        )

        # --- Ads watched ---
        ads = (
            df.groupby(session_groups, as_index=False)['event_name']
              .agg(Ads_Watched_Count=lambda x: (x == 'Ad Rewarded').sum())
        )

        # --- In-game currency ---
        try:
            gold = df.groupby(session_groups, as_index=False).apply(summarize_gold)
            if isinstance(gold.index, pd.MultiIndex):
                gold = gold.reset_index(drop=True)
        except Exception as e:
            logger.warning(f"Gold summarization failed: {e}")
            gold = pd.DataFrame(columns=session_groups)

        # --- Consumables purchased ---
        consumable = pd.DataFrame(columns=session_groups)
        if 'event_params__spent_to' in df.columns and 'shop_consumable_item' in df.columns:
            consumable = (
                df.loc[df['event_params__spent_to'] == 'Consumable Item']
                  .groupby(session_groups, as_index=False)['shop_consumable_item']
                  .agg(
                      Potions_Bought=lambda x: (x == 'Potion').sum(),
                      Incenses_Bought=lambda x: (x == 'Incense').sum(),
                      Amulets_Bought=lambda x: (x == 'Amulet').sum(),
                  )
            )

        # --- Energy spent ---
        energy = pd.DataFrame(columns=session_groups)
        if 'event_params__spent_to' in df.columns:
            energy = (
                df.loc[df['event_params__spent_to'].isin(['Cauldron', 'AliCin', 'Coffee'])]
                  .groupby(session_groups, as_index=False)['event_params__spent_to']
                  .agg(
                      AliCin_Used=lambda x: (x == 'AliCin').sum(),
                      Cauldron_Used=lambda x: (x == 'Cauldron').sum(),
                      Coffee_Used=lambda x: (x == 'Coffee').sum(),
                  )
            )

        # --- Last event per session ---
        skip_events = [
            'User Engagement', 'Screen Viewed', 'Earned Virtual Currency', 'Firebase Campaign',
            'App Removed', 'App Data Cleared', 'App Updated', 'Starting Currencies'
        ]
        df_l_sorted = df.sort_values(['event_datetime'], ascending=False)

        def pick_last_valid(g):
            non_skip = g[~g['event_name'].isin(skip_events)]
            row = non_skip.iloc[0] if len(non_skip) > 0 else g.iloc[0]
            return pd.DataFrame({
                'event_params__ga_session_id': [row['event_params__ga_session_id']],
                'user_pseudo_id': [row['user_pseudo_id']],
                'last_event_name': [row['event_name']],
                'last_event_time': [row['event_datetime']],
            })

        session_last_event = (
            df_l_sorted.groupby(session_groups, group_keys=False)
            .apply(pick_last_valid)
            .reset_index(drop=True)
        )

        # --- Merge everything ---
        result = (
            base_sessions
            .merge(session_duration, on=session_groups, how='left')
            .merge(session_start, on=session_groups, how='left')
            .merge(qs_metrics, on=session_groups, how='left')
            .merge(qc_metrics, on=session_groups, how='left')
            .merge(wheel, on=session_groups, how='left')
            .merge(ads, on=session_groups, how='left')
            .merge(gold, on=session_groups, how='left')
            .merge(consumable, on=session_groups, how='left')
            .merge(energy, on=session_groups, how='left')
            .merge(session_last_event, on=session_groups, how='left')
            .fillna({
                'average_tier': 0,
                'average_wrong_answers': 0,
                'Ads_Watched_Count': 0,
                'Wheel_Impression': 0,
                'Wheel_Skips': 0,
                'Wheel_Spins': 0,
                'Potions_Bought': 0,
                'Incenses_Bought': 0,
                'Amulets_Bought': 0,
                'AliCin_Used': 0,
                'Cauldron_Used': 0,
                'Coffee_Used': 0,
            })
        )

        # --- Derived metric ---
        if 'customer_character_count' in result.columns:
            result['bought_new_customer'] = (
                result['customer_character_count'].fillna(0).astype(int) // 3
            )
        else:
            result['bought_new_customer'] = 0

        logger.info(
            f"Session-level dataframe created with {result.shape[0]} records "
            f"and {result.shape[1]} columns."
        )
        return result

    except Exception as e:
        logger.error(f"Error in df_by_sessions: {e}", exc_info=True)
        return pd.DataFrame()



def create_df_by_users(df: pd.DataFrame) -> pd.DataFrame:
    try:
        user_groups = ['user_pseudo_id']

        # --- Ensure required columns exist ---
        required_cols = [
            'event_name', 'session_duration_seconds', 'event_date',
            'event_params__ga_session_id', 'event_params__character_name'
        ]
        missing = [c for c in required_cols if c not in df.columns]
        for c in missing:
            df[c] = None

        # Events to exclude from last_event_date computation
        exclude_last_events = [
            'App Removed', 'App Data Cleared', 'App Updated',
            'User Engagement', 'Screen Viewed', 'Firebase Campaign',
            'Starting Currencies'
        ]
        df_no_end = df[~df['event_name'].isin(exclude_last_events)]

        # --- ensure session_duration_minutes exists ---
        # --- ensure session_duration_minutes exists ---
        df['session_duration_minutes'] = df['session_duration_seconds'] / 60

        # --- sum per unique session per user ---
        df_sessions = (
            df[['user_pseudo_id', 'event_params__ga_session_id', 'session_duration_minutes']]
            .drop_duplicates(subset=['user_pseudo_id', 'event_params__ga_session_id'])
        )

        user_playtime = (
            df_sessions.groupby('user_pseudo_id', as_index=False)
                    .agg(total_playtime_minutes=('session_duration_minutes', 'sum'))
        )

        user_playtime['total_playtime_minutes'] = user_playtime['total_playtime_minutes'].round(2)

        user_df = (
            df.groupby(user_groups, as_index=False)
            .agg(
                first_event_date=('event_date', 'min'),
                total_sessions=('event_params__ga_session_id', 'nunique'),
                total_characters_opened=('event_params__character_name', 'nunique'),
                country=('geo__country', 'first') if 'geo__country' in df.columns else ('event_name', 'first'),
                install_source=('app_info__install_source', 'first') if 'app_info__install_source' in df.columns else ('event_name', 'first'),
                operating_system=('device__operating_system', 'first') if 'device__operating_system' in df.columns else ('event_name', 'first'),
                operating_system_version=('device__operating_system_version', 'first') if 'device__operating_system_version' in df.columns else ('event_name', 'first'),
                is_limited_ad_tracking=('device__is_limited_ad_tracking', 'first') if 'device__is_limited_ad_tracking' in df.columns else ('event_name', 'first'),
                device_language=('device__language', 'first') if 'device__language' in df.columns else ('event_name', 'first'),
                version=('app_info__version', 'last') if 'app_info__version' in df.columns else ('event_name', 'last'),
            )
        )

        # --- then merge into your user_df ---
        user_df = user_df.merge(user_playtime, on='user_pseudo_id', how='left')

        # --- Wheel metrics ---
        wheel = pd.DataFrame(columns=user_groups)
        if 'event_params__mini_game_ri' in df.columns:
            wheel = (
                df.groupby(user_groups, as_index=False)['event_params__mini_game_ri']
                  .agg(
                      Wheel_Impression=lambda x: (x == 'Daily Spin').sum(),
                      Wheel_Skips=lambda x: (x == 'spin_skipped').sum(),
                  )
                  .assign(Wheel_Spins=lambda d: d['Wheel_Impression'] - d['Wheel_Skips'])
            )

        # --- Count event-based actions per user ---

        event_level = (
            df[['user_pseudo_id', 'event_name', 'event_timestamp']]
            .drop_duplicates()
        )
        
        counts_df = (
            event_level.assign(
                total_ads_watched        = (event_level.event_name == 'Ad Rewarded').astype(int),
                total_questions_answered = (event_level.event_name == 'Question Completed').astype(int),
                game_ended               = (event_level.event_name == 'Game Ended').astype(int),
                tutorial_completed       = (event_level.event_params__tutorial_video == 'tutorial_video').astype(int)
                    if 'event_params__tutorial_video' in event_level.columns else 0,
                app_removed              = (event_level.event_name == 'App Removed').astype(int),
                session_started          = (event_level.event_name == 'Session Started').astype(int),
            )
            .groupby('user_pseudo_id', as_index=False)
            .sum(numeric_only=True)
)
        # --- Last event per user ---
        last_event = (
            df_no_end
            .sort_values(['event_date'])
            .drop_duplicates(subset=user_groups, keep='last')
            [[*user_groups, 'event_date', 'event_name']]
            .rename(columns={'event_date': 'last_event_date',
                             'event_name': 'last_event_name'})
        )

        # --- Merge everything ---
        user_df = (
            user_df
            .merge(counts_df, on=user_groups, how='left')
            .merge(last_event, on=user_groups, how='left')
            .merge(wheel, on=user_groups, how='left')
            .fillna({
                'total_ads_watched': 0,
                'total_questions_answered': 0,
                'game_ended': 0,
            })
        )

        user_df['passed_10_min'] = (user_df['total_playtime_minutes'] >= 10).astype(int)

        logger.info(
            f"User-level dataframe created with {user_df.shape[0]} records and "
            f"{user_df.shape[1]} columns."
        )
        return user_df

    except Exception as e:
        logger.error(f"Error in df_by_users: {e}", exc_info=True)
        return pd.DataFrame()


def create_df_by_questions(df: pd.DataFrame) -> pd.DataFrame:
    try:
        question_groups = [
            'question_address',
            'event_params__character_name',
            'event_params__current_tier',
            'event_params__current_question_index',
            'event_params__ga_session_id',
        ]

        # --- Ensure required columns exist ---
        required_cols = [
            'event_name',
            'event_params__spent_to',
            'event_params__menu_name',
            'event_params__answered_wrong',
            'shop_consumable_item',
        ]
        if not all(col in df.columns for col in required_cols + question_groups):
            logger.warning("Missing required columns for df_by_questions.")
            return pd.DataFrame()

        # --- Boolean masks ---
        masks = {
            'question_started': df['event_name'].eq('Question Started'),
            'potions_bought': df['shop_consumable_item'].eq('Potion'),
            'incense_bought': df['shop_consumable_item'].eq('Incense'),
            'amulet_bought': df['shop_consumable_item'].eq('Amulet'),
            'alicin_used': df['event_params__spent_to'].eq('AliCin'),
            'coffee_used': df['event_params__spent_to'].eq('Coffee'),
            'cauldron_used': df['event_params__spent_to'].eq('Cauldron'),
            'scroll_opened': (
                df['event_name'].eq('Menu Opened')
                & df['event_params__menu_name'].eq('Scroll Menu')
            ),
            'answered_correct': df['event_name'].eq('Question Completed'),
            'answered_wrong': df['event_params__answered_wrong'].fillna(0),
            'ads_watched': df['event_name'].eq('Ad Rewarded').fillna(0),
        }

        # --- Convert masks to DataFrame of ints ---
        temp = pd.DataFrame(
            {k: v.astype(int) if v.dtype == bool else v for k, v in masks.items()}
        ).reset_index(drop=True)

        # --- Combine and aggregate ---
        question_df = (
            pd.concat([df[question_groups].reset_index(drop=True), temp], axis=1)
            .groupby(question_groups, as_index=False)
            .sum()
        )

        # --- Derived ratios ---
        def safe_ratio(numer, denom):
            return (numer / denom.replace(0, pd.NA)).fillna(0).round(3)

        question_df['wrong_answer_ratio'] = safe_ratio(
            question_df['answered_wrong'], question_df['question_started']
        )
        question_df['ads_watch_ratio'] = safe_ratio(
            question_df['ads_watched'], question_df['question_started']
        )
        question_df['alicin_use_ratio'] = safe_ratio(
            question_df['alicin_used'], question_df['question_started']
        )
        question_df['coffee_use_ratio'] = safe_ratio(
            question_df['coffee_used'], question_df['question_started']
        )
        question_df['cauldron_use_ratio'] = safe_ratio(
            question_df['cauldron_used'], question_df['question_started']
        )
        question_df['scroll_use_ratio'] = safe_ratio(
            question_df['scroll_opened'], question_df['question_started']
        )

        logger.info(
            f"Question-level dataframe created with {question_df.shape[0]} records and "
            f"{question_df.shape[1]} columns."
        )
        return question_df

    except Exception as e:
        logger.error(f"Error in df_by_questions: {e}", exc_info=True)
        return pd.DataFrame()


def create_df_by_date(df: pd.DataFrame) -> pd.DataFrame:
    try:
        # --- Ensure required columns exist ---
        required_cols = [
            'event_date', 'ts_weekday', 'user_pseudo_id', 'event_name',
            'device__operating_system', 'event_params__ga_session_id'
        ]
        if not all(col in df.columns for col in required_cols):
            logger.warning("Missing required columns for df_by_date.")
            return pd.DataFrame()

        # --- Base date-level aggregations ---
        date_df = (
            df.groupby(['event_date'])
            .agg(
                weekday=('ts_weekday', 'first'),
                unique_users=('user_pseudo_id', 'nunique'),
                new_users=('event_name', lambda x: (x == 'First Open').sum()),
                android_users=('device__operating_system', lambda x: (x == 'ANDROID').sum()),
                ios_users=('device__operating_system', lambda x: (x == 'IOS').sum()),
                uninstall_count=('event_name', lambda x: (x == 'App Removed').sum()),
                unique_sessions=('event_params__ga_session_id', 'nunique'),
                ads_watched=('event_name', lambda x: (x == 'Ad Rewarded').sum()),
                questions_started=('event_name', lambda x: (x == 'Question Started').sum()),
                questions_completed=('event_name', lambda x: (x == 'Question Completed').sum()),
            )
            .reset_index()
        )

        # --- Ads network breakdown ---
        ads_network_df = pd.DataFrame(columns=['event_date'])
        if 'event_params__ad_network' in df.columns:
            ads_network_df = (
                df.groupby(['event_date', 'event_params__ad_network'])
                .size()
                .unstack(fill_value=0)
                .add_prefix('nwk_')
                .reset_index()
            )

        # --- Ads unit breakdown ---
        ads_unit_df = pd.DataFrame(columns=['event_date'])
        if 'event_params__ad_unit_id' in df.columns:
            ads_unit_df = (
                df.groupby(['event_date', 'event_params__ad_unit_id'])
                .size()
                .unstack(fill_value=0)
                .add_prefix('unt_')
                .reset_index()
            )

        # --- Ads instance breakdown ---
        ads_instance_df = pd.DataFrame(columns=['event_date'])
        if 'event_params__ad_instance' in df.columns:
            ads_instance_df = (
                df.groupby(['event_date', 'event_params__ad_instance'])
                .size()
                .unstack(fill_value=0)
                .add_prefix('ins_')
                .reset_index()
            )

        # --- Merge everything ---
        result = (
            date_df
            .merge(ads_network_df, on='event_date', how='left')
            .merge(ads_unit_df, on='event_date', how='left')
            .merge(ads_instance_df, on='event_date', how='left')
            .fillna(0)
        )

        logger.info(
            f"Date-level dataframe created with {result.shape[0]} records and "
            f"{result.shape[1]} columns."
        )
        return result

    except Exception as e:
        logger.error(f"Error in df_by_date: {e}", exc_info=True)
        return pd.DataFrame()

def create_df_technical_events(df: pd.DataFrame) -> pd.DataFrame:
    try:
        # --- Ensure required columns exist ---
        required_cols = [
            'user_pseudo_id',
            'event_params__ga_session_id',
            'event_datetime',
            'app_info__version',
            'device__mobile_marketing_name',
            'device__operating_system_version',
            'event_name',
        ]
        if not all(col in df.columns for col in required_cols):
            logger.warning("Missing required columns for df_technical_events.")
            return pd.DataFrame()

        # --- Sort by user, session, and event time ---
        df = df.sort_values(required_cols)

        # --- Create previous event columns within the same session ---
        df['prev_event_name'] = (
            df.groupby(['user_pseudo_id', 'event_params__ga_session_id'])['event_name'].shift(1)
        )
        if 'event_params__menu_name' in df.columns:
            df['prev_event_menu'] = (
                df.groupby(['user_pseudo_id', 'event_params__ga_session_id'])['event_params__menu_name'].shift(1)
            )
        else:
            df['prev_event_menu'] = pd.NA

        # --- Filter technical events ---
        tech_events = df[df['event_name'].isin(['App Exception', 'Ad Load Failed'])].copy()

        logger.info(
            f"Technical events dataframe created with {tech_events.shape[0]} records and "
            f"{tech_events.shape[1]} columns."
        )

        # --- Keep relevant columns safely ---
        keep_cols = [
            'event_datetime',
            'event_name',
            'user_pseudo_id',
            'event_params__ga_session_id',
            'app_info__version',
            'device__mobile_marketing_name',
            'device__operating_system_version',
            'prev_event_name',
            'prev_event_menu',
            'event_params__ad_network',
            'event_params__ad_instance',
            'event_params__ad_id',
            'event_params__ad_error_code',
            'event_server_delay_seconds',
        ]
        existing_cols = [c for c in keep_cols if c in tech_events.columns]
        return tech_events[existing_cols]

    except Exception as e:
        logger.error(f"Error in df_technical_events: {e}", exc_info=True)
        return pd.DataFrame()


def create_df_by_ads(df: pd.DataFrame) -> pd.DataFrame:
    try:
        # --- Ensure required columns exist ---
        if 'event_name' not in df.columns:
            logger.warning("Missing 'event_name' column for df_by_ads.")
            return pd.DataFrame()

        # --- Filter ad-related events ---
        ad_related_mask = df['event_name'].isin([
            'Ad Loaded', 'Ad Closed', 'Ad Displayed',
            'Ad Rewarded', 'Ad Load Failed', 'Ad Clicked'
        ])
        ads = df[ad_related_mask].copy()

        # --- Columns we want to keep ---
        columns = [
            'event_datetime',
            'event_params__ga_session_id',
            'event_name',
            'event_params__ad_id',
            'event_params__ad_unit_id',
            'event_params__ad_network',
            'event_params__ad_placement',
            'event_params__ad_reward_type',
            'event_params__ad_instance',
            'event_params__ad_error_code',
            'event_params__character_name',
            'event_params__current_tier',
            'event_params__current_question_index',
            'question_address',
            'ts_weekday',
            'ts_daytime_named',
            'app_info__version',
            'geo__country',
            'device__operating_system',
            'event_server_delay_seconds',
        ]

        # --- Keep only existing columns ---
        existing_cols = [c for c in columns if c in ads.columns]
        ads = ads[existing_cols]

        # --- Fill NA for selected columns if they exist ---
        fill_na = [
            'event_params__ad_network',
            'event_params__ad_placement',
            'event_params__ad_reward_type',
            'event_params__ad_instance',
        ]
        for col in fill_na:
            if col in ads.columns:
                ads[col] = ads[col].fillna('Unknown/Missing')

        logger.info(
            f"Rewarded ads dataframe created with {ads.shape[0]} records and "
            f"{ads.shape[1]} columns."
        )
        return ads

    except Exception as e:
        logger.error(f"Error in df_by_ads: {e}", exc_info=True)
        return pd.DataFrame()