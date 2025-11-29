import pandas as pd
from config.logging import get_logger

logger = get_logger(__name__)

def forward_fill_progress(df: pd.DataFrame, context=None) -> pd.DataFrame:
    try:
        df_sorted = df.sort_values(by=['user_pseudo_id', 'event_params__ga_session_id', 'event_datetime'])
        cols_to_fill = [
            'event_params__character_name',
            'event_params__current_tier',
            'event_params__current_qi',
        ]
        df_sorted[cols_to_fill] = (
            df_sorted
            .groupby(['user_pseudo_id', 'event_params__ga_session_id'])[cols_to_fill]
            .ffill()
        )
        df.loc[df_sorted.index, cols_to_fill] = df_sorted[cols_to_fill]
        logger.info(f"Character names, tiers, and question indices forward-filled for {df['event_params__ga_session_id'].nunique()} unique sessions.")
    except Exception as e:
        logger.error(f"Error in forward_fill_progress: {e}", exc_info=True)
    return df


def question_cumulative_qi(df: pd.DataFrame, context=None) -> pd.DataFrame:
    try:
        df['cumulative_question_index'] = pd.to_numeric(df['event_params__current_question_index'], errors='coerce')
        df.loc[(df['event_params__current_tier'] == 2) & (df['event_params__character_name'] == 't'), 'cumulative_question_index'] += 12
        df.loc[(df['event_params__current_tier'] == 2) & (df['event_params__character_name'] != 't'), 'cumulative_question_index'] += 16
        df.loc[(df['event_params__current_tier'] == 3) & (df['event_params__character_name'] == 't'), 'cumulative_question_index'] += 24
        df.loc[(df['event_params__current_tier'] == 3) & (df['event_params__character_name'] != 't'), 'cumulative_question_index'] += 28
        df.loc[(df['event_params__current_tier'] == 4) & (df['event_params__character_name'] == 't'), 'cumulative_question_index'] += 36
        df.loc[(df['event_params__current_tier'] == 4) & (df['event_params__character_name'] != 't'), 'cumulative_question_index'] += 40
        df.loc[df['event_params__current_tier'].isna(), 'cumulative_question_index'] = pd.NA
        logger.info(f"Cumulative question index calculated for {df['cumulative_question_index'].notna().sum()} rows.")
    except Exception as e:
        logger.error(f"Error in question_cumulative_qi: {e}", exc_info=True)
    return df


def mini_game_features(df: pd.DataFrame, context=None) -> pd.DataFrame:
    try:
        col = 'event_params__mini_game_ri'
        mask = df[col].str.startswith('maze_hand', na=False)
        parts = df.loc[mask, col].str.split('_', expand=True)
        gender_hand = parts[2].str.extract(r'(?P<Gender>Woman|Man)Hand(?P<Hand>\w+)')
        levels = parts[5]
        df.loc[mask, 'maze_gender'] = gender_hand['Gender']
        df.loc[mask, 'maze_hand'] = gender_hand['Hand']
        df.loc[mask, 'maze_level'] = levels
        logger.info(f"Extracted maze hand data for {mask.sum()} rows.")
    except Exception as e:
        logger.error(f"Error in mini_game_features: {e}", exc_info=True)
    return df


def mini_game_reward_split(df: pd.DataFrame, context=None) -> pd.DataFrame:
    try:
        col = 'event_params__mini_game_ri'
        mask = df[col].str.startswith('buff', na=False)
        parts = df.loc[mask, col].str.split('_', expand=True)
        buff_type = parts[2].str.extract(r'(?P<BuffType>\w+)')
        buff_gift = parts[3].str.extract(r'(?P<BuffGift>\w+)')
        buff_gold = parts[5].str.extract(r'(?P<BuffGold>\w+)')
        df.loc[mask, 'buff_type'] = buff_type['BuffType']
        df.loc[mask, 'buff_gift'] = buff_gift['BuffGift'].str.lower() == 'true'
        df.loc[mask, 'buff_gold'] = buff_gold['BuffGold'].str.lower() == 'true'
        logger.info(f"Extracted buff data for {mask.sum()} rows.")
    except Exception as e:
        logger.error(f"Error in mini_game_reward_split: {e}", exc_info=True)
    return df


def mini_game_buffs(df: pd.DataFrame, context=None) -> pd.DataFrame:
    try:
        col = 'event_params__mini_game_ri'
        mask = df[col].str.startswith('earned_buff', na=False)
        parts = df.loc[mask, col].str.split('_', expand=True)
        buff_type = parts[2].str.extract(r'(?P<BuffType>\w+)')
        df.loc[mask, 'earned_buff_type'] = buff_type['BuffType']
        logger.info(f"Extracted earned buff data for {mask.sum()} rows.")
    except Exception as e:
        logger.error(f"Error in mini_game_buffs: {e}", exc_info=True)
    return df


def mini_game_dolls(df: pd.DataFrame, context=None) -> pd.DataFrame:
    try:
        col = 'event_params__spent_to'
        mask = df[col].str.contains('doll', na=False)
        parts = df.loc[mask, col].str.split('doll', expand=True)
        df.loc[mask, 'doll_name'] = parts[0].str.strip()
        df.loc[mask, col] = 'Doll'
        logger.info(f"Extracted doll data for {mask.sum()} rows.")
    except Exception as e:
        logger.error(f"Error in mini_game_dolls: {e}", exc_info=True)
    return df


def currency_define_permanent(df: pd.DataFrame, context=None) -> pd.DataFrame:
    try:
        col = 'event_params__spent_to'
        mask = df[col].str.contains('dreamcatcher|catcollar|library1|library2|bugspray|schedule|crystal|horseshoe', na=False)
        df.loc[mask, 'shop_permanent_item'] = df.loc[mask, col].str.extract(r'(dreamcatcher|catcollar|library1|library2|bugspray|schedule|crystal|horseshoe)')[0]
        df.loc[mask, col] = 'Permanent Item'
        logger.info(f"Extracted permanent shop item data for {mask.sum()} rows.")
    except Exception as e:
        logger.error(f"Error in currency_define_permanent: {e}", exc_info=True)
    return df


def currency_define_consumable(df: pd.DataFrame, context=None) -> pd.DataFrame:
    try:
        col = 'event_params__spent_to'
        mask = df[col].str.contains('potion|ıncense|amulet|incense', na=False)
        df.loc[mask, 'shop_consumable_item'] = df.loc[mask, col].str.extract(r'(potion|ıncense|amulet|incense)')[0]
        df.loc[mask, col] = 'Consumable Item'
        logger.info(f"Extracted consumable shop item data for {mask.sum()} rows.")
    except Exception as e:
        logger.error(f"Error in currency_define_consumable: {e}", exc_info=True)
    return df


def currency_define_board(df: pd.DataFrame, context=None) -> pd.DataFrame:
    try:
        col = 'event_params__spent_to'
        mask = (~df[col].isin(['Doll', 'Crystal Ball', 'Permanent Item', 'Consumable Item'])) & \
            (df['event_params__where_its_spent'].isin(['board', 'board_item']))
        df.loc[mask, 'board_item'] = df.loc[mask, col]
        df.loc[mask, col] = 'Board Item'
        logger.info(f"Extracted board item data for {mask.sum()} rows.")
    except Exception as e:
        logger.error(f"Error in currency_define_board: {e}", exc_info=True)
    return df


def currency_define_keys(df: pd.DataFrame, context=None) -> pd.DataFrame:
    try:
        df.loc[df['event_params__spent_to'] == 'key', 'event_params__spent_to'] = 'Key'
        logger.info(f"Defined keys in spent_to for {df['event_params__spent_to'].eq('Key').sum()} rows.")
    except Exception as e:
        logger.error(f"Error in currency_define_keys: {e}", exc_info=True)
    return df


def question_addressable_index(df: pd.DataFrame, context=None) -> pd.DataFrame:
    try:
        df['question_address'] = df['event_params__character_name'] + ' - T: ' + df['event_params__current_tier'].astype(str) + ' - Q: ' + df['event_params__current_question_index'].astype(str)
        logger.info(f"Question address created for {df['question_address'].notna().sum()} rows.")
    except Exception as e:
        logger.error(f"Error in question_addressable_index: {e}", exc_info=True)
    return df

def question_answer_wrong_zeros(df: pd.DataFrame, context=None) -> pd.DataFrame:
    try:
        mask = (df['event_name'] == 'Question Completed') & (df['event_params__answered_wrong'].isna())
        df.loc[mask, 'event_params__answered_wrong'] = 0
        logger.info("Filled NaN values in 'event_params__answered_wrong' with 0 for 'Question Completed' events.")
    except Exception as e:
        logger.error(f"Error in question_answer_wrong_zeros: {e}", exc_info=True)
    return df

