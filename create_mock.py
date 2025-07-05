import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

row_count = 1022563
np.random.seed(42)
rng = np.random.default_rng(42)

def random_string(length=8):
    return ''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=length))

def random_datetime(start, end):
    return pd.to_datetime(start + timedelta(seconds=random.randint(0, int((end - start).total_seconds()))), utc=True)

start_date = datetime(2024, 1, 1)
end_date = datetime(2025, 1, 1)

df = pd.DataFrame({
    "event_name": np.random.choice(["app_open", "level_start", "purchase", "ad_click"], row_count),
    "event_bundle_sequence_id": np.arange(row_count),
    "user_pseudo_id": [f"user_{i}" for i in range(row_count)],
    "stream_id": np.random.randint(0, 10000, row_count),
    "platform": np.random.choice(["ANDROID", "IOS", "WEB"], row_count),
    "is_active_user": np.random.choice([True, False], row_count),
    "batch_event_index": np.random.randint(0, 50, row_count),
    "event_params__ga_session_id": np.random.randint(100000, 999999, row_count),
    "event_params__firebase_screen_id": np.where(np.random.rand(row_count) < 0.99, np.random.randn(row_count)*1000, np.nan),
    "event_params__ga_session_number": np.where(np.random.rand(row_count) < 0.999, np.random.randint(1,10,row_count), np.nan),
    "event_params__ad_platform": np.where(np.random.rand(row_count) < 0.02, np.random.choice(["admob", "facebook"], row_count), None),
    "event_params__firebase_screen_class": np.where(np.random.rand(row_count) < 0.99, np.random.choice(["MainActivity", "GameActivity"], row_count), None),
    "event_params__ad_shown_where": np.where(np.random.rand(row_count) < 0.02, np.random.choice(["menu", "reward"], row_count), None),
    "event_params__ad_unit_id": np.where(np.random.rand(row_count) < 0.02, [random_string(10) for _ in range(row_count)], None),
    "event_params__engaged_session_event": np.where(np.random.rand(row_count) < 0.99, np.random.choice([0,1], row_count), np.nan),
    "event_params__firebase_event_origin": np.random.choice(["auto", "user"], row_count),
    "user__first_open_time": pd.date_range(start="2022-01-01", periods=row_count, freq="min").astype(str),
    "user__ga_session_number": np.where(np.random.rand(row_count) < 0.999, np.random.randint(1,100,row_count), np.nan),
    "user__ga_session_id": np.where(np.random.rand(row_count) < 0.999, np.random.randint(1000000,9999999,row_count), np.nan),
    "privacy_info__analytics_storage": np.random.choice(["granted","denied"], row_count),
    "privacy_info__ads_storage": np.random.choice(["granted","denied"], row_count),
    "device__category": np.random.choice(["mobile","tablet"], row_count),
    "device__mobile_brand_name": np.where(np.random.rand(row_count) < 0.996, np.random.choice(["Samsung", "Apple", "Xiaomi"], row_count), None),
    "device__mobile_model_name": np.where(np.random.rand(row_count) < 0.996, [random_string(6) for _ in range(row_count)], None),
    "device__mobile_os_hardware_model": [random_string(5) for _ in range(row_count)],
    "device__operating_system": np.random.choice(["Android","iOS"], row_count),
    "device__operating_system_version": [f"{random.randint(1,14)}.{random.randint(0,9)}" for _ in range(row_count)],
    "device__advertising_id": [None]*row_count,
    "device__language": np.random.choice(["en","es","de","fr"], row_count),
    "device__is_limited_ad_tracking": np.random.choice(["true","false"], row_count),
    "geo__city": np.random.choice(["New York", "Berlin", "Tokyo"], row_count),
    "geo__country": np.random.choice(["US", "DE", "JP"], row_count),
    "geo__continent": np.random.choice(["NA","EU","AS"], row_count),
    "geo__region": np.random.choice(["NY","BE","TO"], row_count),
    "geo__sub_continent": np.random.choice(["Northern America","Western Europe","Eastern Asia"], row_count),
    "geo__metro": np.random.choice(["NYC","BER","TYO"], row_count),
    "app_info__id": [random_string(10) for _ in range(row_count)],
    "app_info__version": [f"{random.randint(1,5)}.{random.randint(0,9)}.{random.randint(0,9)}" for _ in range(row_count)],
    "app_info__install_store": [None]*row_count,
    "app_info__firebase_app_id": [random_string(12) for _ in range(row_count)],
    "app_info__install_source": np.random.choice(["playstore","appstore"], row_count),
    "event_params__firebase_conversion": np.where(np.random.rand(row_count) < 0.17, np.random.randint(0,2,row_count), np.nan),
    "event_params__previous_first_open_count": np.where(np.random.rand(row_count) < 0.001, np.random.randint(1,5,row_count), np.nan),
    "event_params__menu_name": np.where(np.random.rand(row_count) < 0.27, np.random.choice(["settings","inventory","shop"], row_count), None),
    "event_params__entrances": np.where(np.random.rand(row_count) < 0.007, np.random.uniform(1,10,row_count), np.nan),
    "event_params__ad_network": np.where(np.random.rand(row_count) < 0.01, np.random.choice(["admob","facebook"], row_count), None),
    "event_params__ad_format": np.where(np.random.rand(row_count) < 0.01, np.random.choice(["banner","rewarded"], row_count), None),
    "event_params__session_engaged": np.where(np.random.rand(row_count) < 0.007, np.random.uniform(0,1,row_count), np.nan),
    "event_params__current_qi": np.where(np.random.rand(row_count) < 0.4, np.random.randint(1,10,row_count), pd.NA),
    "event_params__character_name": np.where(np.random.rand(row_count) < 0.96, np.random.choice(["hero1","hero2","hero3"], row_count), None),
    "event_params__current_tier": np.where(np.random.rand(row_count) < 0.96, np.random.randint(1,5,row_count), pd.NA),
    "event_params__mini_game_ri": np.where(np.random.rand(row_count) < 0.04, [random_string(5) for _ in range(row_count)], None),
    "event_params__mini_game_name": np.where(np.random.rand(row_count) < 0.04, np.random.choice(["quiz","maze","match"], row_count), None),
    "event_params__answered_wrong": np.where(np.random.rand(row_count) < 0.05, np.random.randint(0,2,row_count), np.nan),
    "event_params__where_its_earned": np.where(np.random.rand(row_count) < 0.19, np.random.choice(["mission","quest","bonus"], row_count), None),
    "event_params__currency_name": np.where(np.random.rand(row_count) < 0.22, np.random.choice(["gold","crystal","token"], row_count), None),
    "event_params__earned_amount": np.where(np.random.rand(row_count) < 0.19, np.random.uniform(1,500,row_count), np.nan),
    "event_params__how_its_earned": np.where(np.random.rand(row_count) < 0.19, np.random.choice(["defeat_enemy","complete_level"], row_count), None),
    "event_params__spent_amount": np.where(np.random.rand(row_count) < 0.03, np.random.uniform(1,300,row_count), np.nan),
    "event_params__where_its_spent": np.where(np.random.rand(row_count) < 0.03, np.random.choice(["shop","upgrade"], row_count), None),
    "event_params__spent_to": np.where(np.random.rand(row_count) < 0.03, np.random.choice(["item","buff"], row_count), None),
    "event_params__firebase_error": np.where(np.random.rand(row_count) < 0.008, np.random.randint(0,10,row_count), np.nan),
    "event_params__fatal": np.where(np.random.rand(row_count) < 0.0005, np.random.randint(0,2,row_count), np.nan),
    "event_params__timestamp": np.where(np.random.rand(row_count) < 0.0005, rng.integers(1e9, 1e10, size=row_count, dtype=np.int64), np.nan,),
    "time_delta": np.where(np.random.rand(row_count) < 0.98, np.random.uniform(0,1000,row_count), np.nan),
    "event_datetime": [random_datetime(start_date, end_date) for _ in range(row_count)],
    "event_date": [random_datetime(start_date, end_date) for _ in range(row_count)],
    "event_time": [f"{random.randint(0,23)}:{random.randint(0,59):02}:{random.randint(0,59):02}" for _ in range(row_count)],
    "event_previous_date": np.where(np.random.rand(row_count) < 0.98, [random_datetime(start_date, end_date) for _ in range(row_count)], pd.NaT),
    "event_previous_time": np.where(np.random.rand(row_count) < 0.98, [f"{random.randint(0,23)}:{random.randint(0,59):02}:{random.randint(0,59):02}" for _ in range(row_count)], None),
    "event_first_touch_date": [random_datetime(start_date, end_date) for _ in range(row_count)],
    "event_first_touch_time": [f"{random.randint(0,23)}:{random.randint(0,59):02}:{random.randint(0,59):02}" for _ in range(row_count)],
    "user__first_open_date": [random_datetime(start_date, end_date) for _ in range(row_count)],
    "device__time_zone_offset_hours": np.random.uniform(-12,14,row_count),
    "event_params__engagement_time_seconds": np.where(np.random.rand(row_count) < 0.026, np.random.uniform(0,300,row_count), np.nan),
    "event_server_delay_seconds": np.random.uniform(0,5,row_count),
    "event_params__time_spent_seconds": np.where(np.random.rand(row_count) < 0.35, np.random.uniform(1,600,row_count), np.nan),
    "ts_weekday": pd.Categorical(np.random.choice(["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"], row_count)),
    "ts_local_time": [random_datetime(start_date, end_date) for _ in range(row_count)],
    "ts_hour": np.random.randint(0,24,row_count).astype(np.int32),
    "ts_daytime_named": np.random.choice(["Morning","Afternoon","Evening","Night"], row_count),
    "ts_is_weekend": np.random.choice(["yes","no"], row_count),
    "event_params__current_question_index": np.where(np.random.rand(row_count) < 0.96, np.random.randint(0,50,row_count), pd.NA),
    "cumulative_question_index": np.where(np.random.rand(row_count) < 0.4, np.random.randint(0,500,row_count), pd.NA),
    "inferred_session_id": np.random.randint(1e6,1e9,row_count),
    "session_duration_seconds": np.random.uniform(10,3600,row_count),
    "maze_gender": np.where(np.random.rand(row_count) < 0.004, np.random.choice(["male","female"], row_count), None),
    "maze_hand": np.where(np.random.rand(row_count) < 0.004, np.random.choice(["left","right"], row_count), None),
    "maze_level": np.where(np.random.rand(row_count) < 0.004, np.random.choice(["easy","medium","hard"], row_count), None),
    "buff_type": np.where(np.random.rand(row_count) < 0.006, np.random.choice(["speed","shield"], row_count), None),
    "buff_gift": np.where(np.random.rand(row_count) < 0.006, np.random.choice(["gift1","gift2"], row_count), None),
    "buff_gold": np.where(np.random.rand(row_count) < 0.006, np.random.choice(["100","200"], row_count), None),
    "earned_buff_type": np.where(np.random.rand(row_count) < 0.001, np.random.choice(["speed","shield"], row_count), None),
    "doll_name": np.where(np.random.rand(row_count) < 0.0015, np.random.choice(["doll1","doll2"], row_count), None),
    "spent_in_crystal": np.where(np.random.rand(row_count) < 0.008, np.random.choice(["crystal1","crystal2"], row_count), None),
    "shop_permanent_item": np.where(np.random.rand(row_count) < 0.003, np.random.choice(["item1","item2"], row_count), None),
    "shop_consumable_item": np.where(np.random.rand(row_count) < 0.006, np.random.choice(["consumable1","consumable2"], row_count), None),
    "board_item": np.where(np.random.rand(row_count) < 0.012, np.random.choice(["board1","board2"], row_count), None),
    "question_correct_incorrect": np.where(np.random.rand(row_count) < 0.18, np.random.choice(["correct","incorrect"], row_count), None),
    "question_address": np.where(np.random.rand(row_count) < 0.96, [random_string(15) for _ in range(row_count)], None)
})

print(df.info())
print(df.head())

# Uncomment below if you want to save to file:
df.to_csv("mock_dataset.csv", index=False)
