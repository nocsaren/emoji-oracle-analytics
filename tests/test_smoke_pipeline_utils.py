import pandas as pd


def test_flatten_dataframe_smoke_handles_nested_and_empty_items():
    from pipeline.utils.flattening_functions import flatten_dataframe

    df = pd.DataFrame(
        [
            {
                "event_date": "20251223",
                "event_timestamp": 123,
                "event_name": "test",
                "user_pseudo_id": "u1",
                "event_params": [{"key": "count", "value": {"int_value": 0}}],
                "user_properties": [{"key": "level", "value": {"int_value": 5}}],
                "privacy_info": {"ads_storage": "granted"},
                "device": '{"category": "mobile"}',
                "geo": {"country": "US"},
                # commonly a list; empty should not raise
                "items": [],
            }
        ]
    )

    out = flatten_dataframe(df)
    assert len(out) == 1
    assert out.loc[0, "event_params.count"] == 0
    assert out.loc[0, "user.level"] == 5
    assert out.loc[0, "privacy_info.ads_storage"] == "granted"
    assert out.loc[0, "device.category"] == "mobile"


def test_add_durations_smoke():
    from pipeline.utils.time_and_date_functions import add_durations

    df = pd.DataFrame(
        [
            {
                "user_pseudo_id": "u1",
                "event_params__ga_session_id": 1,
                "event_name": "session_start",
                "event_datetime": pd.Timestamp("2025-12-23T00:00:00Z"),
            },
            {
                "user_pseudo_id": "u1",
                "event_params__ga_session_id": 1,
                "event_name": "some_event",
                "event_datetime": pd.Timestamp("2025-12-23T00:00:30Z"),
            },
        ]
    )

    out = add_durations(df)
    assert "session_duration_seconds" in out.columns
    assert out["session_duration_seconds"].notna().any()


def test_create_df_by_sessions_smoke():
    from pipeline.utils.split_functions import create_df_by_sessions

    # Minimal rows + columns to avoid KeyErrors in groupby/utility code.
    df = pd.DataFrame(
        [
            {
                "user_pseudo_id": "u1",
                "event_params__ga_session_id": 1,
                "event_datetime": pd.Timestamp("2025-12-23T00:00:00Z"),
                "event_name": "Question Started",
                "session_duration_seconds": 30.0,
                "session_start_time": pd.Timestamp("2025-12-23T00:00:00Z"),
                "event_params__character_name": "t",
                "event_params__current_tier": 1,
                "event_params__answered_wrong": 0,
                "event_params__mini_game_ri": None,
                "shop_consumable_item": None,
                "event_params__spent_to": None,
                "event_params__currency_name": None,
                "event_params__earned_amount": 0,
                "event_params__spent_amount": 0,
                "event_params__gold": 0,
            },
            {
                "user_pseudo_id": "u1",
                "event_params__ga_session_id": 1,
                "event_datetime": pd.Timestamp("2025-12-23T00:00:20Z"),
                "event_name": "Question Completed",
                "session_duration_seconds": 30.0,
                "session_start_time": pd.Timestamp("2025-12-23T00:00:00Z"),
                "event_params__character_name": "t",
                "event_params__current_tier": 1,
                "event_params__answered_wrong": 1,
                "event_params__mini_game_ri": None,
                "shop_consumable_item": None,
                "event_params__spent_to": None,
                "event_params__currency_name": "Gold",
                "event_params__earned_amount": 100,
                "event_params__spent_amount": 0,
                "event_params__gold": 0,
            },
        ]
    )

    out = create_df_by_sessions(df)
    # Smoke check: should return a DataFrame (possibly empty if filtering removes rows)
    assert isinstance(out, pd.DataFrame)


def test_create_df_by_questions_smoke():
    from pipeline.utils.split_functions import create_df_by_questions

    df = pd.DataFrame(
        [
            {
                "user_pseudo_id": "u1",
                "event_params__ga_session_id": 1,
                "event_name": "Question Started",
                "event_params__character_name": "t",
                "event_params__current_tier": 1,
                "event_params__current_question_index": 1,
                "event_params__menu_name": None,
                "event_params__answered_wrong": 0,
                "shop_consumable_item": None,
                "event_params__spent_to": None,
            },
            {
                "user_pseudo_id": "u1",
                "event_params__ga_session_id": 1,
                "event_name": "Question Completed",
                "event_params__character_name": "t",
                "event_params__current_tier": 1,
                "event_params__current_question_index": 1,
                "event_params__menu_name": None,
                "event_params__answered_wrong": 1,
                "shop_consumable_item": None,
                "event_params__spent_to": None,
            },
            # denom==0 case for ratios
            {
                "user_pseudo_id": "u2",
                "event_params__ga_session_id": 2,
                "event_name": "Ad Rewarded",
                "event_params__character_name": "t",
                "event_params__current_tier": 1,
                "event_params__current_question_index": 2,
                "event_params__menu_name": None,
                "event_params__answered_wrong": 0,
                "shop_consumable_item": None,
                "event_params__spent_to": None,
            },
        ]
    )

    out = create_df_by_questions(df)
    assert isinstance(out, pd.DataFrame)
    if not out.empty:
        assert "wrong_answer_ratio" in out.columns
        assert "ads_watch_ratio" in out.columns
