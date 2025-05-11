
column_order = [
    'event_name',

    # Dates (event.*)
    'event_date', 
    'event_time', 
    'event_previous_date', 
    'event_previous_time',
    'event_first_touch_date', 
    'event_first_touch_time',

    # Top-level IDs
    'event_bundle_sequence_id', 
    'user_id', 
    'user_pseudo_id',

    # user.*
    'user.first_open_date',
    'user.first_open_time', 
    'user.ga_session_id', 
    'user.ga_session_number',

    # app_info.*
    'app_info.id', 
    'app_info.firebase_app_id', 
    'app_info.version',
    'app_info.install_store', 
    'app_info.install_source',

    # device.*
    'device.advertising_id', 
    'device.vendor_id',
    'device.category', 
    'device.mobile_brand_name', 
    'device.mobile_model_name',
    'device.mobile_marketing_name', 
    'device.mobile_os_hardware_model',
    'device.operating_system', 
    'device.operating_system_version',
    'device.language', 
    'device.is_limited_ad_tracking',
    'device.browser', 
    'device.browser_version', 
    'device.web_info',
    'device.time_zone_offset_hours',

    # geo.*
    'geo.city', 
    'geo.country', 
    'geo.continent', 
    'geo.region',
    'geo.sub_continent', 
    'geo.metro',

    # traffic_source.*
    'traffic_source.name', 
    'traffic_source.medium', 
    'traffic_source.source',

    # collected_traffic_source (top-level)
    'collected_traffic_source',

    # event_params.* IDs first
    'event_params.ga_session_id', 
    'event_params.firebase_screen_id', 
    'event_params.ad_unit_id',
    'event_params.ad_format',
    'event_params.ad_network',
    'event_params.ad_platform',
    'event_params.ad_shown_where',
    'event_params.answered_wrong',
    'event_params.character_name',
    'event_params.current_qi',
    'event_params.current_question_index',
    'event_params.current_tier',
    'event_params.earned_amount',
    'event_params.engaged_session_event',
    'event_params.engagement_time_seconds',
    'event_params.entrances',
    'event_params.firebase_conversion',
    'event_params.firebase_error',
    'event_params.firebase_event_origin',
    'event_params.firebase_screen_class',
    'event_params.ga_session_number',
    'event_params.how_its_earned',
    'event_params.menu_name',
    'event_params.mini_game_name',
    'event_params.mini_game_ri',
    'event_params.previous_first_open_count',
    'event_params.session_engaged',
    'event_params.spent_amount',
    'event_params.spent_to',
    'event_params.system_app',
    'event_params.system_app_update',
    'event_params.time_spent_seconds',
    'event_params.update_with_analytics',
    'event_params.where_its_earned',
    'event_params.where_its_spent',
    'event_params.currency_name',

    # batch_*
    'batch_event_index', 
    'batch_ordering_id', 
    'batch_page_id',

    # privacy_info.*
    'privacy_info.ads_storage', 
    'privacy_info.analytics_storage',
    'privacy_info.uses_transient_token',

    # Misc / top-level
    'event_dimensions',    
    'event_server_delay_seconds',
    'event_value_in_usd',
    'ecommerce',
    'is_active_user',
    'platform',
    'stream_id',
    'user_ltv',

    # Time Series
    'event_datetime',
    'ts_weekday',
    'ts_is_weekend', 
    'ts_local_time', 
    'ts_hour', 
    'ts_daytime_named'
]