'''
This module contains lists and maps used for organizing and displaying data in the application.
designed to be used in conjunction with the main application logic.
'''


# This is a list and order of the column names that I used for conveniancy in EDA and LookerStudio. 

df_column_names_map = {

    # 🎯 Event Core Info
    'event_name': 'event_name',
    'event_bundle_sequence_id': 'event_batch_id',
    'user_pseudo_id': 'user_pseudo_id',
    'stream_id': 'stream_identifier',
    'platform': 'platform',
    'is_active_user': 'is_active_user',
    'batch_event_index': 'event_index_in_batch',

    # ⏱️ Event Timing
    'event_datetime': 'event_datetime',
    'event_date': 'event_date',
    'event_time': 'event_time',
    'event_previous_date': 'previous_event_date',
    'event_previous_time': 'previous_event_time',
    'event_first_touch_date': 'first_touch_date',
    'event_first_touch_time': 'first_touch_time',
    'time_delta': 'time_since_previous_event',
    'ts_weekday': 'weekday',
    'ts_local_time': 'local_time',
    'ts_hour': 'hour_of_day',
    'ts_daytime_named': 'time_of_day',
    'ts_is_weekend': 'is_weekend',
    'device__time_zone_offset_hours': 'time_zone_offset_hours',
    'event_server_delay_seconds': 'server_delay_seconds',

    # 🧭 Session Tracking
    'event_params__ga_session_id': 'session_id',
    'event_params__ga_session_number': 'session_number',
    'event_params__firebase_event_origin': 'firebase_event_origin',
    'event_params__engaged_session_event': 'engaged_session_event',
    'event_params__session_engaged': 'session_was_engaged',
    'user__ga_session_number': 'user_session_count',
    'user__ga_session_id': 'user_session_id',
    'inferred_session_id': 'inferred_session_id',
    'session_duration_seconds': 'session_duration_seconds',
    'session_start_time': 'session_start_time',
    'session_end_time': 'session_end_time',

    # 📺 Screen & Navigation
    'event_params__firebase_screen_id': 'screen_id',
    'event_params__firebase_screen_class': 'screen_class',
    'event_params__menu_name': 'menu_name',
    'event_params__entrances': 'entrances_count',

    # 📱 Device Info
    'device__category': 'device_type',
    'device__mobile_brand_name': 'mobile_brand',
    'device__mobile_model_name': 'mobile_model',
    'device__mobile_os_hardware_model': 'device_hardware_model',
    'device__operating_system': 'operating_system',
    'device__operating_system_version': 'os_version',
    'device__advertising_id': 'advertising_id',
    'device__language': 'device_language',
    'device__is_limited_ad_tracking': 'ad_tracking_limited',

    # 🌍 Geolocation
    'geo__city': 'city',
    'geo__country': 'country',
    'geo__continent': 'continent',
    'geo__region': 'region',
    'geo__sub_continent': 'subcontinent',
    'geo__metro': 'metro_area',

    # 📲 App Metadata
    'app_info__id': 'app_id',
    'app_info__version': 'app_version',
    'app_info__install_store': 'install_store',
    'app_info__firebase_app_id': 'firebase_app_id',
    'app_info__install_source': 'app_install_source',

    # 🔒 Privacy Settings
    'privacy_info__analytics_storage': 'consent_analytics_storage',
    'privacy_info__ads_storage': 'consent_ads_storage',

    # 📈 Ads & Monetization
    'event_params__ad_platform': 'ad_platform',
    'event_params__ad_shown_where': 'ad_shown_in',
    'event_params__ad_unit_id': 'ad_unit_id',
    'event_params__ad_network': 'ad_network',
    'event_params__ad_format': 'ad_format',
    'event_params__firebase_conversion': 'conversion_event',

    # 🛠️ Debug / Quality
    'event_params__firebase_error': 'firebase_error',
    'event_params__fatal': 'fatal_error',
    'event_params__timestamp': 'raw_timestamp',

    # ⏳ Engagement & Duration
    'event_params__engagement_time_seconds': 'engagement_time_seconds',
    'event_params__time_spent_seconds': 'time_spent_on_activity_seconds',

    # 🧠 Gameplay (Maze & Buffs)
    'maze_gender': 'maze_gender',
    'maze_hand': 'maze_hand',
    'maze_level': 'maze_level',
    'buff_type': 'buff_type',
    'buff_gift': 'buff_gift',
    'buff_gold': 'buff_gold',
    'earned_buff_type': 'earned_buff_type',

    # 🎮 Game Event Data
    'event_params__current_qi': 'original_qi',
    'question_address': 'question_address',
    'event_params__character_name': 'character_name',
    'event_params__current_tier': 'current_tier',
    'event_params__current_question_index': 'current_question_index',
    'event_params__current_qi': 'original_qi',
    'cumulative_question_index': 'cumulative_question_index',
    'question_address': 'question_address',
    'event_params__mini_game_ri': 'mini_game_round_index',
    'event_params__mini_game_name': 'mini_game_name',
    'event_params__answered_wrong': 'answered_incorrectly',
    'question_correct_incorrect': 'question_correct_incorrect',

    # 💰 Currency Events
    'event_params__where_its_earned': 'where_currency_was_earned',
    'event_params__currency_name': 'currency_name',
    'event_params__earned_amount': 'currency_earned',
    'event_params__how_its_earned': 'how_currency_was_earned',
    'event_params__spent_amount': 'currency_spent',
    'event_params__where_its_spent': 'where_currency_was_spent',
    'event_params__spent_to': 'spent_on',

    # 🛒 Shop
    'doll_name': 'doll_name',
    'spent_in_crystal': 'spent_in_crystal',
    'shop_permanent_item': 'permanent_item',
    'shop_consumable_item': 'consumable_item',
    'board_item': 'board_item',

    # 🔄 User Lifecycle
    'user__first_open_time': 'user_first_open_time',
    'user__first_open_date': 'user_first_open_date',
    'event_params__previous_first_open_count': 'previous_first_open_count',
}




columns_to_drop = ['event_timestamp',
                    'event_previous_timestamp', 
                    'user_first_touch_timestamp', 
                    'event_server_timestamp_offset', 
                    'device__time_zone_offset_seconds', 
                    'event_params__engagement_time_msec',
                    'event_previous_datetime',
                    'event_params__time_spent',
                    'event_first_touch_datetime',
                    'user__first_open_datetime',
                    'event_value_in_usd', 
                    'user_id', 
                    'batch_page_id',	
                    'batch_ordering_id', 
                    'privacy_info__uses_transient_token',
                    'user_ltv',
                    'device__mobile_marketing_name', 
                    'device__vendor_id',
                    'device__browser', 
                    'device__browser_version', 
                    'device__web_info', 
                    'event_dimensions',
                    'traffic_source__name', 
                    'traffic_source__medium',	
                    'traffic_source__source',	
                    'ecommerce', 
                    'event_params__update_with_analytics', 
                    'event_params__system_app_update',
                    'collected_traffic_source',
                    'event_params__system_app'
]



# These dictionaries map specific VALUES to their display names.
# They are used to convert raw values in the data to more user-friendly names.


event_name_map = {'ad_clicked': 'Ad Clicked',
                  'app_remove': 'App Removed',
                  'first_open': 'First Open',
                  'menu_closed': 'Menu Closed',
                  'menu_opened': 'Menu Opened',
                  'screen_view': 'Screen Viewed',
                  'ad_impression': 'Ad Impression',
                  'session_start': 'Session Started',
                  'app_clear_data': 'App Data Cleared',
                  'user_engagement': 'User Engagement',
                  'question_started': 'Question Started',
                  'mini_game_started': 'Mini-game Started',
                  'question_completed': 'Question Completed',
                  'mini_game_completed': 'Mini-game Completed',
                  'earn_virtual_currency': 'Earned Virtual Currency',
                  'spend_virtual_currency': 'Spent Virtual Currency',
                  'mini_game_failed': 'Mini-game Failed',
                  'app_exception': 'App Exception'
                    }



event_params__mini_game_ri_map = {'stone_game': 'Stone Game',                                 
                                  'cauldron_game': 'Cauldron Game',
                                  'coffee_game': 'Coffee Game',
                                  'card_game': 'Card Game',
                                  'daily_spin': 'Daily Spin',
                                  'completed': 'Completed',
                                  'failed': 'Failed',
                                  'gold_500': 'Gold 500',
                                      }

event_params__menu_name_map = {'ScrollMenu' : 'Scroll Menu',
                               'CrystalMenu' : 'Crystal Menu',
                               'CrystalAligninMenu' : 'Crystal Alignin Menu',
                               'WannaPlayMenu' : 'Wanna Play Menu',
                               'ShopMenu' : 'Shop Menu',
                               'BoardMenu' : 'Board Menu',
                               'CrystalCharacterMenu' : 'Crystal Character Menu',
                               'EnergyGoldExchange' : 'Energy Gold Exchange',
                               'CrystalCauldronMenu' : 'Crystal Cauldron Menu',
                               'CrystalEnergyMenu' : 'Crystal Energy Menu',
                               'CrystalCoffeeMenu' : 'Crystal Coffee Menu',
                               'WheelOfFortune' : 'Wheel of Fortune',
                               'scroll_menu' : 'Scroll Menu',
                                    }
 
event_params__character_name_map = { 'aturtle': 'A Turtle',
                                     'littlea': 'Little A',
                                     'sinnct': 'Sinnct',
                                     'obviousjoe': 'Obvious Joe',
                                     'erjohn': 'ER John',
                                     'billy': 'Billy',
                                     'maydenis': 'Maydenis',
                                     'almiralotus': 'Almira Lotus',
                                     't': 'T',
                                     'mrspearl': 'Mrs. Pearl',
                                     'therock': 'The Rock',
                                     'army': 'Army',
                                     'ladydodo': 'Lady Dodo',
                                     'dlion': 'D Lion',
                                     'frenchie': 'Frenchie',
                                     'mi': 'Mi',
                                     'biga': 'Biga',
                                     'cjay': 'C Jay',
                                     'mo': 'Mo',
                                     'mryogurt': "Mr. Yogurt",
                                     'crystalraw': "Crystal Raw",
                                     'aisha': "Aisha",
                                     'mustafa': "Mustafa",
                                     'fathergold': "Father Gold",
                                     'tracy': "Tracy",
                                     'whymargaret': "Why Margaret",
                                     'suedoluni': "Sue Doluni"
                                        }

event_params__mini_game_name_map = { 'stone_mini_game': 'Stone Game',
                                     'cauldron_mini_game': 'Cauldron Game',
                                     'star_mini_game': 'Star Game',
                                     'maze_mini_game': 'Maze Game',
                                     'coffee_mini_game': 'Coffee Game',
                                     'card_mini_game': 'Card Game',
                                     'wheel_of_fortune': 'Wheel of Fortune',
                                     'voodoo_mini_game': 'Voodoo Game',
                                     'catch_up_cauldron': 'Catch Up Cauldron'
                                        }





event_params__where_its_earned_map = { 'mini_game': 'Mini-game',
                                       'question': 'Question',
                                       'wanna_play': 'Wanna Play',
                                       'energy_gold_exchange': 'Energy Gold Exchange',
}



event_params__currency_name_map = { 'gold': 'Gold',
                                    'energy': 'Energy'
}


event_params__how_its_earned_map = { 'combo': 'Combo',
                                     'normal': 'Normal',
                                     'mini_game_completed': 'Mini-game Completed',
                                     'star_mini_game': 'Star Game',
                                     'CauldronMiniGame': 'Cauldron Game',
                                     'CoffeeMiniGame': 'Coffee Game',
                                     'card_mini_game': 'Card Game',
                                     'maze_mini_game': 'Maze Game',
                                     'mini_game_failed': 'Mini-game Failed',
}
                                    
event_params__where_its_spent_map = { 'shop': 'Shop',
                                      'board': 'Board',
                                      'crystal': 'Crystal',
}

event_params__ad_shown_where_map = { 'crystal_character_ad': 'Crystal Character',
                                     'crystal_energy_ad': 'Crystal Energy',
                                     'wheel_of_fortune_ad': 'Wheel of Fortune',
                                     'ad_shown_where': 'ADSHOWNWHERE',
                                     'wanna_play_ad': 'Wanna Play',
                                     'EnergyGoldExchangeAd': 'Energy Gold Exchange'
}

event_params__menu_name_map = { 'Scroll Menu': 'Scroll Menu',
                                'crystal_menu': 'Crystal Menu',
                                'crystal_aliginn_menu': 'Crystal AliCin Menu',
                                'wanna_play_menu': 'Wanna Play Menu',
                                'shop_menu': 'Shop Menu',
                                'board_menu': 'Board Menu',
                                'crystal_character_menu': 'Crystal Character Menu',
                                'energy_gold_exchange': 'Energy Gold Exchange',
                                'crystal_cauldron_menu': 'Crystal Cauldron Menu',
                                'crystal_energy_menu': 'Crystal Energy Menu',
                                'crystal_coffee_menu': 'Crystal Coffee Menu',
                                'wheel_of_fortune': 'Wheel of Fortune',
                                'scroll_menu': 'Scroll Menu',
}




spent_in_crystal_map = {'cauldron' : 'Cauldron',
                        'aliginn' : 'AliCin',
                        'coffee' : 'Coffee'
}


shop_consumable_item_map = {'potion': 'Potion',
                            'incense': 'Incense',
                            'amulet': 'Amulet',
                            'ıncense': 'Incense',
}
                            
shop_permanent_item_map = {'dreamcatcher': 'Dreamcatcher',
                           'catcollar': 'Cat Collar',
                           'library1': 'Library 1',
                           'library2': 'Library 2',
                           'bugspray': 'Bug Spray',
                           'schedule': 'Schedule',
                           'crystal': 'Crystal',
                           'horseshoe': 'Horseshoe'
}

ts_weekday_map = {'Monday': 'Pazartesi',
                  'Tuesday': 'Salı',
                  'Wednesday': 'Çarşamba',
                  'Thursday': 'Perşembe',
                  'Friday': 'Cuma',
                  'Saturday': 'Cumartesi',
                  'Sunday': 'Pazar'
}


map_of_maps = {
    'event_name': event_name_map,
    'event_params__mini_game_ri': event_params__mini_game_ri_map,
    'event_params__menu_name': event_params__menu_name_map,
    'event_params__character_name': event_params__character_name_map,
    'event_params__mini_game_name': event_params__mini_game_name_map,
    'event_params__where_its_earned': event_params__where_its_earned_map,
    'event_params__currency_name': event_params__currency_name_map,
    'event_params__how_its_earned': event_params__how_its_earned_map,
    'event_params__where_its_spent': event_params__where_its_spent_map,
    'event_params__ad_shown_where': event_params__ad_shown_where_map,
    'doll_name' : event_params__character_name_map,
    'spent_in_crystal': spent_in_crystal_map,
    'shop_consumable_item': shop_consumable_item_map,
    'shop_permanent_item': shop_permanent_item_map,
    'ts_weekday': ts_weekday_map,
    'event_params__menu_name': event_params__menu_name_map
}

