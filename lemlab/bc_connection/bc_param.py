from sqlalchemy import Text, BigInteger


info_user_column_names = ["id_user", "balance_account", "t_update_balance", "price_energy_bid_max",
                          "price_energy_offer_min", "preference_quality", "premium_preference_quality",
                          "type_market_agent", "horizon_trading", "ts_delivery_first", "ts_delivery_last"]
info_meter_column_names = ["id_meter", "id_user", "type_meter", "id_meter_main", "id_aggregator", "quality_energy",
                           "ts_delivery_first", "ts_delivery_last", "info_additional"]
positions_market_ex_ante_column_names = ["id_user", "qty_energy", "price_energy", "quality_energy",
                                         "premium_preference_quality", "type_position", "number_position",
                                         "status_position", "t_submission", "ts_delivery"]
info_user_column_dtypes = [Text(), BigInteger(), BigInteger(), BigInteger(), BigInteger(), Text(), BigInteger(),
                           Text(), BigInteger(), BigInteger(), BigInteger()]
