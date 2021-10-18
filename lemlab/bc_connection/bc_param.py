from sqlalchemy import Text, BigInteger, Boolean

# Column names (sorted alphabetically)
BALANCE_ACCOUNT = 'balance_account'
DELTA_BALANCE = 'delta_balance'
ENERGY_BALANCING_NEGATIVE = 'energy_balancing_negative'
ENERGY_BALANCING_POSITIVE = 'energy_balancing_positive'
ENERGY_CUMULATED = 'energy_cumulated'
ENERGY_IN = 'energy_in'
ENERGY_IN_CUM = 'energy_in_cum'
ENERGY_OUT = 'energy_out'
ENERGY_OUT_CUM = 'energy_out_cum'
EXTENSION_BID = '_bid'
EXTENSION_OFFER = '_offer'
HORIZON_TRADING = 'horizon_trading'
ID_AGGREGATOR = 'id_aggregator'
ID_BID = 'id_bid'
ID_MARKET_AGENT = 'id_market_agent'
ID_MATCHING = 'id_matching'
ID_METER = 'id_meter'
ID_METER_SUPER = 'id_meter_super'
ID_OFFER = 'id_offer'
ID_SOURCE = 'id_source'
ID_USER = 'id_user'
ID_USER_BID = 'id_user_bid'
ID_USER_OFFER = 'id_user_offer'
INFO_ADDITIONAL = 'info_additional'
NUMBER_POSITION = 'number_position'
NUMBER_POSITION_BID = 'number_position_bid'
NUMBER_POSITION_OFFER = 'number_position_offer'
PREFERENCE_QUALITY = 'preference_quality'
PREMIUM_PREFERENCE_QUALITY = 'premium_preference_quality'
PRICE_ENERGY = 'price_energy'
PRICE_ENERGY_BALANCING_NEGATIVE = 'price_energy_balancing_negative'
PRICE_ENERGY_BALANCING_POSITIVE = 'price_energy_balancing_positive'
PRICE_ENERGY_BID = 'price_energy_bid'
PRICE_ENERGY_BID_MAX = 'price_energy_bid_max'
PRICE_ENERGY_BID_MIN = 'price_energy_bid_min'
PRICE_ENERGY_LEVIES_NEGATIVE = "price_energy_levies_negative"
PRICE_ENERGY_LEVIES_POSITIVE = "price_energy_levies_positive"
PRICE_ENERGY_MARKET = 'price_energy_market'
PRICE_ENERGY_OFFER = 'price_energy_offer'
PRICE_ENERGY_OFFER_MAX = 'price_energy_offer_max'
PRICE_ENERGY_OFFER_MIN = 'price_energy_offer_min'
QTY_ENERGY = 'qty_energy'
QTY_ENERGY_BIDS_CUM = "qty_energy_bids_cum"
QTY_ENERGY_OFFERS_CUM = "qty_energy_offers_cum"
QTY_ENERGY_TRADED = 'qty_energy_traded'
QTY_ENERGY_TRADED_CUM = "qty_energy_traded_cum"
QUALITY_ENERGY = 'quality_energy'
QUALITY_ENERGY_BID = 'quality_energy_bid'
QUALITY_ENERGY_MARKET = 'quality_energy_market'
QUALITY_ENERGY_OFFER = 'quality_energy_offer'
STATUS_METER_READINGS_PROCESSED = 'status_meter_readings_processed'
STATUS_POSITION = 'status_position'
STATUS_SETTLEMENT_COMPLETE = 'status_settlement_complete'
STRATEGY_MARKET_AGENT = 'strategy_market_agent'
TS_DELIVERY = 'ts_delivery'
TS_DELIVERY_FIRST = 'ts_delivery_first'
TS_DELIVERY_LAST = 'ts_delivery_last'
TYPE_METER = 'type_meter'
TYPE_POSITION = 'type_position'
TYPE_TRANSACTION = 'type_transaction'
T_CLEARED = 't_cleared'
T_READING = 't_reading'
T_SUBMISSION = 't_submission'
T_UPDATE_BALANCE = 't_update_balance'

# Column base names to be dynamically added
PRICE_ENERGY_MARKET_ = PRICE_ENERGY_MARKET + '_'
SHARE_QUALITY_ = 'share_quality_offers_cleared_'

SHARE_PREFERENCE_BIDS_ = 'share_preference_bids_'
SHARE_QUALITY_OFFERS_ = 'share_quality_offers_'
SHARE_PREFERENCE_BIDS_CLEARED_ = 'share_preference_bids_cleared_'
SHARE_QUALITY_OFFERS_CLEARED_ = 'share_quality_offers_cleared_'

# Conversion factors
EURO_TO_SIGMA = 1e9  # conversion rate from euro to internal currency sigma

info_user_column_names = [ID_USER, BALANCE_ACCOUNT, T_UPDATE_BALANCE, PRICE_ENERGY_BID_MAX,
                          PRICE_ENERGY_OFFER_MIN, PREFERENCE_QUALITY, PREMIUM_PREFERENCE_QUALITY,
                          STRATEGY_MARKET_AGENT, HORIZON_TRADING, ID_MARKET_AGENT,
                          TS_DELIVERY_FIRST, TS_DELIVERY_LAST]

info_user_column_dtypes = [Text(), BigInteger(), BigInteger(), BigInteger(), BigInteger(), Text(), BigInteger(),
                           Text(), BigInteger(), BigInteger(), BigInteger()]

info_meter_column_names = [ID_METER, ID_USER, ID_METER_SUPER, TYPE_METER, ID_AGGREGATOR, QUALITY_ENERGY,
                           TS_DELIVERY_FIRST, TS_DELIVERY_LAST, INFO_ADDITIONAL]

info_meter_column_dtypes = [Text(), Text(), Text(), Text(), Text(), Text(), BigInteger(), BigInteger(), Text()]

energy_balance_column_names = [ID_METER, TS_DELIVERY, ENERGY_BALANCING_POSITIVE, ENERGY_BALANCING_NEGATIVE]

energy_balance_column_dtypes = [Text(), BigInteger(), BigInteger(), BigInteger()]

# in the blockchain, but not in the DataBase, QUALITY_ENERGY is an int
# representing the quality for faster and easier computation
positions_market_ex_ante_column_names = [ID_USER, QTY_ENERGY, PRICE_ENERGY, QUALITY_ENERGY, PREMIUM_PREFERENCE_QUALITY,
                                         TYPE_POSITION, NUMBER_POSITION, STATUS_POSITION, T_SUBMISSION, TS_DELIVERY]
positions_market_ex_ante_column_dtypes = [Text(), BigInteger(), BigInteger(), BigInteger(), BigInteger(),
                                          Text(), BigInteger(), BigInteger, BigInteger(), BigInteger()]

status_settlement_column_names = [TS_DELIVERY, STATUS_METER_READINGS_PROCESSED, STATUS_SETTLEMENT_COMPLETE, ]

status_settlement_column_dtypes = [BigInteger(), BigInteger(), BigInteger()]

meter_reading_delta_column_names = [TS_DELIVERY, ID_METER, ENERGY_IN, ENERGY_OUT]
meter_reading_delta_column_dtypes = [BigInteger(), Text(), BigInteger(), BigInteger()]

market_result_column_names = [ID_USER_OFFER, PRICE_ENERGY_OFFER, NUMBER_POSITION_OFFER, TS_DELIVERY, ID_USER_BID,
                              PRICE_ENERGY_BID, NUMBER_POSITION_BID, PRICE_ENERGY_MARKET_ + "uniform",
                              PRICE_ENERGY_MARKET_ + "discriminative", QTY_ENERGY_TRADED, SHARE_QUALITY_ + "na",
                              SHARE_QUALITY_ + "local", SHARE_QUALITY_ + "green", SHARE_QUALITY_ + "green_local",
                              T_CLEARED]

market_result_column_dtypes = [Text(), BigInteger(), BigInteger(), BigInteger(), Text(), BigInteger(), BigInteger(),
                               BigInteger(), BigInteger(), BigInteger(), BigInteger(), BigInteger(), BigInteger(),
                               BigInteger(), BigInteger()]

prices_settlement_column_names = [TS_DELIVERY, PRICE_ENERGY_BALANCING_POSITIVE, PRICE_ENERGY_BALANCING_NEGATIVE,
                                  PRICE_ENERGY_LEVIES_POSITIVE, PRICE_ENERGY_LEVIES_NEGATIVE]

prices_settlement_column_dtypes = [BigInteger(), BigInteger, BigInteger(), BigInteger(), BigInteger()]

logs_transactions_column_names = [ID_USER, TS_DELIVERY, PRICE_ENERGY_MARKET, TYPE_TRANSACTION, QTY_ENERGY,
                                  DELTA_BALANCE, T_UPDATE_BALANCE, SHARE_QUALITY_OFFERS_CLEARED_ + "na",
                                  SHARE_QUALITY_OFFERS_CLEARED_ + "local",
                                  SHARE_QUALITY_OFFERS_CLEARED_ + "green",
                                  SHARE_QUALITY_OFFERS_CLEARED_ + "green_local"]

logs_transactions_column_dtypes = [Text(), BigInteger(), BigInteger(), Text(), BigInteger(), BigInteger(), BigInteger(),
                                   BigInteger(), BigInteger(), BigInteger(), BigInteger()]


def map_name_to_dtype(names_column, dtypes_column):
    assert len(names_column) == len(dtypes_column)
    mapping = dict([(name, dtype) for name, dtype in zip(names_column, dtypes_column)])
    return mapping
