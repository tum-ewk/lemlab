__author__ = "michelzade"
__credits__ = []
__license__ = ""
__maintainer__ = "sdlumpp"
__email__ = "sebastian.lumpp@tum.de"

from sqlalchemy import Text, BigInteger
import dataclasses
from dataclasses import field

# names of static sql tables
NAME_TABLE_INFO_USER = "info_user"
NAME_TABLE_INFO_METER = "info_meter"
NAME_TABLE_POSITIONS_MARKET_EX_ANTE = "positions_market_ex_ante"
NAME_TABLE_POSITIONS_MARKET_EX_ANTE_ARCHIVE = "positions_market_ex_ante_archive"
NAME_TABLE_STATUS_SETTLEMENT = "status_settlement"
NAME_TABLE_READINGS_METER_CUMULATIVE = "readings_meter_cumulative"
NAME_TABLE_READINGS_METER_DELTA = "readings_meter_delta"
NAME_TABLE_ENERGY_BALANCING = "energy_balancing"
NAME_TABLE_PRICES_SETTLEMENT = "prices_settlement"

# names of tables that will be dynamically generated
NAME_TABLE_RESULTS_MARKET_EX_ANTE_ = "results_market_ex_ante_"
NAME_TABLE_RESULTS_MARKET_EX_POST_ = "results_market_ex_post_"
NAME_TABLE_LOGS_TRANSACTIONS = "logs_transactions"

NAME_ACCOUNT_USER = "market_participant"

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
SHARE_QUALITY_ = 'share_quality_'

SHARE_PREFERENCE_BIDS_ = 'share_preference_bids_'
SHARE_QUALITY_OFFERS_ = 'share_quality_offers_'
SHARE_PREFERENCE_BIDS_CLEARED_ = 'share_preference_bids_cleared_'
SHARE_QUALITY_OFFERS_CLEARED_ = 'share_quality_offers_cleared_'


# Conversion factors
EURO_TO_SIGMA = 1e9            # conversion rate from euro to internal currency sigma


@dataclasses.dataclass
class LemlabColumn:
    """class for defining lemlab column forms"""
    name: str = ""
    dtype: BigInteger = 0
    pk: bool = False


@dataclasses.dataclass
class LemlabTable:
    """class for defining and possibly modifying table forms"""
    name: str = ""
    list_columns: list = field(default_factory=list)
    user_accounts: str = ""
    list_rights: list = None

    def replace(self):
        output = LemlabTable()
        output.name = self.name
        output.list_columns = self.list_columns[:]
        output.user_accounts = self.user_accounts
        output.list_rights = self.list_rights
        return output


# Table set up
table_info_user = LemlabTable()
table_info_user.name = NAME_TABLE_INFO_USER
table_info_user.list_columns = [LemlabColumn(ID_USER, Text(), True),
                                LemlabColumn(BALANCE_ACCOUNT, BigInteger()),
                                LemlabColumn(T_UPDATE_BALANCE, BigInteger()),
                                LemlabColumn(PRICE_ENERGY_BID_MAX, BigInteger()),
                                LemlabColumn(PRICE_ENERGY_OFFER_MIN, BigInteger()),
                                LemlabColumn(PREFERENCE_QUALITY, Text()),
                                LemlabColumn(PREMIUM_PREFERENCE_QUALITY, BigInteger()),
                                LemlabColumn(STRATEGY_MARKET_AGENT, Text()),
                                LemlabColumn(HORIZON_TRADING, BigInteger()),
                                LemlabColumn(ID_MARKET_AGENT, Text()),
                                LemlabColumn(TS_DELIVERY_FIRST, BigInteger()),
                                LemlabColumn(TS_DELIVERY_LAST, BigInteger())]
table_info_user.user_accounts = NAME_ACCOUNT_USER
table_info_user.list_rights = ["SELECT"]

table_info_meter = LemlabTable()
table_info_meter.name = NAME_TABLE_INFO_METER
table_info_meter.list_columns = [LemlabColumn(ID_METER, Text(), True),
                                 LemlabColumn(ID_USER, Text()),
                                 LemlabColumn(ID_METER_SUPER, Text()),
                                 LemlabColumn(TYPE_METER, Text()),
                                 LemlabColumn(ID_AGGREGATOR, Text()),
                                 LemlabColumn(QUALITY_ENERGY, Text()),
                                 LemlabColumn(TS_DELIVERY_FIRST, BigInteger()),
                                 LemlabColumn(TS_DELIVERY_LAST, BigInteger()),
                                 LemlabColumn(INFO_ADDITIONAL, Text())]
table_info_meter.user_accounts = NAME_ACCOUNT_USER
table_info_meter.list_rights = ["SELECT"]

table_positions_market = LemlabTable()
table_positions_market.name = NAME_TABLE_POSITIONS_MARKET_EX_ANTE
table_positions_market.list_columns = [LemlabColumn(ID_USER, Text(), True),
                                       LemlabColumn(QTY_ENERGY, BigInteger()),
                                       LemlabColumn(PRICE_ENERGY, BigInteger()),
                                       LemlabColumn(QUALITY_ENERGY, Text()),
                                       LemlabColumn(PREMIUM_PREFERENCE_QUALITY, BigInteger()),
                                       LemlabColumn(TYPE_POSITION, Text(), True),
                                       LemlabColumn(NUMBER_POSITION, BigInteger(), True),
                                       LemlabColumn(STATUS_POSITION, BigInteger()),
                                       LemlabColumn(T_SUBMISSION, BigInteger()),
                                       LemlabColumn(TS_DELIVERY, BigInteger(), True)]
table_positions_market.user_accounts = NAME_ACCOUNT_USER
table_positions_market.list_rights = ["SELECT", "INSERT", "UPDATE", "DELETE"]

table_positions_archive = LemlabTable()
table_positions_archive.name = NAME_TABLE_POSITIONS_MARKET_EX_ANTE_ARCHIVE
table_positions_archive.list_columns = [LemlabColumn(ID_USER, Text()),
                                        LemlabColumn(QTY_ENERGY, BigInteger()),
                                        LemlabColumn(PRICE_ENERGY, BigInteger()),
                                        LemlabColumn(QUALITY_ENERGY, Text()),
                                        LemlabColumn(PREMIUM_PREFERENCE_QUALITY, BigInteger()),
                                        LemlabColumn(TYPE_POSITION, Text()),
                                        LemlabColumn(NUMBER_POSITION, BigInteger()),
                                        LemlabColumn(STATUS_POSITION, BigInteger()),
                                        LemlabColumn(T_SUBMISSION, BigInteger()),
                                        LemlabColumn(TS_DELIVERY, BigInteger())]
table_positions_archive.user_accounts = NAME_ACCOUNT_USER
table_positions_archive.list_rights = ["SELECT", "INSERT", "UPDATE", "DELETE"]


table_status_settlement = LemlabTable()
table_status_settlement.name = NAME_TABLE_STATUS_SETTLEMENT
table_status_settlement.list_columns = [LemlabColumn(TS_DELIVERY, BigInteger(), True),
                                        LemlabColumn(STATUS_METER_READINGS_PROCESSED, BigInteger()),
                                        LemlabColumn(STATUS_SETTLEMENT_COMPLETE, BigInteger())]
table_status_settlement.user_accounts = NAME_ACCOUNT_USER
table_status_settlement.list_rights = []

table_readings_meter_cumulative = LemlabTable()
table_readings_meter_cumulative.name = NAME_TABLE_READINGS_METER_CUMULATIVE
table_readings_meter_cumulative.list_columns = [LemlabColumn(T_READING, BigInteger(), True),
                                                LemlabColumn(ID_METER, Text(), True),
                                                LemlabColumn(ENERGY_IN_CUM, BigInteger()),
                                                LemlabColumn(ENERGY_OUT_CUM, BigInteger())]
table_readings_meter_cumulative.user_accounts = NAME_ACCOUNT_USER
table_readings_meter_cumulative.list_rights = ["SELECT", "INSERT", "UPDATE"]

table_readings_meter_delta = LemlabTable()
table_readings_meter_delta.name = NAME_TABLE_READINGS_METER_DELTA
table_readings_meter_delta.list_columns = [LemlabColumn(TS_DELIVERY, BigInteger(), True),
                                           LemlabColumn(ID_METER, Text(), True),
                                           LemlabColumn(ENERGY_IN, BigInteger()),
                                           LemlabColumn(ENERGY_OUT, BigInteger())]
table_readings_meter_delta.user_accounts = NAME_ACCOUNT_USER
table_readings_meter_delta.list_rights = ["SELECT"]

table_energy_balancing = LemlabTable()
table_energy_balancing.name = NAME_TABLE_ENERGY_BALANCING
table_energy_balancing.list_columns = [LemlabColumn(ID_METER, Text(), True),
                                       LemlabColumn(TS_DELIVERY, BigInteger(), True),
                                       LemlabColumn(ENERGY_BALANCING_POSITIVE, BigInteger()),
                                       LemlabColumn(ENERGY_BALANCING_NEGATIVE, BigInteger())]
table_energy_balancing.user_accounts = NAME_ACCOUNT_USER
table_energy_balancing.list_rights = []

table_prices_settlement = LemlabTable()
table_prices_settlement.name = NAME_TABLE_PRICES_SETTLEMENT
table_prices_settlement.list_columns = [LemlabColumn(TS_DELIVERY, BigInteger(), True),
                                        LemlabColumn(PRICE_ENERGY_BALANCING_POSITIVE, BigInteger()),
                                        LemlabColumn(PRICE_ENERGY_BALANCING_NEGATIVE, BigInteger()),
                                        LemlabColumn(PRICE_ENERGY_LEVIES_POSITIVE, BigInteger()),
                                        LemlabColumn(PRICE_ENERGY_LEVIES_NEGATIVE, BigInteger())]
table_prices_settlement.user_accounts = NAME_ACCOUNT_USER
table_prices_settlement.list_rights = ["SELECT"]

# further columns are generated dynamically in db_connection
# additional columns: clearing prices and shares of energy qualities

table_results_market_ex_ante_base = LemlabTable()
table_results_market_ex_ante_base.name = NAME_TABLE_RESULTS_MARKET_EX_ANTE_
table_results_market_ex_ante_base.list_columns = [LemlabColumn(ID_USER_OFFER, Text(), True),
                                                  LemlabColumn(NUMBER_POSITION_OFFER, BigInteger(), True),
                                                  LemlabColumn(PRICE_ENERGY_OFFER, BigInteger()),
                                                  LemlabColumn(ID_USER_BID, Text(), True),
                                                  LemlabColumn(NUMBER_POSITION_BID, BigInteger(), True),
                                                  LemlabColumn(PRICE_ENERGY_BID, BigInteger()),
                                                  LemlabColumn(QTY_ENERGY_TRADED, BigInteger()),
                                                  LemlabColumn(T_CLEARED, BigInteger(), True),
                                                  LemlabColumn(TS_DELIVERY, BigInteger(), True)]
table_results_market_ex_ante_base.user_accounts = NAME_ACCOUNT_USER
table_results_market_ex_ante_base.list_rights = ["SELECT"]

table_results_market_ex_post_base = LemlabTable()
table_results_market_ex_post_base.name = NAME_TABLE_RESULTS_MARKET_EX_POST_
table_results_market_ex_post_base.list_columns = [LemlabColumn(TS_DELIVERY, BigInteger(), True)]
table_results_market_ex_post_base.user_accounts = NAME_ACCOUNT_USER
table_results_market_ex_post_base.list_rights = ["SELECT"]

table_logs_transactions_base = LemlabTable()
table_logs_transactions_base.name = NAME_TABLE_LOGS_TRANSACTIONS
table_logs_transactions_base.list_columns = [LemlabColumn(ID_USER, Text()),
                                             LemlabColumn(TS_DELIVERY, BigInteger()),
                                             LemlabColumn(PRICE_ENERGY_MARKET, BigInteger()),
                                             LemlabColumn(TYPE_TRANSACTION, Text()),
                                             LemlabColumn(QTY_ENERGY, BigInteger()),
                                             LemlabColumn(DELTA_BALANCE, BigInteger()),
                                             LemlabColumn(T_UPDATE_BALANCE, BigInteger())]
table_logs_transactions_base.user_accounts = NAME_ACCOUNT_USER
table_logs_transactions_base.list_rights = ["SELECT"]

# list of tables to be extended by DatabaseConnection instance containing
# LemlabTable objects describing the tables contained in the database

LIST_TABLES = [table_info_user,
               table_info_meter,
               table_positions_market,
               table_positions_archive,
               table_readings_meter_cumulative,
               table_readings_meter_delta,
               table_status_settlement,
               table_energy_balancing,
               table_prices_settlement]
