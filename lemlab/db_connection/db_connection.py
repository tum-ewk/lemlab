__author__ = "sdlumpp"
__credits__ = ["michelzade"]
__license__ = ""
__maintainer__ = "sdlumpp"
__email__ = "sebastian.lumpp@tum.de"

import pandas as pd
import sqlalchemy as db
import lemlab.db_connection.db_param as db_p
import time


class DatabaseConnection:
    """Database connection provides all database connection methods required by lemlab.
       In order to remain database-agnostic no other module may connect to the database directly."""

    def __init__(self, db_dict, lem_config):

        # String to create engine connection. Necessary to write directly from DataFrames to database tables
        string_eng = f"postgresql://" \
                     f"{db_dict.get('user')}" \
                     f":{db_dict.get('pw')}" \
                     f"@{db_dict.get('host')}" \
                     f":{db_dict.get('port')}" \
                     f"/{db_dict.get('db')}"

        self.engine = db.create_engine(string_eng, pool_size=10)

        self.lem_config = lem_config
        self.db_param = db_p

        self.list_tables = self.db_param.LIST_TABLES[:]

        self._dynamize_db_param()

    def init_db(self, clear_tables=False, reformat_tables=False):
        for table in self.list_tables:
            self._init_table(table=table,
                             clear_table=clear_tables,
                             reformat_table=reformat_tables)

    def end_connection(self):
        self.engine.dispose()

    ###################################################
    # Functions for the info_user table
    # Market participants only

    def get_info_user(self, id_user=None):
        sql = f"SELECT * FROM \"{self.db_param.NAME_TABLE_INFO_USER}\""
        if id_user is not None:
            sql += f" WHERE {self.db_param.ID_USER} = '{id_user}'"
        return self._query_data_free(sql)

    def get_list_all_users(self, ts_delivery_active=None):
        # select all meters
        # if t_d is used, only meters active at the selected t_d are returned
        sql = f"SELECT {self.db_param.ID_USER} FROM {self.db_param.NAME_TABLE_INFO_USER}"
        if ts_delivery_active is not None:
            sql += f" AND {self.db_param.TS_DELIVERY_FIRST} <= {ts_delivery_active}" \
                   f" AND {self.db_param.TS_DELIVERY_FIRST} >= {ts_delivery_active};"
        return list(self._query_data_free(sql).loc[:, self.db_param.ID_USER])

    # Admins only

    def register_user(self, df_in):
        self.insert(table_name=self.db_param.NAME_TABLE_INFO_USER,
                    df_insert=df_in)

    def edit_user(self, df_user):
        list_columns_all = self.get_table_columns(table_name=self.db_param.NAME_TABLE_INFO_USER)
        list_columns_pk = self.get_table_columns(table_name=self.db_param.NAME_TABLE_INFO_USER, pk_only=True)
        list_columns_not_pk = list(set(list_columns_all) - set(list_columns_pk))
        sql = f"UPDATE {self.db_param.NAME_TABLE_INFO_USER} SET "
        for column in list_columns_not_pk[:-1]:
            if type(df_user.loc[0, column]) is str:
                sql += f"{column} = '{df_user.loc[0, column]}', "
            else:
                sql += f"{column} = {df_user.loc[0, column]}, "
        if type(df_user.loc[0, list_columns_not_pk[-1]]) is str:
            sql += f"{list_columns_not_pk[-1]} = '{df_user.loc[0, list_columns_not_pk[-1]]}'"
        else:
            sql += f"{list_columns_not_pk[-1]} = {df_user.loc[0, list_columns_not_pk[-1]]}"
        sql += f" WHERE {self.db_param.ID_USER} = '{df_user.loc[0, self.db_param.ID_USER]}';"
        self.engine.execute(sql)

    def delete_user(self, id_user):
        sql = f"DELETE FROM \"{self.db_param.NAME_TABLE_INFO_USER}\" " \
              f" WHERE {self.db_param.ID_USER} = '{id_user}';"
        self.engine.execute(sql)

    ###################################################
    # Functions for the meter registration table
    # Market participants only

    def get_info_meter(self, id_user="%%", id_meter="%%",  ts_delivery_active=None):
        sql = f"SELECT * FROM \"{self.db_param.NAME_TABLE_INFO_METER}\" " \
              f"WHERE {self.db_param.ID_USER} LIKE '{id_user}' " \
              f"AND {self.db_param.ID_METER} LIKE '{id_meter}'"
        if ts_delivery_active is not None:
            sql += f"AND {self.db_param.TS_DELIVERY_FIRST} <= {ts_delivery_active}" \
                   f" AND {self.db_param.TS_DELIVERY_LAST} >= {ts_delivery_active}"
        return self._query_data_free(sql)

    def get_mapping_to_user(self):
        info_meter = self._query_data_free(f"SELECT {self.db_param.ID_METER}, {self.db_param.ID_USER}"
                                           f" FROM {self.db_param.NAME_TABLE_INFO_METER}")

        dict_mapping_1 = dict([(i, a) for i, a in zip(info_meter["id_meter"], info_meter["id_user"])])

        info_user = self._query_data_free(f"SELECT {self.db_param.ID_USER}, {self.db_param.ID_MARKET_AGENT}"
                                          f" FROM {self.db_param.NAME_TABLE_INFO_USER}")

        dict_mapping_2 = dict([(i, a) for i, a in zip(info_user["id_market_agent"], info_user["id_user"])])

        dict_mapping = {**dict_mapping_1, **dict_mapping_2}

        list_users = list(set(info_meter["id_user"]))

        for user in list_users:
            dict_mapping[user] = user
        return dict_mapping

    def get_map_to_main_meter(self):
        info_meter = self._query_data_free(f"SELECT {self.db_param.ID_METER}, {self.db_param.ID_USER}"
                                           f" FROM {self.db_param.NAME_TABLE_INFO_METER}"
                                           f" WHERE {self.db_param.TYPE_METER} LIKE '%%grid%%'")

        map_grid_meter_to_self = dict([(i, a) for i, a in zip(info_meter["id_meter"], info_meter["id_meter"])])

        map_user_to_meter = dict([(i, a) for i, a in zip(info_meter["id_user"], info_meter["id_meter"])])

        info_user = self._query_data_free(f"SELECT {self.db_param.ID_USER}, {self.db_param.ID_MARKET_AGENT}"
                                          f" FROM {self.db_param.NAME_TABLE_INFO_USER}")

        map_ma_to_user = dict([(i, a) for i, a in zip(info_user["id_market_agent"], info_user["id_user"])])

        map_everything_to_main_meter = {}
        for id_ma in map_ma_to_user:
            map_everything_to_main_meter[id_ma] = map_user_to_meter.get(map_ma_to_user[id_ma], "0000000000")
        map_everything_to_main_meter = {**map_everything_to_main_meter, **map_grid_meter_to_self, **map_user_to_meter}
        return map_everything_to_main_meter

    def get_list_main_meters(self, ts_delivery_active=None):
        # select main meters
        # if t_d is used, only meters active at the selected t_d are returned
        sql = f"SELECT {self.db_param.ID_METER} FROM {self.db_param.NAME_TABLE_INFO_METER}" \
              f" WHERE ({self.db_param.TYPE_METER} = 1 OR {self.db_param.TYPE_METER} = 2)"

        if ts_delivery_active is not None:
            sql += f" AND {self.db_param.TS_DELIVERY_FIRST} <= {ts_delivery_active}" \
                   f" AND {self.db_param.TS_DELIVERY_LAST} >= {ts_delivery_active};"
        return list(self._query_data_free(sql).loc[:, self.db_param.ID_METER])

    def get_list_all_meters(self, ts_delivery_active=None, non_virtual=True):
        # select all meters
        # if t_d is used, only meters active at the selected t_d are returned
        sql = f"SELECT {self.db_param.ID_METER} FROM {self.db_param.NAME_TABLE_INFO_METER}"

        if ts_delivery_active is not None:
            sql += f" WHERE ({self.db_param.TS_DELIVERY_FIRST} <= {ts_delivery_active}" \
                   f" AND {self.db_param.TS_DELIVERY_LAST} >= {ts_delivery_active})"
        if non_virtual:
            sql += f" AND {self.db_param.TYPE_METER} NOT LIKE 'virtual%%'"
        return list(self._query_data_free(sql).loc[:, self.db_param.ID_METER])

    def get_map_meter_to_quality(self):
        info_meter = self._query_data_free(f"SELECT {self.db_param.ID_METER}, {self.db_param.QUALITY_ENERGY}"
                                           f" FROM {self.db_param.NAME_TABLE_INFO_METER}")

        map_quality_to_meter = dict([(i, a) for i, a in zip(info_meter[self.db_param.ID_METER],
                                                            info_meter[self.db_param.QUALITY_ENERGY])])

        return map_quality_to_meter

    # Admins only

    def register_meter(self, df_in):
        self.insert(table_name=self.db_param.NAME_TABLE_INFO_METER,
                    df_insert=df_in)

    def edit_meter(self, df_meter):
        list_columns_all = self.get_table_columns(table_name=self.db_param.NAME_TABLE_INFO_METER)
        list_columns_pk = self.get_table_columns(table_name=self.db_param.NAME_TABLE_INFO_METER, pk_only=True)
        list_columns_not_pk = list(set(list_columns_all) - set(list_columns_pk))
        sql = f"UPDATE {self.db_param.NAME_TABLE_INFO_METER} SET "
        for column in list_columns_not_pk[:-1]:
            if type(df_meter.loc[0, column]) is str:
                sql += f"{column} = '{df_meter.loc[0, column]}', "
            else:
                sql += f"{column} = {df_meter.loc[0, column]}, "
        if type(df_meter.loc[0, list_columns_not_pk[-1]]) is str:
            sql += f"{list_columns_not_pk[-1]} = '{df_meter.loc[0, list_columns_not_pk[-1]]}' "
        else:
            sql += f"{list_columns_not_pk[-1]} = {df_meter.loc[0, list_columns_not_pk[-1]]} "
        sql += f" WHERE {self.db_param.ID_METER} = '{df_meter.loc[0, self.db_param.ID_METER]}';"
        self.engine.execute(sql)

    def delete_meter(self, id_meter):
        sql = f"DELETE FROM {self.db_param.NAME_TABLE_INFO_METER} " \
              f" WHERE {self.db_param.ID_METER} = '{id_meter}';"
        self.engine.execute(sql)

    ###################################################
    # Functions for the market bid submission table
    # Market participants only
    def post_positions(self, df_bids, t_override=None):
        t_now = round(time.time()) if t_override is None else t_override
        df_bids.loc[:, self.db_param.T_SUBMISSION] = t_now
        self.upsert(table_name=self.db_param.NAME_TABLE_POSITIONS_MARKET_EX_ANTE,
                    df_insert=df_bids)

    def clear_positions(self, id_user):
        self.engine.execute(f"DELETE FROM {self.db_param.NAME_TABLE_POSITIONS_MARKET_EX_ANTE} "
                            f"WHERE {self.db_param.ID_USER} "
                            f"LIKE {id_user}")

    def get_open_positions(self, id_user="%%", ts_delivery_first=None,
                           ts_delivery_last=None, clear_table=False, archive=False):
        ts_delivery_first = ts_delivery_first if ts_delivery_first is not None else 0
        ts_delivery_last = ts_delivery_last if ts_delivery_last is not None else 2147483647

        # query the open bids for the market trading horizon
        open_bids = self._query_data_free(
            f"SELECT * FROM {self.db_param.NAME_TABLE_POSITIONS_MARKET_EX_ANTE} "
            f"WHERE {self.db_param.ID_USER} LIKE '{id_user}' "
            f"AND {self.db_param.TYPE_POSITION} LIKE 'bid' "
            f"AND {self.db_param.TS_DELIVERY} "
            f"BETWEEN {ts_delivery_first} "
            f"AND {ts_delivery_last} "
            f"ORDER BY {self.db_param.TS_DELIVERY}"
        )

        # query the open offers for the market trading horizon
        open_offers = self._query_data_free(
            f"SELECT * FROM {self.db_param.NAME_TABLE_POSITIONS_MARKET_EX_ANTE} "
            f"WHERE {self.db_param.ID_USER} LIKE '{id_user}' "
            f"AND {self.db_param.TYPE_POSITION} LIKE 'offer' "
            f"AND {self.db_param.TS_DELIVERY} "
            f"BETWEEN {ts_delivery_first} "
            f"AND {ts_delivery_last} "
            f"ORDER BY {self.db_param.TS_DELIVERY}"
        )

        if archive:
            self.insert(table_name=self.db_param.NAME_TABLE_POSITIONS_MARKET_EX_ANTE_ARCHIVE,
                        df_insert=open_bids)
            self.insert(table_name=self.db_param.NAME_TABLE_POSITIONS_MARKET_EX_ANTE_ARCHIVE,
                        df_insert=open_offers)

        if clear_table:
            self._clear_table(self.db_param.NAME_TABLE_POSITIONS_MARKET_EX_ANTE)

        return open_bids, open_offers

    def get_positions_archive(self, id_user="%%", ts_delivery_first=None, ts_delivery_last=None):

        ts_delivery_first = ts_delivery_first if ts_delivery_first is not None else 0
        ts_delivery_last = ts_delivery_last if ts_delivery_last is not None else 2147483647

        # query the open bids for the market trading horizon
        bids_archived = self._query_data_free(
            f"SELECT * FROM {self.db_param.NAME_TABLE_POSITIONS_MARKET_EX_ANTE_ARCHIVE} "
            f"WHERE {self.db_param.ID_USER} LIKE '{id_user}' "
            f"AND {self.db_param.TYPE_POSITION} LIKE 'bid' "
            f"AND {self.db_param.TS_DELIVERY} "
            f"BETWEEN {ts_delivery_first} "
            f"AND {ts_delivery_last} "
            f"ORDER BY {self.db_param.TS_DELIVERY}"
        )

        # query the open offers for the market trading horizon
        offers_archived = self._query_data_free(
            f"SELECT * FROM {self.db_param.NAME_TABLE_POSITIONS_MARKET_EX_ANTE_ARCHIVE} "
            f"WHERE {self.db_param.ID_USER} LIKE '{id_user}' "
            f"AND {self.db_param.TYPE_POSITION} LIKE 'offer' "
            f"AND {self.db_param.TS_DELIVERY} "
            f"BETWEEN {ts_delivery_first} "
            f"AND {ts_delivery_last} "
            f"ORDER BY {self.db_param.TS_DELIVERY}"
        )

        return bids_archived, offers_archived

    # Admins only

    ###################################################
    # Functions for the market results table
    # Market participants only

    def get_results_market_ex_ante(self, table_name=None, id_user="%%", ts_delivery_first=None, ts_delivery_last=None,
                                   t_cleared_first=None, t_cleared_last=None):

        # generate table name to be queried
        if table_name is None:
            table_name = self.db_param.NAME_TABLE_RESULTS_MARKET_EX_ANTE_ + self.lem_config["types_clearing_ex_ante"][0]
        # price_name = f"{self.db_param.PRICE_ENERGY_CLEARED}_{self.lem_config['types_pricing'][0]}"

        # query the matched bids for the market trading horizon
        matched_bids_by_timestep = None
        ts_delivery_first = ts_delivery_first if ts_delivery_first is not None else 0
        ts_delivery_last = ts_delivery_last if ts_delivery_last is not None else 2147483647
        t_cleared_first = t_cleared_first if t_cleared_first is not None else 0
        t_cleared_last = t_cleared_last if t_cleared_last is not None else 2147483647

        matched_bids = self._query_data_free(
            f"SELECT * FROM {table_name} "
            f"WHERE ({self.db_param.ID_USER_BID} LIKE '{id_user}' "
            f"OR {self.db_param.ID_USER_OFFER} LIKE '{id_user}') "
            f"AND {self.db_param.TS_DELIVERY} "
            f"BETWEEN '{ts_delivery_first}' "
            f"AND '{ts_delivery_last}' "
            f"AND {self.db_param.T_CLEARED} "
            f"BETWEEN '{t_cleared_first}' "
            f"AND '{t_cleared_last}' "
            f"ORDER BY {self.db_param.TS_DELIVERY}")

        if id_user != "%%" and len(matched_bids):
            # summate matched market bids for id_user for the market trading horizon
            # initialize dataframe
            matched_bids_by_timestep = pd.DataFrame(
                [[ts_d, 0] for ts_d in range(ts_delivery_first, ts_delivery_last + 1, 900)],
                columns=[self.db_param.TS_DELIVERY, "net_bids"]).set_index(self.db_param.TS_DELIVERY)

            # calc qty energy bid by
            matched_bids["qty_energy_bid"] = \
                matched_bids[matched_bids[self.db_param.ID_USER_BID] == id_user]["qty_energy_traded"]

            # calc qty energy offered by user
            matched_bids["qty_energy_offered"] = matched_bids[matched_bids["id_user_offer"] == id_user][
                "qty_energy_traded"]

            matched_bids = matched_bids.fillna(0)
            matched_bids["net_bids"] = matched_bids["qty_energy_offered"] - matched_bids["qty_energy_bid"]

            if len(matched_bids):
                matched_bids_by_timestep["net_bids"] = matched_bids.groupby(self.db_param.TS_DELIVERY).sum()["net_bids"]
            else:
                pass
            matched_bids_by_timestep = matched_bids_by_timestep.fillna(0)

        elif id_user != "%%":
            matched_bids_by_timestep = pd.DataFrame(
                [[ts_d, 0] for ts_d in range(ts_delivery_first, ts_delivery_last + 1, 900)],
                columns=[self.db_param.TS_DELIVERY, "net_bids"]).set_index(self.db_param.TS_DELIVERY)

        return matched_bids, matched_bids_by_timestep

    # Admins only

    def log_results_market(self, name_table, results_market):
        # Write results back to database
        self.insert(table_name=name_table,
                    df_insert=results_market)

    ###################################################
    # Functions for the settlement flags table
    # Market participants only

    # Admins only

    def set_status_settlement(self, df_in):
        self.upsert(table_name=self.db_param.NAME_TABLE_STATUS_SETTLEMENT,
                    df_insert=df_in)

    def get_status_settlement(self, ts_delivery=None):
        if ts_delivery is None:
            return self._query_data_free(f"SELECT * FROM {self.db_param.NAME_TABLE_STATUS_SETTLEMENT}  "
                                         f"ORDER BY {self.db_param.TS_DELIVERY}")
        else:
            return self._query_data_free(f"SELECT * FROM {self.db_param.NAME_TABLE_STATUS_SETTLEMENT}  "
                                         f"WHERE {self.db_param.TS_DELIVERY}={ts_delivery}")

    ###################################################
    # Functions for the cumulative meter readings table
    # Market participants only

    def log_meter_readings_cumulative(self, df_readings_meter):
        self.upsert(table_name=self.db_param.NAME_TABLE_READINGS_METER_CUMULATIVE,
                    df_insert=df_readings_meter)

    def get_meter_readings_cumulative(self, t_reading_first, t_reading_last,
                                      id_meter="%%"):
        # query cumulative meter readings
        readings_meter_cumulative = self._query_data_free(
            f"SELECT * FROM {self.db_param.NAME_TABLE_READINGS_METER_CUMULATIVE} "
            f"WHERE {self.db_param.ID_METER} LIKE '{id_meter}' "
            f"AND {self.db_param.T_READING} "
            f"BETWEEN '{t_reading_first}' "
            f"AND '{t_reading_last}' "
            f"ORDER BY {self.db_param.T_READING}")
        return readings_meter_cumulative

    # Admins only

    ###################################################
    # Functions for the meter reading deltas table
    # Market participants only

    # Admins only

    def log_readings_meter_delta(self, df_readings):
        if len(df_readings):
            self.upsert(table_name=self.db_param.NAME_TABLE_READINGS_METER_DELTA,
                        df_insert=df_readings)

    def get_meter_readings_delta(self, id_meter="%%", ts_delivery_first=None, ts_delivery_last=None):
        # query quarter-hourly energy flows

        ts_delivery_first = ts_delivery_first if ts_delivery_first is not None else 0
        ts_delivery_last = ts_delivery_last if ts_delivery_last is not None else 2147483647

        readings_meter_delta = self._query_data_free(
            f"SELECT * FROM {self.db_param.NAME_TABLE_READINGS_METER_DELTA} "
            f"WHERE {self.db_param.ID_METER} LIKE '{id_meter}' "
            f"AND {self.db_param.TS_DELIVERY} "
            f"BETWEEN '{ts_delivery_first}' "
            f"AND '{ts_delivery_last}' "
            f"ORDER BY {self.db_param.TS_DELIVERY}")
        return readings_meter_delta

    def get_meter_readings_by_type(self, ts_delivery, types_meters=None):
        if types_meters is None:
            types_meters = []
        if len(types_meters) == 0:
            types_meters = [0, 1, 2, 3, 4, 5]

        df_meters = self._query_data_free(f"SELECT * FROM {self.db_param.NAME_TABLE_INFO_METER}"
                                          f" WHERE {self.db_param.TS_DELIVERY_FIRST} <= {ts_delivery} "
                                          f" AND {self.db_param.TS_DELIVERY_LAST} >= {ts_delivery}")
        df_meters = df_meters[df_meters["type_meter"].isin([self.lem_config["types_meter"][i] for i in types_meters])]
        list_meters = list(df_meters["id_meter"])

        str_list_meters = "\'" + '\', \''.join(list_meters) + "\'"

        sql = f"SELECT * FROM {self.db_param.NAME_TABLE_READINGS_METER_DELTA} " \
              f"WHERE {self.db_param.ID_METER} IN ({str_list_meters}) " \
              f"AND {self.db_param.TS_DELIVERY} = {ts_delivery} " \
              f"ORDER BY {self.db_param.TS_DELIVERY}"

        return self._query_data_free(sql)

    ###################################################
    # Functions for the ex_post_pricing results table
    # Market participants only

    # Admins only

    def log_results_market_ex_post(self, df_in, table_name=None):
        if table_name is None:
            table_name = self.db_param.NAME_TABLE_RESULTS_MARKET_EX_POST_ + self.lem_config["types_clearing_ex_post"][0]
        self.upsert(table_name=table_name,
                    df_insert=df_in)

    def get_results_market_ex_post(self, table_name=None, ts_delivery_first=None, ts_delivery_last=None):
        if table_name is None:
            table_name = self.db_param.NAME_TABLE_RESULTS_MARKET_EX_POST_ + self.lem_config["types_clearing_ex_post"][0]
        if ts_delivery_first is None and ts_delivery_last is None:
            sql = f"SELECT * FROM {table_name} " \
                  f"ORDER BY self.db_param.TS_DELIVERY"
        elif ts_delivery_first is not None and ts_delivery_last is None:
            sql = f"SELECT * FROM {table_name} " \
                  f"WHERE {self.db_param.TS_DELIVERY} = {ts_delivery_first}"
        else:
            sql = f"SELECT * FROM {table_name} " \
                  f"WHERE {self.db_param.TS_DELIVERY} BETWEEN {ts_delivery_first} " \
                  f"AND {ts_delivery_last}"
        return self._query_data_free(sql)

    ###################################################
    # Functions for the balancing energy table
    # Market participants only

    def get_energy_balancing(self, ts_delivery=None):
        sql = f"SELECT * FROM {self.db_param.NAME_TABLE_ENERGY_BALANCING}"

        if ts_delivery is not None:
            sql += f" WHERE {self.db_param.TS_DELIVERY} = '{ts_delivery}'"

        return self._query_data_free(sql)

    # Admins only

    def log_energy_balancing(self, df_in):
        self.upsert(table_name=self.db_param.NAME_TABLE_ENERGY_BALANCING,
                    df_insert=df_in)

    ###################################################
    # Functions for the settlement prices table
    # Market participants only

    def get_prices_settlement(self, ts_delivery_first=None, ts_delivery_last=None):

        ts_delivery_first = ts_delivery_first if ts_delivery_first is not None else 0
        ts_delivery_last = ts_delivery_last if ts_delivery_last is not None else ts_delivery_first

        sql = f"SELECT * FROM {self.db_param.NAME_TABLE_PRICES_SETTLEMENT} " \
              f"WHERE {self.db_param.TS_DELIVERY} " \
              f"BETWEEN {ts_delivery_first} AND {ts_delivery_last} " \
              f"ORDER BY {self.db_param.TS_DELIVERY}  "

        return self._query_data_free(sql)

    # Admins only

    def set_prices_settlement(self, df_settlement):
        self.upsert(table_name=self.db_param.NAME_TABLE_PRICES_SETTLEMENT,
                    df_insert=df_settlement)

    ###################################################
    # Functions for the transaction logging table
    # Market participants only

    def get_logs_transactions(self, id_user="%%", ts_delivery_first=None,
                              ts_delivery_last=None):
        # query the settlement_logs

        ts_delivery_first = ts_delivery_first if ts_delivery_first is not None else 0
        ts_delivery_last = ts_delivery_last if ts_delivery_last is not None else 2147483647

        logs_transactions = self._query_data_free(
            f"SELECT * FROM {self.db_param.NAME_TABLE_LOGS_TRANSACTIONS} "
            f"WHERE {self.db_param.ID_USER} LIKE '{id_user}' "
            f"AND {self.db_param.TS_DELIVERY} "
            f"BETWEEN '{ts_delivery_first}' "
            f"AND '{ts_delivery_last}' "
            f"ORDER BY {self.db_param.TS_DELIVERY}")
        return logs_transactions

    # Admins only

    def update_balance_user(self, update_balance_df):
        conn = self.engine.connect()

        for _, row in update_balance_df.iterrows():
            sql = f"UPDATE {self.db_param.NAME_TABLE_INFO_USER} " \
                  f" SET {self.db_param.BALANCE_ACCOUNT} = {self.db_param.BALANCE_ACCOUNT}" \
                  f" + {row[self.db_param.DELTA_BALANCE]}," \
                  f" {self.db_param.T_UPDATE_BALANCE} = {row[self.db_param.T_UPDATE_BALANCE]}" \
                  f" WHERE {self.db_param.ID_USER} = '{row[self.db_param.ID_USER]}'"

            conn.execute(sql)

        conn.close()

    def log_transactions(self, df_tx):
        # Write results back to database
        self.insert(table_name=self.db_param.NAME_TABLE_LOGS_TRANSACTIONS,
                    df_insert=df_tx)

    ######################################################################
    # General functions
    def insert(self, table_name, df_insert):
        df_insert.to_sql(name=table_name,
                         con=self.engine,
                         if_exists='append',
                         index=False)

    def upsert(self, table_name, df_insert):
        sql = f"INSERT INTO {table_name}"
        list_columns_all = self.get_table_columns(table_name)
        sql += " ("
        for column in list_columns_all[:-1]:
            sql += f"{column}, "
        sql += f"{list_columns_all[-1]}) VALUES "

        for i, (index, row) in enumerate(df_insert.iterrows()):
            sql += f"("
            column = ""
            for column in list_columns_all[:-1]:
                if type(row[column]) == str:
                    sql += f"'{row[column]}', "
                else:
                    sql += f"{row[column]}, "
            if type(row[column]) == str:
                sql += f"'{row[list_columns_all[-1]]}')"
            else:
                sql += f"{row[list_columns_all[-1]]})"

            if i < len(df_insert)-1:
                sql += ", "

        sql += f" ON CONFLICT"
        list_columns_pk = self.get_table_columns(table_name, pk_only=True)
        sql += " ("
        for column in list_columns_pk[:-1]:
            sql += f"{column}, "
        sql += f"{list_columns_pk[-1]}) DO UPDATE SET "
        list_columns_not_pk = list(set(list_columns_all) - set(list_columns_pk))
        for column in list_columns_not_pk[:-1]:
            sql += f"{column} = EXCLUDED.{column}, "
        sql += f"{list_columns_not_pk[-1]} = EXCLUDED.{list_columns_not_pk[-1]};"
        self.engine.execute(sql)

    ###################################################
    # Internal functions
    def _query_data_free(self, sql):
        return pd.read_sql_query(sql, self.engine)

    def _init_table(self, table, clear_table=False, reformat_table=False):
        try:
            table_exists = self.engine.dialect.has_table(self.engine, table.name)
        # on some linux systems, the above line does not work. Use the following line instead
        except db.exc.ArgumentError:
            table_exists = db.inspect(self.engine).has_table(table.name)

        if not table_exists:  # If table does not exist, create new table.
            self._create_table(table)
        elif reformat_table:  # if table does exists and reformat is true, drop and re-add table
            self._drop_table(table.name)
            self._create_table(table)
        elif clear_table:  # If table does exist and delete is true, clear table contents.
            self._clear_table(table.name)

        # user_account "market_participant" may read the contents of this table
        if len(table.list_rights):
            sql = f"GRANT "
            for right in table.list_rights[:-1]:
                sql += f"{right}, "
            sql += f"{table.list_rights[-1]} "
            sql += f"ON TABLE \"{table.name}\" to {table.user_accounts};"
            with self.engine.begin() as conn:
                conn.execute(sql)

    def _create_table(self, lemlab_table):
        metadata = db.MetaData(self.engine)
        sql_table = db.Table(lemlab_table.name, metadata)
        for column in lemlab_table.list_columns:
            sql_table.append_column(db.Column(column.name, column.dtype, primary_key=column.pk))
        metadata.create_all()

    def _clear_table(self, table_name):
        try:
            self.engine.execute(f"DELETE FROM \"{table_name}\"")
        except (Exception, db.exc.DatabaseError) as error:
            print("Error: ", error)

    def _drop_table(self, table_name):
        try:
            self.engine.execute("DROP TABLE " + table_name)
        except (Exception, db.exc.DatabaseError) as error:
            print("Error: ", error)

    def _dynamize_tables_results_markets(self, market_type):
        if market_type == "ex_ante":
            key_types_clearing = "types_clearing_ex_ante"
            key_types_pricing = "types_pricing_ex_ante"
            table_name_base = self.db_param.NAME_TABLE_RESULTS_MARKET_EX_ANTE_
            table_base = self.db_param.table_results_market_ex_ante_base.replace()
        else:
            key_types_clearing = "types_clearing_ex_post"
            key_types_pricing = "types_pricing_ex_post"
            table_name_base = self.db_param.NAME_TABLE_RESULTS_MARKET_EX_POST_
            table_base = self.db_param.table_results_market_ex_post_base.replace()
        for key in self.lem_config[key_types_clearing]:
            new_table_name = table_name_base + self.lem_config[key_types_clearing][key]
            new_table = table_base.replace()
            new_table.name = new_table_name
            self.list_tables.append(new_table)

            for _key in self.lem_config[key_types_pricing]:
                self._add_column_to_table(
                    table_name=new_table_name,
                    _column=db_p.LemlabColumn(
                        f"{self.db_param.PRICE_ENERGY_MARKET_}" + f"{self.lem_config[key_types_pricing][_key]}",
                        db.BigInteger()))
            if market_type == "ex_ante" and not self.lem_config["share_quality_logging_extended"]:
                for _key in self.lem_config["types_quality"]:
                    self._add_column_to_table(
                        table_name=new_table_name,
                        _column=db_p.LemlabColumn(
                            f"{self.db_param.SHARE_QUALITY_OFFERS_CLEARED_}"
                            + f"{self.lem_config['types_quality'][_key]}",
                            db.BigInteger()))
            elif market_type == "ex_ante":
                for _key in self.lem_config["types_quality"]:
                    self._add_column_to_table(
                        table_name=new_table_name,
                        _column=db_p.LemlabColumn(
                            f"{self.db_param.SHARE_PREFERENCE_BIDS_}" + f"{self.lem_config['types_quality'][_key]}",
                            db.BigInteger()))
                    self._add_column_to_table(
                        table_name=new_table_name,
                        _column=db_p.LemlabColumn(
                            f"{self.db_param.SHARE_QUALITY_OFFERS_}" + f"{self.lem_config['types_quality'][_key]}",
                            db.BigInteger()))
                    self._add_column_to_table(
                        table_name=new_table_name,
                        _column=db_p.LemlabColumn(
                            f"{self.db_param.SHARE_PREFERENCE_BIDS_CLEARED_}"
                            + f"{self.lem_config['types_quality'][_key]}",
                            db.BigInteger()))
                    self._add_column_to_table(
                        table_name=new_table_name,
                        _column=db_p.LemlabColumn(
                            f"{self.db_param.SHARE_QUALITY_OFFERS_CLEARED_}"
                            + f"{self.lem_config['types_quality'][_key]}",
                            db.BigInteger()))
                self._add_column_to_table(
                    table_name=new_table_name,
                    _column=db_p.LemlabColumn(
                        f"{self.db_param.QTY_ENERGY_BIDS_CUM}",
                        db.BigInteger()))
                self._add_column_to_table(
                    table_name=new_table_name,
                    _column=db_p.LemlabColumn(
                        f"{self.db_param.QTY_ENERGY_OFFERS_CUM}",
                        db.BigInteger()))
                self._add_column_to_table(
                    table_name=new_table_name,
                    _column=db_p.LemlabColumn(
                        f"{self.db_param.QTY_ENERGY_TRADED_CUM}",
                        db.BigInteger()))
            else:
                for _key in self.lem_config["types_quality"]:
                    self._add_column_to_table(
                        table_name=new_table_name,
                        _column=db_p.LemlabColumn(
                            f"{self.db_param.SHARE_QUALITY_}" + f"{self.lem_config['types_quality'][_key]}",
                            db.BigInteger()))

    def _add_column_to_table(self, table_name, _column):
        for i, table in enumerate(self.list_tables):
            if table.name == table_name:
                self.list_tables[i].list_columns.append(_column)

    def _dynamize_table_logs_transactions(self):
        table_name_base = self.db_param.NAME_TABLE_LOGS_TRANSACTIONS
        table_base = self.db_param.table_logs_transactions_base
        new_table_name = table_name_base
        new_table = table_base.replace()
        new_table.name = new_table_name
        self.list_tables.append(new_table)

        for key in self.lem_config["types_quality"]:
            self._add_column_to_table(
                table_name=table_name_base,
                _column=db_p.LemlabColumn(
                    f"{self.db_param.SHARE_QUALITY_}" + f"{self.lem_config['types_quality'][key]}",
                    db.BigInteger()))

    def get_table_columns(self, table_name, pk_only=False, dtype=False):
        self.db_param.table_columns = {}
        for table in self.list_tables:
            if table.name == table_name:
                _list_columns = []
                _list_dtype = []
                for column in table.list_columns:
                    if (pk_only and column.pk) \
                            or not pk_only:
                        _list_columns.append(column.name)
                        if dtype:
                            _list_dtype.append(column.dtype.python_type)
                if dtype:
                    return _list_columns, _list_dtype
                return _list_columns

    def save_all_tables(self, path):
        for table in self.list_tables:
            df_table_contents = self._query_data_free(f"SELECT * FROM \"{table.name}\"")
            df_table_contents.to_csv(path + f"/{table.name}.csv")

    def _dynamize_db_param(self):
        # customize database formatting here
        # create market_results tables for every requested market result type
        # each table gets a column for each requested price type and for each energy quality
        if self.lem_config["types_clearing_ex_ante"]:
            self._dynamize_tables_results_markets(market_type="ex_ante")
        if self.lem_config["types_clearing_ex_post"]:
            self._dynamize_tables_results_markets(market_type="ex_post")
        # # logs transactions gets a column for each energy quality
        self._dynamize_table_logs_transactions()

    def drop_all_existing_tables(self, only_names=False, substring=None):
        # Get all existing table names
        list_tables = db.inspect(self.engine).get_table_names()
        # Check whether only_names is true
        if only_names:
            # extract only names containing substring
            list_tables = [i for i in list_tables if substring in i]
        # Drop all tables in table_names
        for table in list_tables:
            self._drop_table(table)
