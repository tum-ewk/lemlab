import time
import json
from bisect import bisect_left
import pandas as pd
import feather as ft
from pprint import pp


def settle_lem(db_obj_admin,
               path_simulation,
               config,
               t_now=None):
    # set current time
    t_now = round(time.time()) if t_now is None else t_now
    # check for which ts_delivery meter readings have been logged
    # calculate energy flows for those ts_delivery
    # label processed steps in status_settlement
    update_complete_meter_readings(db_obj=db_obj_admin)
    # set settlement prices for current simulation ts_delivery in database, as agents need these for their MPCs
    # alternative: adjust agents' settlement price prediction algorithms
    set_prices_settlement(db_obj=db_obj_admin,
                          path_simulation=path_simulation,
                          list_ts_delivery=[t_now - t_now % 900])
    # generate list of ts_delivery that are ready to be settled
    # this means meter readings have been processed
    list_ts_delivery_ready = _get_list_ts_delivery_ready(db_obj=db_obj_admin)
    # if ex-ante market is the active market
    # market transaction settlement is handled directly by ex-ante market clearing
    if config["lem"]["types_clearing_ex_post"]:
        set_community_price(db_obj=db_obj_admin,
                            path_simulation=path_simulation,
                            lem_config=config["lem"],
                            list_ts_delivery=list_ts_delivery_ready)

    if config["lem"]["types_clearing_ex_ante"]:
        # determine balancing energy flows
        determine_balancing_energy(db_obj=db_obj_admin,
                                   list_ts_delivery=list_ts_delivery_ready)
        # settle balancing energy costs with each user
        update_balance_balancing_costs(db_obj=db_obj_admin,
                                       list_ts_delivery=list_ts_delivery_ready,
                                       lem_config=config["lem"],
                                       t_now=t_now,
                                       id_supplier=config["supplier"]["id_user"])
    else:
        _update_balance_ex_post(db_obj=db_obj_admin,
                                id_supplier=config["supplier"]["id_user"],
                                lem_config=config["lem"],
                                list_ts_delivery=list_ts_delivery_ready,
                                t_now=t_now)
    # settle levy costs with each user
    # levies are all extra costs, taxes, grid ...
    update_balance_levies(db_obj=db_obj_admin,
                          list_ts_delivery=list_ts_delivery_ready,
                          lem_config=config["lem"],
                          t_now=t_now,
                          id_supplier=config["supplier"]["id_user"])

    # initialize new settlement status for current ts_delivery
    for ts_d in list_ts_delivery_ready:
        dict_status = {
            db_obj_admin.db_param.TS_DELIVERY: [ts_d],
            db_obj_admin.db_param.STATUS_METER_READINGS_PROCESSED: [1],
            db_obj_admin.db_param.STATUS_SETTLEMENT_COMPLETE: [1]
        }
        db_obj_admin.set_status_settlement(pd.DataFrame().from_dict(dict_status))


def update_complete_meter_readings(db_obj):
    df_clearing_log = db_obj.get_status_settlement()
    list_t_d_meter_readings_incomplete = list(df_clearing_log.loc[
                                                  df_clearing_log["status_meter_readings_processed"] == 0].ts_delivery)

    for ts_delivery in list_t_d_meter_readings_incomplete:
        # get a list of meters active at the ts_delivery under consideration
        list_meters = sorted(db_obj.get_list_all_meters(ts_delivery_active=ts_delivery))
        # get list of meters that logged a meter reading BEFORE AND AFTER the ts_delivery under consideration
        list_meters_logged = sorted(_get_list_meters_logged(db_obj, ts_delivery))

        # proceed only if all meters have logged values
        if list_meters == list_meters_logged:
            # print(f"    {ts_delivery} is commencing settlement.")
            list_meter_reading_delta = []
            df_metering_logs_cumulative = db_obj.get_meter_readings_cumulative(
                t_reading_first=ts_delivery,
                t_reading_last=ts_delivery + 900)
            df_metering_logs_cumulative_prev = \
                df_metering_logs_cumulative[df_metering_logs_cumulative["t_reading"] == ts_delivery]
            df_metering_logs_cumulative_now = \
                df_metering_logs_cumulative[df_metering_logs_cumulative["t_reading"] == ts_delivery + 900]
            for meter in list_meters:
                # determine metering delta and log to meter_readings_delta
                energy_in_delta = \
                    df_metering_logs_cumulative_now[
                        df_metering_logs_cumulative_now[db_obj.db_param.ID_METER] == meter].energy_in_cum.values[0] - \
                    df_metering_logs_cumulative_prev[
                        df_metering_logs_cumulative_prev[db_obj.db_param.ID_METER] == meter].energy_in_cum.values[0]
                energy_out_delta = \
                    df_metering_logs_cumulative_now[
                        df_metering_logs_cumulative_now[db_obj.db_param.ID_METER] == meter].energy_out_cum.values[0] - \
                    df_metering_logs_cumulative_prev[
                        df_metering_logs_cumulative_prev[db_obj.db_param.ID_METER] == meter].energy_out_cum.values[0]
                list_meter_reading_delta.append([ts_delivery, energy_in_delta, energy_out_delta, meter])

            df_meter_reading_delta = pd.DataFrame(list_meter_reading_delta,
                                                  columns=[db_obj.db_param.TS_DELIVERY, db_obj.db_param.ENERGY_IN,
                                                           db_obj.db_param.ENERGY_OUT, db_obj.db_param.ID_METER])
            db_obj.log_readings_meter_delta(df_meter_reading_delta)
            dict_status = {
                db_obj.db_param.TS_DELIVERY: [ts_delivery],
                db_obj.db_param.STATUS_METER_READINGS_PROCESSED: [1],
                db_obj.db_param.STATUS_SETTLEMENT_COMPLETE: [0]
            }
            db_obj.set_status_settlement(pd.DataFrame().from_dict(dict_status))
        else:
            # print(f"    {ts_delivery} cannot be settled yet as not all smart meters have logged their readings yet.")
            break


def determine_balancing_energy(db_obj, list_ts_delivery):
    for ts_d in list_ts_delivery:
        meter_readings_delta = db_obj.get_meter_readings_delta(ts_delivery_first=ts_d, ts_delivery_last=ts_d,
                                                               id_meter='%%grid%%')     # grid=main_meters
        market_results, _, = db_obj.get_results_market_ex_ante(ts_delivery_first=ts_d, ts_delivery_last=ts_d)
        for _, entry in meter_readings_delta.iterrows():
            current_meter_id = entry.loc[db_obj.db_param.ID_METER]
            current_actual_energy = entry.loc[db_obj.db_param.ENERGY_OUT] - entry.loc[db_obj.db_param.ENERGY_IN]
            current_market_energy = 0
            for _, result in market_results.iterrows():
                if result.loc[db_obj.db_param.ID_USER_BID] == current_meter_id:
                    current_market_energy -= result.loc[db_obj.db_param.QTY_ENERGY_TRADED]
                elif result.loc[db_obj.db_param.ID_USER_OFFER] == current_meter_id:
                    current_market_energy += result.loc[db_obj.db_param.QTY_ENERGY_TRADED]
            current_balancing_energy = current_actual_energy - current_market_energy

            dict_bal_ener = {
                db_obj.db_param.ID_METER: [current_meter_id],
                db_obj.db_param.TS_DELIVERY: [ts_d],
                db_obj.db_param.ENERGY_BALANCING_POSITIVE: [_decomp_float(float_in=current_balancing_energy,
                                                                          return_val="pos")],
                db_obj.db_param.ENERGY_BALANCING_NEGATIVE: [_decomp_float(float_in=current_balancing_energy,
                                                                          return_val="neg")]
            }
            db_obj.log_energy_balancing(pd.DataFrame(dict_bal_ener))


def set_prices_settlement(db_obj, path_simulation, list_ts_delivery):
    # read platform config
    with open(f"{path_simulation}/platform/config_account.json", "r") as read_file:
        config_dict = json.load(read_file)

    euro_kwh_to_sigma_wh = db_obj.db_param.EURO_TO_SIGMA / 1000

    for ts_delivery in list_ts_delivery:
        price_bal_pos = config_dict["price_energy_balancing_positive"] * euro_kwh_to_sigma_wh
        price_bal_neg = config_dict["price_energy_balancing_negative"] * euro_kwh_to_sigma_wh
        price_levies_pos = config_dict["price_energy_levies_positive"] * euro_kwh_to_sigma_wh
        price_levies_neg = config_dict["price_energy_levies_negative"] * euro_kwh_to_sigma_wh
        if config_dict["bal_energy_pricing_mechanism"] == "file":
            df_bal_prices = ft.read_dataframe(f"{path_simulation}/platform/balancing_prices.ft"
                                              ).set_index("timestamp")
            price_bal_pos = df_bal_prices.loc[ts_delivery, "price_balancing_energy_positive"] * euro_kwh_to_sigma_wh
            price_bal_neg = df_bal_prices.loc[ts_delivery, "price_balancing_energy_negative"] * euro_kwh_to_sigma_wh
        if config_dict["levy_pricing_mechanism"] == "file":
            df_levy_prices = ft.read_dataframe(f"{path_simulation}/platform/levy_prices.ft"
                                               ).set_index("timestamp")
            price_levies_pos = df_levy_prices.loc[ts_delivery, "price_energy_levies_positive"] * euro_kwh_to_sigma_wh
            price_levies_neg = df_levy_prices.loc[ts_delivery, "price_energy_levies_negative"] * euro_kwh_to_sigma_wh

        dict_settlement_prices = {
            db_obj.db_param.TS_DELIVERY: [ts_delivery],
            db_obj.db_param.PRICE_ENERGY_BALANCING_POSITIVE: [price_bal_pos],
            db_obj.db_param.PRICE_ENERGY_BALANCING_NEGATIVE: [price_bal_neg],
            db_obj.db_param.PRICE_ENERGY_LEVIES_POSITIVE: [price_levies_pos],
            db_obj.db_param.PRICE_ENERGY_LEVIES_NEGATIVE: [price_levies_neg]
        }
        db_obj.set_prices_settlement(pd.DataFrame().from_dict(dict_settlement_prices))


def update_balance_balancing_costs(db_obj, t_now, lem_config, list_ts_delivery, id_supplier="supplier01"):
    dict_map_to_user = db_obj.get_mapping_to_user()

    for ts_d in list_ts_delivery:
        settlement_prices = db_obj.get_prices_settlement(ts_delivery_first=ts_d)

        pos_bal_ener_price = int(settlement_prices[db_obj.db_param.PRICE_ENERGY_BALANCING_POSITIVE])
        neg_bal_ener_price = int(settlement_prices[db_obj.db_param.PRICE_ENERGY_BALANCING_NEGATIVE])

        balancing_energies = db_obj.get_energy_balancing(ts_delivery=ts_d)

        # set transaction form
        dict_transactions = {
            db_obj.db_param.ID_USER: [],
            db_obj.db_param.TS_DELIVERY: [],
            db_obj.db_param.PRICE_ENERGY_MARKET: [],
            db_obj.db_param.TYPE_TRANSACTION: [],
            db_obj.db_param.QTY_ENERGY: [],
            db_obj.db_param.DELTA_BALANCE: [],
            db_obj.db_param.T_UPDATE_BALANCE: [],
        }
        for quality in lem_config["types_quality"]:
            dict_transactions.update({db_obj.db_param.SHARE_QUALITY_ + lem_config["types_quality"][quality]: []})

        for _, entry in balancing_energies.iterrows():
            if entry.loc[db_obj.db_param.ENERGY_BALANCING_POSITIVE] != 0:
                transaction_value = entry.loc[db_obj.db_param.ENERGY_BALANCING_POSITIVE] * pos_bal_ener_price

                # credit supplier
                dict_transactions[db_obj.db_param.ID_USER].append(id_supplier)
                dict_transactions[db_obj.db_param.TS_DELIVERY].append(ts_d)
                dict_transactions[db_obj.db_param.PRICE_ENERGY_MARKET].append(pos_bal_ener_price)
                dict_transactions[db_obj.db_param.TYPE_TRANSACTION].append("balancing")
                dict_transactions[db_obj.db_param.QTY_ENERGY].append(
                    entry.loc[db_obj.db_param.ENERGY_BALANCING_POSITIVE])
                dict_transactions[db_obj.db_param.DELTA_BALANCE].append(transaction_value)
                dict_transactions[db_obj.db_param.T_UPDATE_BALANCE].append(t_now)
                for quality in lem_config["types_quality"]:
                    dict_transactions[db_obj.db_param.SHARE_QUALITY_ + lem_config["types_quality"][quality]].append(0)

                # debit consumer
                dict_transactions[db_obj.db_param.ID_USER].append(dict_map_to_user[entry.loc[db_obj.db_param.ID_METER]])
                dict_transactions[db_obj.db_param.TS_DELIVERY].append(ts_d)
                dict_transactions[db_obj.db_param.PRICE_ENERGY_MARKET].append(pos_bal_ener_price)
                dict_transactions[db_obj.db_param.TYPE_TRANSACTION].append("balancing")
                dict_transactions[db_obj.db_param.QTY_ENERGY].append(
                    entry.loc[db_obj.db_param.ENERGY_BALANCING_POSITIVE])
                dict_transactions[db_obj.db_param.DELTA_BALANCE].append(-1 * transaction_value)
                dict_transactions[db_obj.db_param.T_UPDATE_BALANCE].append(t_now)
                for quality in lem_config["types_quality"]:
                    dict_transactions[db_obj.db_param.SHARE_QUALITY_ + lem_config["types_quality"][quality]].append(0)

            elif entry.loc[db_obj.db_param.ENERGY_BALANCING_NEGATIVE] != 0:
                transaction_value = entry.loc[db_obj.db_param.ENERGY_BALANCING_NEGATIVE] * neg_bal_ener_price
                # credit supplier
                dict_transactions[db_obj.db_param.ID_USER].append(id_supplier)
                dict_transactions[db_obj.db_param.TS_DELIVERY].append(ts_d)
                dict_transactions[db_obj.db_param.PRICE_ENERGY_MARKET].append(neg_bal_ener_price)
                dict_transactions[db_obj.db_param.TYPE_TRANSACTION].append("balancing")
                dict_transactions[db_obj.db_param.QTY_ENERGY].append(
                    entry.loc[db_obj.db_param.ENERGY_BALANCING_NEGATIVE])
                dict_transactions[db_obj.db_param.DELTA_BALANCE].append(transaction_value)
                dict_transactions[db_obj.db_param.T_UPDATE_BALANCE].append(t_now)
                for quality in lem_config["types_quality"]:
                    dict_transactions[db_obj.db_param.SHARE_QUALITY_ + lem_config["types_quality"][quality]].append(0)

                # debit consumer
                dict_transactions[db_obj.db_param.ID_USER].append(
                    dict_map_to_user[entry.loc[db_obj.db_param.ID_METER]])
                dict_transactions[db_obj.db_param.TS_DELIVERY].append(ts_d)
                dict_transactions[db_obj.db_param.PRICE_ENERGY_MARKET].append(neg_bal_ener_price)
                dict_transactions[db_obj.db_param.TYPE_TRANSACTION].append("balancing")
                dict_transactions[db_obj.db_param.QTY_ENERGY].append(
                    entry.loc[db_obj.db_param.ENERGY_BALANCING_NEGATIVE])
                dict_transactions[db_obj.db_param.DELTA_BALANCE].append(-1 * transaction_value)
                dict_transactions[db_obj.db_param.T_UPDATE_BALANCE].append(t_now)
                for quality in lem_config["types_quality"]:
                    dict_transactions[db_obj.db_param.SHARE_QUALITY_ + lem_config["types_quality"][quality]].append(0)
    if len(list_ts_delivery) and len(dict_transactions[db_obj.db_param.ID_USER]):
        db_obj.log_transactions(pd.DataFrame.from_dict(dict_transactions))
        db_obj.update_balance_user(pd.DataFrame.from_dict(dict_transactions))


def update_balance_levies(db_obj, t_now, lem_config, list_ts_delivery, id_supplier="supplier01"):
    dict_map_to_user = db_obj.get_mapping_to_user()
    # set transaction form
    dict_transactions = {
        db_obj.db_param.ID_USER: [],
        db_obj.db_param.TS_DELIVERY: [],
        db_obj.db_param.PRICE_ENERGY_MARKET: [],
        db_obj.db_param.TYPE_TRANSACTION: [],
        db_obj.db_param.QTY_ENERGY: [],
        db_obj.db_param.DELTA_BALANCE: [],
        db_obj.db_param.T_UPDATE_BALANCE: [],
    }
    for quality in lem_config["types_quality"]:
        dict_transactions.update({db_obj.db_param.SHARE_QUALITY_ + lem_config["types_quality"][quality]: []})

    for ts_d in list_ts_delivery:
        meter_readings_delta = db_obj.get_main_meter_net_flow(ts_d)
        settlement_prices = db_obj.get_prices_settlement(ts_delivery_first=ts_d)
        levies_pos = int(settlement_prices[db_obj.db_param.PRICE_ENERGY_LEVIES_POSITIVE])
        levies_neg = int(settlement_prices[db_obj.db_param.PRICE_ENERGY_LEVIES_NEGATIVE])

        for _, entry in meter_readings_delta.iterrows():
            if entry.loc[db_obj.db_param.ENERGY_OUT] != 0 and levies_pos != 0:
                transaction_value = entry.loc[db_obj.db_param.ENERGY_OUT] * levies_pos
                # credit supplier
                dict_transactions[db_obj.db_param.ID_USER].append(id_supplier)
                dict_transactions[db_obj.db_param.TS_DELIVERY].append(ts_d)
                dict_transactions[db_obj.db_param.PRICE_ENERGY_MARKET].append(levies_pos)
                dict_transactions[db_obj.db_param.TYPE_TRANSACTION].append("levies")
                dict_transactions[db_obj.db_param.QTY_ENERGY].append(entry.loc[db_obj.db_param.ENERGY_OUT])
                dict_transactions[db_obj.db_param.DELTA_BALANCE].append(transaction_value)
                dict_transactions[db_obj.db_param.T_UPDATE_BALANCE].append(t_now)
                for quality in lem_config["types_quality"]:
                    dict_transactions[db_obj.db_param.SHARE_QUALITY_ + lem_config["types_quality"][quality]].append(0)

                # debit consumer
                dict_transactions[db_obj.db_param.ID_USER].append(dict_map_to_user[entry.loc[db_obj.db_param.ID_METER]])
                dict_transactions[db_obj.db_param.TS_DELIVERY].append(ts_d)
                dict_transactions[db_obj.db_param.PRICE_ENERGY_MARKET].append(levies_pos)
                dict_transactions[db_obj.db_param.TYPE_TRANSACTION].append("levies")
                dict_transactions[db_obj.db_param.QTY_ENERGY].append(entry.loc[db_obj.db_param.ENERGY_OUT])
                dict_transactions[db_obj.db_param.DELTA_BALANCE].append(-1 * transaction_value)
                dict_transactions[db_obj.db_param.T_UPDATE_BALANCE].append(t_now)
                for quality in lem_config["types_quality"]:
                    dict_transactions[db_obj.db_param.SHARE_QUALITY_ + lem_config["types_quality"][quality]].append(0)

            elif int(entry.loc[db_obj.db_param.ENERGY_IN]) != 0 and levies_neg != 0:
                transaction_value = entry.loc[db_obj.db_param.ENERGY_IN] * levies_neg
                # credit supplier
                dict_transactions[db_obj.db_param.ID_USER].append(id_supplier)
                dict_transactions[db_obj.db_param.TS_DELIVERY].append(ts_d)
                dict_transactions[db_obj.db_param.PRICE_ENERGY_MARKET].append(levies_neg)
                dict_transactions[db_obj.db_param.TYPE_TRANSACTION].append("levies")
                dict_transactions[db_obj.db_param.QTY_ENERGY].append(entry.loc[db_obj.db_param.ENERGY_IN])
                dict_transactions[db_obj.db_param.DELTA_BALANCE].append(transaction_value)
                dict_transactions[db_obj.db_param.T_UPDATE_BALANCE].append(t_now)
                for quality in lem_config["types_quality"]:
                    dict_transactions[db_obj.db_param.SHARE_QUALITY_ + lem_config["types_quality"][quality]].append(0)

                # debit consumer
                dict_transactions[db_obj.db_param.ID_USER].append(dict_map_to_user[entry.loc[db_obj.db_param.ID_METER]])
                dict_transactions[db_obj.db_param.TS_DELIVERY].append(ts_d)
                dict_transactions[db_obj.db_param.PRICE_ENERGY_MARKET].append(levies_neg)
                dict_transactions[db_obj.db_param.TYPE_TRANSACTION].append("levies")
                dict_transactions[db_obj.db_param.QTY_ENERGY].append(entry.loc[db_obj.db_param.ENERGY_IN])
                dict_transactions[db_obj.db_param.DELTA_BALANCE].append(-1 * transaction_value)
                dict_transactions[db_obj.db_param.T_UPDATE_BALANCE].append(t_now)
                for quality in lem_config["types_quality"]:
                    dict_transactions[db_obj.db_param.SHARE_QUALITY_ + lem_config["types_quality"][quality]].append(0)
    if len(list_ts_delivery) and len(dict_transactions[db_obj.db_param.ID_USER]):
        db_obj.log_transactions(pd.DataFrame.from_dict(dict_transactions))
        db_obj.update_balance_user(pd.DataFrame.from_dict(dict_transactions))


def determine_prices_ex_post_markets(db_obj_admin, path_simulation, lem_config, list_ts_delivery, t_now=None):
    for type_clearing in lem_config["types_clearing_ex_post"]:
        if lem_config["types_clearing_ex_post"][type_clearing] == "community":
            set_community_price(db_obj=db_obj_admin, path_simulation=path_simulation,
                                lem_config=lem_config, list_ts_delivery=list_ts_delivery,
                                t_now=t_now)


def set_community_price(db_obj, path_simulation, lem_config, list_ts_delivery):
    """
    Calculate the Strommunity price for the chosen time of delivery (default, previous timestep) and post the result
    to the database.

    :return:
    """
    euro_kwh_to_sigma_wh = db_obj.db_param.EURO_TO_SIGMA / 1000
    dict_lookup_tables = {}
    dict_results_ex_post = {db_obj.db_param.TS_DELIVERY: []}

    for type_pricing in lem_config["types_pricing_ex_post"]:
        with open(f"./{path_simulation}/platform"
                  f"/{lem_config['types_pricing_ex_post'][type_pricing]}.json", "r") as read_file:
            dict_lookup_tables[lem_config['types_pricing_ex_post'][type_pricing]] = json.load(read_file)
        dict_results_ex_post.update({db_obj.db_param.PRICE_ENERGY_MARKET_
                                     + lem_config['types_pricing_ex_post'][type_pricing]: []})

    for quality in lem_config["types_quality"]:
        dict_results_ex_post.update({db_obj.db_param.SHARE_QUALITY_ + lem_config["types_quality"][quality]: []})

    for ts_d in list_ts_delivery:
        main_meter_net_flows = db_obj.get_main_meter_net_flow(ts_d)

        # initalize feed-in and consumption
        # consumption is non-zero to avoid division by zero
        total_feed_in = 0
        total_consumption = 1

        for entry in main_meter_net_flows.itertuples():
            total_feed_in += entry.energy_out
            total_consumption += entry.energy_in

        for type_pricing in lem_config["types_pricing_ex_post"]:
            lookup_supply_ratio = dict_lookup_tables[lem_config['types_pricing_ex_post'][type_pricing]][
                "supply_demand_ratio"]
            lookup_price = dict_lookup_tables[lem_config['types_pricing_ex_post'][type_pricing]]["price"]

            local_share = total_feed_in / float(total_consumption)
            price = _lookup(local_share, lookup_supply_ratio, lookup_price) * euro_kwh_to_sigma_wh

            dict_results_ex_post[db_obj.db_param.TS_DELIVERY].append(ts_d)
            dict_results_ex_post[db_obj.db_param.PRICE_ENERGY_MARKET_
                                 + lem_config['types_pricing_ex_post'][type_pricing]].append(price)

        local_share = round(local_share * 100)
        local_share = 100 if local_share > 100 else local_share
        local_share = 0 if local_share < 0 else local_share
        for quality in lem_config["types_quality"]:
            if lem_config["types_quality"][quality] == "local":
                dict_results_ex_post[
                    db_obj.db_param.SHARE_QUALITY_ + lem_config["types_quality"][quality]].append(local_share)
            elif lem_config["types_quality"][quality] == "NA":
                dict_results_ex_post[
                    db_obj.db_param.SHARE_QUALITY_ + lem_config["types_quality"][quality]].append(100 - local_share)
            else:
                dict_results_ex_post[
                    db_obj.db_param.SHARE_QUALITY_ + lem_config["types_quality"][quality]].append(0)

    if len(list_ts_delivery) and len(dict_results_ex_post[db_obj.db_param.TS_DELIVERY]):
        db_obj.log_results_market_ex_post(pd.DataFrame(dict_results_ex_post))


def _update_balance_ex_post(db_obj, id_supplier, t_now, list_ts_delivery, lem_config):
    dict_map_to_user = db_obj.get_mapping_to_user()

    for ts_d in list_ts_delivery:
        meter_readings_delta = db_obj.get_main_meter_net_flow(ts_d)

        ex_post_prices = db_obj.get_results_market_ex_post(ts_delivery_first=ts_d)
        price = int(ex_post_prices[db_obj.db_param.PRICE_ENERGY_MARKET_
                                   + lem_config['types_pricing_ex_post'][0]])

        # set transaction form
        dict_transactions = {
            db_obj.db_param.ID_USER: [],
            db_obj.db_param.TS_DELIVERY: [],
            db_obj.db_param.PRICE_ENERGY_MARKET: [],
            db_obj.db_param.TYPE_TRANSACTION: [],
            db_obj.db_param.QTY_ENERGY: [],
            db_obj.db_param.DELTA_BALANCE: [],
            db_obj.db_param.T_UPDATE_BALANCE: [],
        }
        for quality in lem_config["types_quality"]:
            dict_transactions.update({db_obj.db_param.SHARE_QUALITY_ + lem_config["types_quality"][quality]: []})

        for _, entry in meter_readings_delta.iterrows():
            if entry.loc[db_obj.db_param.ENERGY_OUT] != 0:
                transaction_value = entry.loc[db_obj.db_param.ENERGY_OUT] * price
                # credit supplier
                dict_transactions[db_obj.db_param.ID_USER].append(id_supplier)
                dict_transactions[db_obj.db_param.TS_DELIVERY].append(ts_d)
                dict_transactions[db_obj.db_param.PRICE_ENERGY_MARKET].append(price)
                dict_transactions[db_obj.db_param.TYPE_TRANSACTION].append("market")
                dict_transactions[db_obj.db_param.QTY_ENERGY].append(-1 * entry.loc[db_obj.db_param.ENERGY_OUT])
                dict_transactions[db_obj.db_param.DELTA_BALANCE].append(transaction_value)
                dict_transactions[db_obj.db_param.T_UPDATE_BALANCE].append(t_now)
                for quality in lem_config["types_quality"]:
                    dict_transactions[db_obj.db_param.SHARE_QUALITY_ + lem_config["types_quality"][quality]].append(0)

                # debit consumer
                dict_transactions[db_obj.db_param.ID_USER].append(dict_map_to_user[entry.loc[db_obj.db_param.ID_METER]])
                dict_transactions[db_obj.db_param.TS_DELIVERY].append(ts_d)
                dict_transactions[db_obj.db_param.PRICE_ENERGY_MARKET].append(price)
                dict_transactions[db_obj.db_param.TYPE_TRANSACTION].append("market")
                dict_transactions[db_obj.db_param.QTY_ENERGY].append(entry.loc[db_obj.db_param.ENERGY_OUT])
                dict_transactions[db_obj.db_param.DELTA_BALANCE].append(-1 * transaction_value)
                dict_transactions[db_obj.db_param.T_UPDATE_BALANCE].append(t_now)
                for quality in lem_config["types_quality"]:
                    dict_transactions[db_obj.db_param.SHARE_QUALITY_ + lem_config["types_quality"][quality]].append(0)

            elif int(entry.loc[db_obj.db_param.ENERGY_IN]) != 0:
                transaction_value = -1 * entry.loc[db_obj.db_param.ENERGY_IN] * price
                # credit supplier
                dict_transactions[db_obj.db_param.ID_USER].append(id_supplier)
                dict_transactions[db_obj.db_param.TS_DELIVERY].append(ts_d)
                dict_transactions[db_obj.db_param.PRICE_ENERGY_MARKET].append(price)
                dict_transactions[db_obj.db_param.TYPE_TRANSACTION].append("market")
                dict_transactions[db_obj.db_param.QTY_ENERGY].append(entry.loc[db_obj.db_param.ENERGY_IN])
                dict_transactions[db_obj.db_param.DELTA_BALANCE].append(transaction_value)
                dict_transactions[db_obj.db_param.T_UPDATE_BALANCE].append(t_now)
                for quality in lem_config["types_quality"]:
                    dict_transactions[db_obj.db_param.SHARE_QUALITY_ + lem_config["types_quality"][quality]].append(0)

                # debit consumer
                dict_transactions[db_obj.db_param.ID_USER].append(dict_map_to_user[entry.loc[db_obj.db_param.ID_METER]])
                dict_transactions[db_obj.db_param.TS_DELIVERY].append(ts_d)
                dict_transactions[db_obj.db_param.PRICE_ENERGY_MARKET].append(price)
                dict_transactions[db_obj.db_param.TYPE_TRANSACTION].append("market")
                dict_transactions[db_obj.db_param.QTY_ENERGY].append(-1 * entry.loc[db_obj.db_param.ENERGY_IN])
                dict_transactions[db_obj.db_param.DELTA_BALANCE].append(transaction_value)
                dict_transactions[db_obj.db_param.T_UPDATE_BALANCE].append(t_now)
                for quality in lem_config["types_quality"]:
                    dict_transactions[db_obj.db_param.SHARE_QUALITY_ + lem_config["types_quality"][quality]].append(0)
        if len(list_ts_delivery) and len(dict_transactions[db_obj.db_param.ID_USER]):
            db_obj.log_transactions(pd.DataFrame.from_dict(dict_transactions))
            db_obj.update_balance_user(pd.DataFrame.from_dict(dict_transactions))


########################################################################################################################
# Internal methods and functions
########################################################################################################################

def _get_list_ts_delivery_ready(db_obj):
    df_clearing_log = db_obj.get_status_settlement()
    list_ts_delivery_ready = \
        list(df_clearing_log.loc[(df_clearing_log[db_obj.db_param.STATUS_METER_READINGS_PROCESSED] == 1)
                                 & (df_clearing_log[db_obj.db_param.STATUS_SETTLEMENT_COMPLETE] == 0)
                                 ].ts_delivery)
    return list_ts_delivery_ready


def _get_list_meters_logged(db_obj, ts_delivery):
    # get list of meters that logged a meter reading at the beginning of the ts_delivery being examined
    df_meter_readings_cum = db_obj.get_meter_readings_cumulative(
        t_reading_first=ts_delivery,
        t_reading_last=ts_delivery + 900)
    list_logged_before = list(df_meter_readings_cum[df_meter_readings_cum["t_reading"] == ts_delivery].id_meter)
    list_logged_after = list(df_meter_readings_cum[df_meter_readings_cum["t_reading"] == ts_delivery + 900].id_meter)

    # return list of meters that logged at beginning and end of the ts_delivery being considered
    return list(set(list_logged_before).intersection(list_logged_after))


def _lookup(x, x_axis, y_axis):
    """
    Static internal method:
    Perform lookup on provided table. Find y-value for desired x-value

    :param x: x-value to look up
    :param x_axis: x-axis of lookup table
    :param y_axis: y-value of lookup table

    :return: float, y-value corresponding to x-value input
    """
    if x <= x_axis[0]:
        return y_axis[0]
    if x >= x_axis[-1]:
        return y_axis[-1]

    i = bisect_left(x_axis, x)
    k = (x - x_axis[i - 1]) / (x_axis[i] - x_axis[i - 1])
    y = k * (y_axis[i] - y_axis[i - 1]) + y_axis[i - 1]
    return y


def _decomp_float(float_in, return_val="pos", dec_places=0):
    """
    Static internal method:
    Decompose float into positive and negative components. Returns one of the two components.

    :param float_in: float to be decomposed.
    :param return_val: if "pos" return positive component. else if "neg" return negative component.
    :param dec_places: number of decimal places to round the return value to

    :return: positive or negative component of the input value
    """

    if float_in >= 0:
        pos_comp = round(float_in, dec_places)
        neg_comp = 0
    else:
        pos_comp = 0
        neg_comp = round(float_in, dec_places)
    if return_val == "pos":
        return abs(pos_comp)
    else:
        return abs(neg_comp)
