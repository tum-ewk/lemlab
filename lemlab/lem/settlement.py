__author__ = "sdlumpp"
__credits__ = ["michelzade"]
__license__ = ""
__maintainer__ = "sdlumpp"
__email__ = "sebastian.lumpp@tum.de"

import json
from bisect import bisect_left
import pandas as pd
import feather as ft
import numpy as np

""" This module contains the functions required to settle the local energy market.
    Settlement means all of the market functionality that takes place after the physical delivery of electricity
    has taken place. This includes:
    1 - validation of and post-processing of metering data
    2 - determining balancing energy (deviation from ex-ante market results)
    3 - calculating ex-post market prices and labelling (community markets that don't rely on ex-ante trading)
    4 - calculating settlement prices (balancing prices and levies) either in advance or ex-post
    5 - calculating the value of and logging transactions based on all of the above"""


def update_complete_meter_readings(db_obj):
    """
    Check for which ts_delivery ALL meter readings have been logged and calculate energy deltas for each meter.
    Label processed steps in status_settlement
    :param db_obj: instance of DatabaseConnection

    :return None:
    """
    # return list of all timesteps, extract unprocessed steps
    df_clearing_log = db_obj.get_status_settlement()
    list_t_d_meter_readings_incomplete = \
        list(df_clearing_log.loc[df_clearing_log["status_meter_readings_processed"] == 0].ts_delivery)

    for ts_delivery in list_t_d_meter_readings_incomplete:
        # get a list of meters active at the ts_delivery under consideration
        list_meters = sorted(db_obj.get_list_all_meters(ts_delivery_active=ts_delivery))
        # get list of meters that logged a meter reading
        # immediately BEFORE AND AFTER the ts_delivery under consideration
        list_meters_logged = sorted(_get_list_meters_logged(db_obj, ts_delivery))
        # proceed only if all meters have logged values
        # get cumulative meter readings, before and after
        df_metering_logs_cumulative = db_obj.get_meter_readings_cumulative(
            t_reading_first=ts_delivery,
            t_reading_last=ts_delivery + 900)

        if set(list_meters).issubset(set(list_meters_logged)):
            df_metering_logs_cumulative_prev = \
                df_metering_logs_cumulative[df_metering_logs_cumulative["t_reading"] == ts_delivery
                                            ].set_index(db_obj.db_param.ID_METER)
            df_metering_logs_cumulative_now = \
                df_metering_logs_cumulative[df_metering_logs_cumulative["t_reading"] == ts_delivery + 900
                                            ].set_index(db_obj.db_param.ID_METER)
            # calculate energy delta, log deltas to database
            df_meter_reading_delta = df_metering_logs_cumulative_now - df_metering_logs_cumulative_prev
            df_meter_reading_delta["t_reading"] = ts_delivery
            df_meter_reading_delta = df_meter_reading_delta.rename(columns={
                db_obj.db_param.T_READING: db_obj.db_param.TS_DELIVERY,
                db_obj.db_param.ENERGY_IN_CUM: db_obj.db_param.ENERGY_IN,
                db_obj.db_param.ENERGY_OUT_CUM: db_obj.db_param.ENERGY_OUT}).reset_index()
            db_obj.log_readings_meter_delta(df_meter_reading_delta)

            # label timestep as processed
            db_obj.set_status_settlement(pd.DataFrame().from_dict({
                db_obj.db_param.TS_DELIVERY: [ts_delivery],
                db_obj.db_param.STATUS_METER_READINGS_PROCESSED: [1],
                db_obj.db_param.STATUS_SETTLEMENT_COMPLETE: [0]
            }))
            calculate_virtual_submeters(db_obj=db_obj, list_ts_delivery=[ts_delivery])
        else:
            df_metering_logs_cumulative_prev = \
                df_metering_logs_cumulative[df_metering_logs_cumulative["t_reading"] == ts_delivery
                                            ].set_index(db_obj.db_param.ID_METER)
            df_metering_logs_cumulative_now = \
                df_metering_logs_cumulative[df_metering_logs_cumulative["t_reading"] == ts_delivery + 900
                                            ].set_index(db_obj.db_param.ID_METER)

            ix_prev = df_metering_logs_cumulative_prev.index
            ix_now = df_metering_logs_cumulative_now.index
            ix_intersection = ix_prev.intersection(ix_now)

            df_metering_logs_cumulative_prev = df_metering_logs_cumulative_prev.loc[ix_intersection]
            df_metering_logs_cumulative_now = df_metering_logs_cumulative_now.loc[ix_intersection]

            # calculate energy delta, log deltas to database
            if len(ix_intersection):
                df_meter_reading_delta = df_metering_logs_cumulative_now - df_metering_logs_cumulative_prev
                df_meter_reading_delta["t_reading"] = ts_delivery
                df_meter_reading_delta = df_meter_reading_delta.rename(columns={
                    db_obj.db_param.T_READING: db_obj.db_param.TS_DELIVERY,
                    db_obj.db_param.ENERGY_IN_CUM: db_obj.db_param.ENERGY_IN,
                    db_obj.db_param.ENERGY_OUT_CUM: db_obj.db_param.ENERGY_OUT}).reset_index()
                db_obj.log_readings_meter_delta(df_meter_reading_delta)
                calculate_virtual_submeters(db_obj=db_obj, list_ts_delivery=[ts_delivery])


def calculate_virtual_submeters(db_obj, list_ts_delivery):
    """
    In some simulations, some plant have no physical meters. Their power flow must be implicitly calculated and
    assigned to a virtual meter.
    :param db_obj: instance of DatabaseConnection
    :param list_ts_delivery: list of integers, unix timestamps of ts_deliveries to be processed

    :return None:
    """
    # get list of meter readings
    df_readings_meter_delta = db_obj.get_meter_readings_delta(ts_delivery_first=list_ts_delivery[0],
                                                              ts_delivery_last=list_ts_delivery[-1])
    list_readings_meter_delta = []
    for ts_delivery in list_ts_delivery:
        # loop through all ts deliveries
        # get list of all meters currently active
        df_info_meter = db_obj.get_info_meter(ts_delivery_active=ts_delivery)
        # extract the list of virtual meters
        list_virtual_meters = list(df_info_meter[df_info_meter["type_meter"].str.contains("virtual")]["id_meter"])
        # init list of VM readings to be logged
        # for all virtual meters under consideration
        for virtual_meter in list_virtual_meters:
            # get the associated supermeter
            supermeter = df_info_meter.set_index("id_meter").loc[virtual_meter, "id_meter_super"]
            # if the VM is not a grid meter (top level meter)
            if supermeter != "0000000000":
                # return list of associated submeters
                df_submeters = df_info_meter[(df_info_meter["id_meter_super"] == supermeter)
                                             & (df_info_meter["id_meter"] != virtual_meter)]
                list_submeters = list(df_submeters["id_meter"])
                set_readings = set(df_readings_meter_delta[(df_readings_meter_delta["id_meter"].isin(list_submeters))
                                                           & (df_readings_meter_delta["ts_delivery"] == ts_delivery)
                                                           ]["id_meter"])
                mm_reading = df_readings_meter_delta[(df_readings_meter_delta["id_meter"] == supermeter) &
                                                     (df_readings_meter_delta["ts_delivery"] == ts_delivery)]
                if set(list_submeters).issubset(set_readings) and len(mm_reading):
                    # determine "missing" energy
                    # "missing" energy is attributed to the VM
                    cum_energy = 0
                    for meter in list(set_readings):
                        cum_temp = df_readings_meter_delta[(df_readings_meter_delta["id_meter"] == meter) &
                                                           (df_readings_meter_delta["ts_delivery"] == ts_delivery)]
                        cum_energy += int(cum_temp["energy_out"]) - int(cum_temp["energy_in"])

                    mm_energy = int(mm_reading["energy_out"]) - int(mm_reading["energy_in"])
                    vm_energy = mm_energy - cum_energy
                    # append result to list of VM readings to be logged
                    list_readings_meter_delta.append([ts_delivery,
                                                      _decomp_float(vm_energy, "neg"),
                                                      _decomp_float(vm_energy, "pos"),
                                                      virtual_meter])
            # if the VM is a supermeter
            else:
                # sum the flows of all submeters to find VM (supermeter) flow
                supermeter = virtual_meter
                df_submeters = df_info_meter[df_info_meter["id_meter_super"] == supermeter]
                list_submeters = list(df_submeters["id_meter"])
                set_readings = set(df_readings_meter_delta[(df_readings_meter_delta["id_meter"].isin(list_submeters))
                                                           & (df_readings_meter_delta["ts_delivery"] == ts_delivery)
                                                           ]["id_meter"])
                if set(list_submeters).issubset(set_readings) and len(set_readings):
                    cum_energy = 0
                    # determine missing energy flow
                    for meter in list(set_readings):
                        cum_temp = df_readings_meter_delta[(df_readings_meter_delta["id_meter"] == meter) &
                                                           (df_readings_meter_delta["ts_delivery"] == ts_delivery)]
                        cum_energy += int(cum_temp["energy_out"]) - int(cum_temp["energy_in"])
                    vm_energy = cum_energy
                    # append result to list of VM readings to be logged
                    list_readings_meter_delta.append([ts_delivery,
                                                      _decomp_float(vm_energy, "neg"),
                                                      _decomp_float(vm_energy, "pos"),
                                                      virtual_meter])
        # end VM for loop
    # end ts_d for loop

    # log virtual meter deltas to database
    if len(list_readings_meter_delta):
        df_meter_reading_delta = pd.DataFrame(list_readings_meter_delta,
                                              columns=[db_obj.db_param.TS_DELIVERY, db_obj.db_param.ENERGY_IN,
                                                       db_obj.db_param.ENERGY_OUT, db_obj.db_param.ID_METER])
        db_obj.log_readings_meter_delta(df_meter_reading_delta)


def determine_balancing_energy(db_obj, list_ts_delivery):
    """
    Calculate balancing energy used by each main meter.
    Balancing energy is the deviation from the ex-ante market result during ts_delivery
    :param db_obj: instance of DatabaseConnection
    :param list_ts_delivery: list of integers, unix timestamps of ts_deliveries to be processed

    :return None:
    """
    # get mapping of market agent IDs to main meter
    map_id_ma_to_main_meter = db_obj.get_map_to_main_meter()

    dict_bal_ener = {
        db_obj.db_param.ID_METER: [],
        db_obj.db_param.TS_DELIVERY: [],
        db_obj.db_param.ENERGY_BALANCING_POSITIVE: [],
        db_obj.db_param.ENERGY_BALANCING_NEGATIVE: []
    }
    list_ts_delivery = sorted(list_ts_delivery)
    ts_d_first = list_ts_delivery[0] if len(list_ts_delivery) else 0
    ts_d_last = list_ts_delivery[-1] if len(list_ts_delivery) else 0
    market_results_all, _, = db_obj.get_results_market_ex_ante(ts_delivery_first=ts_d_first,
                                                               ts_delivery_last=ts_d_last)
    for ts_d in list_ts_delivery:
        # return MAIN meter reading deltas and ex-ante market results
        main_meter_readings_delta = db_obj.get_meter_readings_by_type(ts_delivery=ts_d, types_meters=[4, 5])
        main_meter_readings_delta["energy_net"] = main_meter_readings_delta[db_obj.db_param.ENERGY_OUT] \
            - main_meter_readings_delta[db_obj.db_param.ENERGY_IN]

        market_results = market_results_all[market_results_all[db_obj.db_param.TS_DELIVERY] == ts_d]
        # relabel market results by main meters, so comparison to energy flows can be made
        market_results = market_results.replace({db_obj.db_param.ID_USER_BID: map_id_ma_to_main_meter,
                                                 db_obj.db_param.ID_USER_OFFER: map_id_ma_to_main_meter})
        # determine balancing energy per meter
        for _, entry in main_meter_readings_delta.iterrows():
            current_market_energy = 0
            current_market_energy -= \
                market_results[market_results[db_obj.db_param.ID_USER_BID] == entry.loc[db_obj.db_param.ID_METER]
                               ][db_obj.db_param.QTY_ENERGY_TRADED].sum()
            current_market_energy += \
                market_results[market_results[db_obj.db_param.ID_USER_OFFER] == entry.loc[db_obj.db_param.ID_METER]
                               ][db_obj.db_param.QTY_ENERGY_TRADED].sum()

            current_balancing_energy = -1 * (current_market_energy - entry.loc["energy_net"])
            # append result to dict
            dict_bal_ener[db_obj.db_param.ID_METER].append(entry.loc[db_obj.db_param.ID_METER])
            dict_bal_ener[db_obj.db_param.TS_DELIVERY].append(ts_d)
            dict_bal_ener[db_obj.db_param.ENERGY_BALANCING_POSITIVE].append(
                _decomp_float(float_in=current_balancing_energy, return_val="pos"))
            dict_bal_ener[db_obj.db_param.ENERGY_BALANCING_NEGATIVE].append(
                _decomp_float(float_in=current_balancing_energy, return_val="neg"))
    # if any values calculated, post to database
    if len(dict_bal_ener[db_obj.db_param.ID_METER]):
        db_obj.log_energy_balancing(pd.DataFrame().from_dict(dict_bal_ener))


def update_balance_balancing_costs(db_obj, t_now, lem_config, list_ts_delivery, id_retailer="retailer01"):
    """
    Determine balancing energy credits and debits and add transactions to database.

    :param db_obj: instance of DatabaseConnection
    :param t_now: integer, unix timestamp current time
    :param lem_config: dictionary containing configuration of LEM
    :param list_ts_delivery: list of integers, unix timestamps of ts_deliveries to be processed
    :param id_retailer: string, retailer id, number, as retailer needs to be credited/debited

    :return None:

    """
    # get mapping from meters to users
    dict_map_to_user = db_obj.get_mapping_to_user()
    # construct transaction dict including dynamic quality columns
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
        # return relevant settlement prices
        settlement_prices = db_obj.get_prices_settlement(ts_delivery_first=ts_d)
        pos_bal_ener_price = int(settlement_prices[db_obj.db_param.PRICE_ENERGY_BALANCING_POSITIVE])
        neg_bal_ener_price = int(settlement_prices[db_obj.db_param.PRICE_ENERGY_BALANCING_NEGATIVE])
        # return balancing energies
        balancing_energies = db_obj.get_energy_balancing(ts_delivery=ts_d)

        # repeat calculation for each balancing energy recorded
        for _, entry in balancing_energies.iterrows():
            if entry.loc[db_obj.db_param.ENERGY_BALANCING_POSITIVE] != 0:
                transaction_value = entry.loc[db_obj.db_param.ENERGY_BALANCING_POSITIVE] * pos_bal_ener_price

                # credit retailer
                dict_transactions[db_obj.db_param.ID_USER].append(id_retailer)
                dict_transactions[db_obj.db_param.TS_DELIVERY].append(ts_d)
                dict_transactions[db_obj.db_param.PRICE_ENERGY_MARKET].append(pos_bal_ener_price)
                dict_transactions[db_obj.db_param.TYPE_TRANSACTION].append("balancing")
                dict_transactions[db_obj.db_param.QTY_ENERGY].append(
                    -1 * entry.loc[db_obj.db_param.ENERGY_BALANCING_POSITIVE])
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
                # credit retailer
                dict_transactions[db_obj.db_param.ID_USER].append(id_retailer)
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
                    -1 * entry.loc[db_obj.db_param.ENERGY_BALANCING_NEGATIVE])
                dict_transactions[db_obj.db_param.DELTA_BALANCE].append(
                    -1 * transaction_value)
                dict_transactions[db_obj.db_param.T_UPDATE_BALANCE].append(t_now)
                for quality in lem_config["types_quality"]:
                    dict_transactions[db_obj.db_param.SHARE_QUALITY_ + lem_config["types_quality"][quality]].append(0)

    # if any balancing energy transactions recorded, post to DB
    if len(dict_transactions[db_obj.db_param.ID_USER]):
        db_obj.log_transactions(pd.DataFrame.from_dict(dict_transactions))
        db_obj.update_balance_user(pd.DataFrame.from_dict(dict_transactions))


def set_prices_settlement(db_obj, path_simulation, list_ts_delivery):
    """
    Determine levy energy debit and credit and add transactions to database.

    :param db_obj: instance of DatabaseConnection
    :param path_simulation: string, path to simulation_results folder
    :param list_ts_delivery: list of integers, unix timestamps of ts_deliveries to be processed

    :return None:
    """
    # load lem config file
    with open(f"{path_simulation}/lem/config_account.json", "r") as read_file:
        config_dict = json.load(read_file)
    # conversion factor from off chain to on chain currency
    euro_kwh_to_sigma_wh = db_obj.db_param.EURO_TO_SIGMA / 1000

    for ts_delivery in list_ts_delivery:
        # default, set settlement prices to fixed value in config
        price_bal_pos = config_dict["price_energy_balancing_positive"] * euro_kwh_to_sigma_wh
        price_bal_neg = config_dict["price_energy_balancing_negative"] * euro_kwh_to_sigma_wh
        price_levies_pos = config_dict["price_energy_levies_positive"] * euro_kwh_to_sigma_wh
        price_levies_neg = config_dict["price_energy_levies_negative"] * euro_kwh_to_sigma_wh
        # if config requires settlement prices to be loaded from a file, override default
        if config_dict["bal_energy_pricing_mechanism"] == "file":
            df_bal_prices = ft.read_dataframe(f"{path_simulation}/lem/balancing_prices.ft"
                                              ).set_index("timestamp")
            price_bal_pos = df_bal_prices.loc[ts_delivery, "price_balancing_energy_positive"] * euro_kwh_to_sigma_wh
            price_bal_neg = df_bal_prices.loc[ts_delivery, "price_balancing_energy_negative"] * euro_kwh_to_sigma_wh
        if config_dict["levy_pricing_mechanism"] == "file":
            df_levy_prices = ft.read_dataframe(f"{path_simulation}/lem/levy_prices.ft"
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
        # log settlement prices to the DB
        db_obj.set_prices_settlement(pd.DataFrame().from_dict(dict_settlement_prices))


def update_balance_levies(db_obj, t_now, lem_config, list_ts_delivery, id_retailer="retailer01"):
    """
    Determine levy energy debit and credit and add transactions to database.

    :param db_obj: instance of DatabaseConnection
    :param t_now: integer, unix timestamp current time
    :param lem_config: dictionary containing configuration of LEM
    :param list_ts_delivery: list of integers, unix timestamps of ts_deliveries to be processed
    :param id_retailer: string, retailer id, number, as retailer needs to be credited/debited

    :return None:
    """
    # get mapping from meter to user
    dict_map_to_user = db_obj.get_mapping_to_user()
    # construct transaction dict including dynamic quality columns
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
        # get meter readings and levy prices
        meter_readings_delta = db_obj.get_meter_readings_by_type(ts_delivery=ts_d, types_meters=[4, 5])
        settlement_prices = db_obj.get_prices_settlement(ts_delivery_first=ts_d)
        levies_pos = int(settlement_prices[db_obj.db_param.PRICE_ENERGY_LEVIES_POSITIVE])
        levies_neg = int(settlement_prices[db_obj.db_param.PRICE_ENERGY_LEVIES_NEGATIVE])
        # for each main meter reading, construct transaction
        for _, entry in meter_readings_delta.iterrows():
            if entry.loc[db_obj.db_param.ENERGY_OUT] != 0 and levies_pos != 0:
                transaction_value = entry.loc[db_obj.db_param.ENERGY_OUT] * levies_pos
                # credit retailer
                dict_transactions[db_obj.db_param.ID_USER].append(id_retailer)
                dict_transactions[db_obj.db_param.TS_DELIVERY].append(ts_d)
                dict_transactions[db_obj.db_param.PRICE_ENERGY_MARKET].append(levies_pos)
                dict_transactions[db_obj.db_param.TYPE_TRANSACTION].append("levies")
                dict_transactions[db_obj.db_param.QTY_ENERGY].append(- 1 * entry.loc[db_obj.db_param.ENERGY_OUT])
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
                # credit retailer
                dict_transactions[db_obj.db_param.ID_USER].append(id_retailer)
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
                dict_transactions[db_obj.db_param.QTY_ENERGY].append(-1 * entry.loc[db_obj.db_param.ENERGY_IN])
                dict_transactions[db_obj.db_param.DELTA_BALANCE].append(-1 * transaction_value)
                dict_transactions[db_obj.db_param.T_UPDATE_BALANCE].append(t_now)
                for quality in lem_config["types_quality"]:
                    dict_transactions[db_obj.db_param.SHARE_QUALITY_ + lem_config["types_quality"][quality]].append(0)
    # post transactions, if any
    if len(dict_transactions[db_obj.db_param.ID_USER]):
        db_obj.log_transactions(pd.DataFrame.from_dict(dict_transactions))
        db_obj.update_balance_user(pd.DataFrame.from_dict(dict_transactions))


def determine_prices_ex_post_markets(db_obj, path_simulation, lem_config, list_ts_delivery):
    """
    Determine prices and quality labelling of all ex-post markets

    :param db_obj: instance of DatabaseConnection
    :param path_simulation: string, path to simulation_results folder
    :param lem_config: dictionary containing configuration of LEM
    :param list_ts_delivery: list of integers, unix timestamps of ts_deliveries to be processed

    :return None:
    """
    # currently only community ex-post markets implemented
    for type_clearing in lem_config["types_clearing_ex_post"]:
        if lem_config["types_clearing_ex_post"][type_clearing] == "community":
            set_community_price(db_obj=db_obj, path_simulation=path_simulation,
                                lem_config=lem_config, list_ts_delivery=list_ts_delivery)


def set_community_price(db_obj, path_simulation, lem_config, list_ts_delivery):
    """
    Determine and log community pricing for each pricing type

    :param db_obj: instance of DatabaseConnection
    :param path_simulation: string, path to simulation_results folder
    :param lem_config: dictionary containing configuration of LEM
    :param list_ts_delivery: list of integers, unix timestamps of ts_deliveries to be processed

    :return None:
    """
    # get currency conversion factor
    euro_kwh_to_sigma_wh = db_obj.db_param.EURO_TO_SIGMA / 1000

    # set up result dictionary format and load pricing curves as lookup tables
    dict_results_ex_post = {db_obj.db_param.TS_DELIVERY: []}
    dict_lookup_tables = {}

    for type_pricing in lem_config["types_pricing_ex_post"]:
        with open(f"{path_simulation}/lem/{lem_config['types_pricing_ex_post'][type_pricing]}.json",
                  "r") as read_file:
            dict_lookup_tables[lem_config['types_pricing_ex_post'][type_pricing]] = json.load(read_file)
        dict_results_ex_post.update({db_obj.db_param.PRICE_ENERGY_MARKET_
                                     + lem_config['types_pricing_ex_post'][type_pricing]: []})

    for quality in lem_config["types_quality"]:
        dict_results_ex_post.update({db_obj.db_param.SHARE_QUALITY_ + lem_config["types_quality"][quality]: []})

    # get required mappings
    info_meter = db_obj.get_info_meter()
    map_submeter_to_main = dict([(i, a) for i, a in zip(info_meter["id_meter"], info_meter["id_meter_super"])])
    map_quality = db_obj.get_map_meter_to_quality()

    for ts_d in list_ts_delivery:
        # return meter energy flows
        dict_results_ex_post[db_obj.db_param.TS_DELIVERY].append(ts_d)

        main_meter_flows = db_obj.get_meter_readings_by_type(ts_delivery=ts_d,
                                                             types_meters=[4, 5])
        submeter_flows = db_obj.get_meter_readings_by_type(ts_delivery=ts_d,
                                                           types_meters=[0, 1])
        # determine energy exchange across market boundaries
        df_outside_flow = main_meter_flows.groupby("ts_delivery").sum()
        if len(main_meter_flows):
            outside_flow = df_outside_flow.iloc[0]["energy_in"] - df_outside_flow.iloc[0]["energy_out"]
            outside_flow = max(outside_flow, 0)
        else:
            outside_flow = 0
        ###
        # Calculate community exchange qualities
        ###
        # add column containing meter qualities
        submeter_flows["quality"] = submeter_flows["id_meter"]
        submeter_flows = submeter_flows.replace({"quality": map_quality})
        # split up produced energy by quality
        for quality in lem_config["types_quality"]:
            submeter_flows.loc[
                submeter_flows["quality"] == lem_config["types_quality"][quality],
                db_obj.db_param.SHARE_QUALITY_ + lem_config["types_quality"][quality]] =\
                submeter_flows["energy_out"]

        # add column containing main meter ids
        submeter_flows["id_meter_main"] = submeter_flows["id_meter"]
        submeter_flows = submeter_flows.replace({"id_meter_main": map_submeter_to_main})
        # group by timestep and main meter
        submeter_flows = submeter_flows.groupby("id_meter_main").sum()
        # add main meter flows out
        submeter_flows["energy_out_main_meter"] = submeter_flows.index
        map_meter_main_to_energy_out = dict([(i, a) for i, a in zip(main_meter_flows[db_obj.db_param.ID_METER],
                                                                    main_meter_flows[db_obj.db_param.ENERGY_OUT])])
        submeter_flows = submeter_flows.replace({"energy_out_main_meter": map_meter_main_to_energy_out})

        # make quality flows percentages
        for quality in lem_config["types_quality"]:
            submeter_flows[db_obj.db_param.SHARE_QUALITY_ + lem_config["types_quality"][quality]] = \
                submeter_flows[db_obj.db_param.SHARE_QUALITY_ + lem_config["types_quality"][quality]].\
                div((submeter_flows["energy_out"])).replace(np.inf, 0)*submeter_flows["energy_out_main_meter"]

        final_qualities = submeter_flows.fillna(0).copy()
        final_qualities["temp"] = 1
        final_qualities = final_qualities.groupby("temp").sum()

        if len(final_qualities):
            final_qualities.loc[1, "share_quality_na"] += outside_flow
            final_qualities.loc[1, "energy_out_main_meter"] += outside_flow

        for quality in lem_config["types_quality"]:
            q = db_obj.db_param.SHARE_QUALITY_ + lem_config["types_quality"][quality]
            if len(final_qualities):
                total_e_out = final_qualities.loc[1, "energy_out_main_meter"] \
                    if final_qualities.loc[1, "energy_out_main_meter"] != 0 else 0.1
                quality = final_qualities.loc[1, q] / total_e_out * 100
            else:
                quality = 0
            dict_results_ex_post[q].append(quality)

        ###
        # Determine community price
        ###
        # initialize feed-in and consumption
        for type_pricing in lem_config["types_pricing_ex_post"]:
            lookup_supply_ratio = \
                dict_lookup_tables[lem_config['types_pricing_ex_post'][type_pricing]]["supply_demand_ratio"]
            lookup_price = \
                dict_lookup_tables[lem_config['types_pricing_ex_post'][type_pricing]]["price"]

            local_share = 1 - dict_results_ex_post["share_quality_na"][-1]/100    # share of all non-local ("NA")
            price = _lookup(local_share, lookup_supply_ratio, lookup_price) * euro_kwh_to_sigma_wh
            dict_results_ex_post[db_obj.db_param.PRICE_ENERGY_MARKET_
                                 + lem_config['types_pricing_ex_post'][type_pricing]].append(price)
    if len(list_ts_delivery) and len(dict_results_ex_post[db_obj.db_param.TS_DELIVERY]):
        db_obj.log_results_market_ex_post(pd.DataFrame(dict_results_ex_post))


def update_balance_ex_post(db_obj, id_retailer, t_now, list_ts_delivery, lem_config):
    """
    Update balance based on energy flows and ex-post prices. Only executed if ex-post is the main market to be settled.

    :param db_obj: instance of DatabaseConnection
    :param t_now: integer, unix timestamp current time
    :param lem_config: dictionary containing configuration of LEM
    :param list_ts_delivery: list of integers, unix timestamps of ts_deliveries to be processed
    :param id_retailer: string, retailer id, number, as retailer needs to be credited/debited

    :return None:
    """
    # get mapping from meter to user
    dict_map_to_user = db_obj.get_mapping_to_user()

    # construct transaction dict including dynamic quality columns
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
        # get energy flows and ex-post prices
        meter_readings_delta = db_obj.get_meter_readings_by_type(ts_delivery=ts_d, types_meters=[4, 5])
        ex_post_prices = db_obj.get_results_market_ex_post(ts_delivery_first=ts_d)
        price = int(ex_post_prices[db_obj.db_param.PRICE_ENERGY_MARKET_
                                   + lem_config['types_pricing_ex_post'][0]])
        # get qualities of ex-post result for this ts_delivery
        shares_quality = {}
        for quality in lem_config["types_quality"]:
            shares_quality[lem_config["types_quality"][quality]] =\
                ex_post_prices[db_obj.db_param.SHARE_QUALITY_ + lem_config["types_quality"][quality]][0]
        # for each main meter reading, update balance
        for _, entry in meter_readings_delta.iterrows():
            if entry.loc[db_obj.db_param.ENERGY_OUT] != 0:
                transaction_value = entry.loc[db_obj.db_param.ENERGY_OUT] * price
                # credit retailer
                dict_transactions[db_obj.db_param.ID_USER].append(id_retailer)
                dict_transactions[db_obj.db_param.TS_DELIVERY].append(ts_d)
                dict_transactions[db_obj.db_param.PRICE_ENERGY_MARKET].append(price)
                dict_transactions[db_obj.db_param.TYPE_TRANSACTION].append("market")
                dict_transactions[db_obj.db_param.QTY_ENERGY].append(-1 * entry.loc[db_obj.db_param.ENERGY_OUT])
                dict_transactions[db_obj.db_param.DELTA_BALANCE].append(-1 * transaction_value)
                dict_transactions[db_obj.db_param.T_UPDATE_BALANCE].append(t_now)
                for quality in lem_config["types_quality"]:
                    dict_transactions[db_obj.db_param.SHARE_QUALITY_
                                      + lem_config["types_quality"][quality]
                                      ].append(shares_quality[lem_config["types_quality"][quality]])

                # debit consumer
                dict_transactions[db_obj.db_param.ID_USER].append(dict_map_to_user[entry.loc[db_obj.db_param.ID_METER]])
                dict_transactions[db_obj.db_param.TS_DELIVERY].append(ts_d)
                dict_transactions[db_obj.db_param.PRICE_ENERGY_MARKET].append(price)
                dict_transactions[db_obj.db_param.TYPE_TRANSACTION].append("market")
                dict_transactions[db_obj.db_param.QTY_ENERGY].append(entry.loc[db_obj.db_param.ENERGY_OUT])
                dict_transactions[db_obj.db_param.DELTA_BALANCE].append(transaction_value)
                dict_transactions[db_obj.db_param.T_UPDATE_BALANCE].append(t_now)
                for quality in lem_config["types_quality"]:
                    dict_transactions[db_obj.db_param.SHARE_QUALITY_
                                      + lem_config["types_quality"][quality]
                                      ].append(shares_quality[lem_config["types_quality"][quality]])

            elif int(entry.loc[db_obj.db_param.ENERGY_IN]) != 0:
                transaction_value = entry.loc[db_obj.db_param.ENERGY_IN] * price
                # credit retailer
                dict_transactions[db_obj.db_param.ID_USER].append(id_retailer)
                dict_transactions[db_obj.db_param.TS_DELIVERY].append(ts_d)
                dict_transactions[db_obj.db_param.PRICE_ENERGY_MARKET].append(price)
                dict_transactions[db_obj.db_param.TYPE_TRANSACTION].append("market")
                dict_transactions[db_obj.db_param.QTY_ENERGY].append(entry.loc[db_obj.db_param.ENERGY_IN])
                dict_transactions[db_obj.db_param.DELTA_BALANCE].append(transaction_value)
                dict_transactions[db_obj.db_param.T_UPDATE_BALANCE].append(t_now)
                for quality in lem_config["types_quality"]:
                    dict_transactions[db_obj.db_param.SHARE_QUALITY_
                                      + lem_config["types_quality"][quality]
                                      ].append(shares_quality[lem_config["types_quality"][quality]])

                # debit consumer
                dict_transactions[db_obj.db_param.ID_USER].append(dict_map_to_user[entry.loc[db_obj.db_param.ID_METER]])
                dict_transactions[db_obj.db_param.TS_DELIVERY].append(ts_d)
                dict_transactions[db_obj.db_param.PRICE_ENERGY_MARKET].append(price)
                dict_transactions[db_obj.db_param.TYPE_TRANSACTION].append("market")
                dict_transactions[db_obj.db_param.QTY_ENERGY].append(-1 * entry.loc[db_obj.db_param.ENERGY_IN])
                dict_transactions[db_obj.db_param.DELTA_BALANCE].append(-1 * transaction_value)
                dict_transactions[db_obj.db_param.T_UPDATE_BALANCE].append(t_now)
                for quality in lem_config["types_quality"]:
                    dict_transactions[db_obj.db_param.SHARE_QUALITY_
                                      + lem_config["types_quality"][quality]
                                      ].append(shares_quality[lem_config["types_quality"][quality]])
    # if any recorded, log transactions to database
    if len(dict_transactions[db_obj.db_param.ID_USER]):
        db_obj.log_transactions(pd.DataFrame.from_dict(dict_transactions))
        db_obj.update_balance_user(pd.DataFrame.from_dict(dict_transactions))


def get_list_ts_delivery_ready(db_obj):
    """
    Returns list of timesteps ready for settlement.

    :param db_obj: instance of DatabaseConnection

    :return None:
    """
    df_clearing_log = db_obj.get_status_settlement()
    list_ts_delivery_ready = \
        list(df_clearing_log.loc[(df_clearing_log[db_obj.db_param.STATUS_METER_READINGS_PROCESSED] == 1)
                                 & (df_clearing_log[db_obj.db_param.STATUS_SETTLEMENT_COMPLETE] == 0)
                                 ].ts_delivery)
    return list_ts_delivery_ready

########################################################################################################################
# Internal methods and functions
########################################################################################################################


def _get_list_meters_logged(db_obj, ts_delivery):
    """
    Get list of meters that logged a meter reading at the BEGINNING AND END of the ts_delivery being examined

    :param db_obj: instance of DatabaseConnection
    :param ts_delivery: integer, unix timestamp of timestep of delivery to be returned

    :return None:
    """
    df_meter_readings_cum = \
        db_obj.get_meter_readings_cumulative(t_reading_first=ts_delivery,
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


if __name__ == "__main__":
    from ruamel.yaml import YAML
    from lemlab.db_connection.db_connection import DatabaseConnection
    with open(f"./scenario_config.yaml") as config_file:
        config_example = YAML().load(config_file)
    # Create a db connection object
    db_obj_feldtest = DatabaseConnection(
        db_dict=config_example['db_connections']['database_connection_admin'],
        lem_config=config_example['lem'])
    determine_prices_ex_post_markets(
        db_obj_feldtest,
        path_simulation="C:/Users/ga59zah/PycharmProjects/lemlab/simulation_results/test_sim",
        lem_config=config_example["lem"],
        list_ts_delivery=[1623919500 + 5 * 900,
                          1623919500 + 6 * 900,
                          1623919500 + 7 * 900,
                          1623919500 + 8 * 900
                          ])
