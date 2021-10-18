import os
import time
import yaml
import pandas as pd
import random

from lemlab.db_connection import db_connection, db_param
from lemlab.platform import lem
from lemlab.bc_connection.bc_connection import BlockchainConnection
from lemlab.platform import lem_settlement
from current_scenario_file import scenario_file_path


def setup_test_general(generate_random_test_data=False):
    yaml_file = scenario_file_path

    # load configuration file
    with open(f"" + yaml_file) as config_file:
        config = yaml.load(config_file, Loader=yaml.FullLoader)

    # Create a db connection object
    db_obj = db_connection.DatabaseConnection(db_dict=config['db_connections']['database_connection_admin'],
                                              lem_config=config['lem'])
    # Create bc connection objects to ClearingExAnte and Settlement contract
    market_contract_dict = config['db_connections']['bc_dict']
    market_contract_dict["contract_name"] = "ClearingExAnte"
    bc_obj_clearing_ex_ante = BlockchainConnection(bc_dict=market_contract_dict)
    settlement_contract_dict = config['db_connections']['bc_dict']
    settlement_contract_dict["contract_name"] = "Settlement"
    bc_obj_settlement = BlockchainConnection(settlement_contract_dict)

    if generate_random_test_data:
        init_random_data(db_obj=db_obj, bc_obj_market=bc_obj_clearing_ex_ante,
                         config=config, bc_obj_settlement=bc_obj_settlement)

    return config, db_obj, bc_obj_clearing_ex_ante, bc_obj_settlement


def init_random_data(db_obj, bc_obj_market, config, bc_obj_settlement):
    # Clear data from db and bc
    db_obj.init_db(clear_tables=True, reformat_tables=True)
    bc_obj_market.clear_temp_data()
    bc_obj_market.clear_permanent_data()
    bc_obj_settlement.clear_data()

    # Create list of random user ids and meter ids
    ids_users_random = lem.create_user_ids(num=config['prosumer']['general_number_of'])
    ids_meter_random = lem.create_user_ids(num=config['prosumer']['general_number_of'])
    ids_market_agents = lem.create_user_ids(num=config['prosumer']['general_number_of'])

    tx_hash = None
    # Register meters and users on database
    for z in range(len(ids_users_random)):
        cols, types = db_obj.get_table_columns(db_obj.db_param.NAME_TABLE_INFO_USER, dtype=True)
        col_data = [ids_users_random[z], 1000, 0, 10000, 100, 'green', 10, 'zi', 0, ids_market_agents[z], 0, 2147483648]
        if any([type(data) != typ for data, typ in zip(col_data, types)]):
            raise TypeError("The types of the data and the columns do not match for the info_user")
        df_insert = pd.DataFrame(
            data=[col_data],
            columns=cols)

        # Register users on bc and db
        db_obj.register_user(df_in=df_insert)
        bc_obj_market.register_user(df_user=df_insert)

        cols, types = db_obj.get_table_columns(db_obj.db_param.NAME_TABLE_INFO_METER, dtype=True)
        col_data = [ids_meter_random[z], ids_users_random[z], "0", "virtual grid meter", '0'*10, 'green', 0, 2147483648,
                    'test']
        if any([type(data) != typ for data, typ in zip(col_data, types)]):
            raise TypeError("The types of data and columns do not match for the id_meter")
        df_insert = pd.DataFrame(
            data=[col_data],
            columns=cols)

        # Register meters on db and bc
        db_obj.register_meter(df_in=df_insert)
        tx_hash = bc_obj_market.register_meter(df_insert)

    bc_obj_market.wait_for_transact(tx_hash)

    # Compute random market positions
    positions = lem.create_random_positions(db_obj=db_obj,
                                            config=config,
                                            ids_user=ids_users_random,
                                            n_positions=500,
                                            verbose=False)
    # Post positions on db
    db_obj.post_positions(positions)
    # on the bc, energy quality needs to be converted to int. In the db it is stored as a string
    positions = lem._convert_qualities_to_int(db_obj, positions, config['lem']['types_quality'])
    bc_obj_market.push_all_positions(positions, temporary=True, permanent=False)


def setup_clearing_ex_ante_test(generate_random_test_data):
    config, db_obj, bc_obj_clearing_ex_ante, bc_obj_settlement = setup_test_general(generate_random_test_data)

    # Initialize clearing parameters
    config_supplier = None
    t_override = round(time.time())
    shuffle = False
    plotting = False
    verbose = False

    # Clear market ex ante on db and bc
    bc_obj_clearing_ex_ante.market_clearing_ex_ante(config["lem"], config_supplier=config_supplier,
                                                    t_override=t_override, shuffle=shuffle, verbose=verbose)
    lem.market_clearing(db_obj=db_obj, config_lem=config["lem"], config_supplier=config_supplier,
                        t_override=t_override, shuffle=shuffle, plotting=plotting, verbose=verbose)

    return config, db_obj, bc_obj_clearing_ex_ante, bc_obj_settlement


def setup_settlement_test(generate_random_test_data):
    config, db_obj, bc_obj_clearing_ex_ante, bc_obj_settlement = setup_clearing_ex_ante_test(generate_random_test_data)

    # Simulate meter readings from market results with random errors
    simulated_meter_readings_delta, ts_delivery_list = simulate_meter_readings_from_market_results(
        db_obj=db_obj, rand_percent_var=15)

    # Log meter readings delta
    bc_obj_settlement.log_meter_readings_delta(simulated_meter_readings_delta)
    db_obj.log_readings_meter_delta(simulated_meter_readings_delta)

    # Calculate/determine balancing energies
    bc_obj_settlement.determine_balancing_energy(ts_delivery_list)
    lem_settlement.determine_balancing_energy(db_obj, ts_delivery_list)

    sim_path = "C:/Users/ga47num/PycharmProjects/lemlab/scenarios/"
    files_path = "C:/Users/ga47num/PycharmProjects/lemlab/input_data/"

    # Set settlement prices in db and bc
    lem_settlement.set_prices_settlement(db_obj=db_obj, path_simulation=sim_path,
                                         files_path=files_path, list_ts_delivery=ts_delivery_list)
    bc_obj_settlement.set_prices_settlement(ts_delivery_list)

    # Update balances according to balancing energies db and bc
    ts_now = round(time.time())
    id_retailer = "retailer01"
    lem_settlement.update_balance_balancing_costs(db_obj=db_obj, t_now=ts_now,
                                                  list_ts_delivery=ts_delivery_list,
                                                  id_retailer=id_retailer, lem_config=config["lem"])
    bc_obj_settlement.update_balance_balancing_costs(list_ts_delivery=ts_delivery_list,
                                                     ts_now=ts_now, supplier_id=id_retailer)

    # Update balances with levies on db and bc
    lem_settlement.update_balance_levies(db_obj=db_obj, t_now=ts_now, list_ts_delivery=ts_delivery_list,
                                         id_retailer=id_retailer, lem_config=config["lem"])
    bc_obj_settlement.update_balance_levies(list_ts_delivery=ts_delivery_list, ts_now=ts_now, id_retailer=id_retailer)

    return config, db_obj, bc_obj_clearing_ex_ante, bc_obj_settlement


def simulate_meter_readings_from_market_results(db_obj=None, rand_percent_var=15):
    """
    Read market results from data base
    Aggregate users with meters for each timestep
    Randomly change energy traded
    Push output energy traded into meter_reading_deltas
    Returns: None
    -------
    random_variance: how much the energy delta is changed to create some energy balances, if 0, no changes
    are made at all
    """
    if db_obj is None:
        yaml_file = scenario_file_path
        # load configuration file
        with open(yaml_file) as config_file:
            config = yaml.load(config_file, Loader=yaml.FullLoader)
        # Create a db connection object
        db_obj = db_connection.DatabaseConnection(db_dict=config['db_connections']['database_connection_admin'],
                                                  lem_config=config['lem'])
        # Initialize database
        db_obj.init_db(clear_tables=False, reformat_tables=False)

    # for this to work, we need to have a full market cleared before, so execute the test if you havent
    market_results, _ = db_obj.get_results_market_ex_ante()
    assert not market_results.empty, "Error: The market results are empty"
    # retrieve list of users and initialize a mapping
    list_users_offers = list(set(market_results[db_obj.db_param.ID_USER_OFFER]))
    list_users_bids = list(set(market_results[db_obj.db_param.ID_USER_BID]))

    user_offers2ts_qty = dict([(user, {}) for user in list_users_offers])
    user_bids2ts_qty = dict([(user, {}) for user in list_users_bids])

    list_ts_delivery = []  # additionally we save all the timesteps registered
    # for each user we have a dictionary with each single timestep as key and the total energy traded in that
    # timestep as value
    for i, row in market_results.iterrows():
        # for the user offers
        if row[db_obj.db_param.TS_DELIVERY] in user_offers2ts_qty[row[db_obj.db_param.ID_USER_OFFER]]:
            user_offers2ts_qty[row[db_obj.db_param.ID_USER_OFFER]][row[db_obj.db_param.TS_DELIVERY]] += row[
                db_obj.db_param.QTY_ENERGY_TRADED]
        else:
            user_offers2ts_qty[row[db_obj.db_param.ID_USER_OFFER]][row[db_obj.db_param.TS_DELIVERY]] = row[
                db_obj.db_param.QTY_ENERGY_TRADED]

        # for the user bids
        if row[db_obj.db_param.TS_DELIVERY] in user_bids2ts_qty[row[db_obj.db_param.ID_USER_BID]]:
            user_bids2ts_qty[row[db_obj.db_param.ID_USER_BID]][row[db_obj.db_param.TS_DELIVERY]] -= row[
                db_obj.db_param.QTY_ENERGY_TRADED]
        else:
            user_bids2ts_qty[row[db_obj.db_param.ID_USER_BID]][row[db_obj.db_param.TS_DELIVERY]] = row[
                db_obj.db_param.QTY_ENERGY_TRADED]

        list_ts_delivery.append(row[db_obj.db_param.TS_DELIVERY])

    list_ts_delivery = sorted(list(set(list_ts_delivery)))  # eliminate duplicates and sort in ascending order
    # we know aggregate both the users who bid and the ones who offer
    list_users_offers.extend(list_users_bids)
    list_users_offers = list(set(list_users_offers))

    # we now map each user to its meter
    map_user2meter = db_obj.get_map_to_main_meter()
    # we filter the rest of the mappings from the dict and get only user 2 meter
    user2meter = dict([(user, map_user2meter[user]) for user in list_users_offers])

    assert list_users_offers == list(user2meter.keys()), "The list of users from the market result and the meters " \
                                                         "does not match "

    meter2ts_qty = dict([(user2meter[user], {}) for user in list_users_offers])

    for user, meter in user2meter.items():
        # we first extract the timesteps where the user had an interaction, offer or bid
        try:
            list_current_ts = list(user_offers2ts_qty[user].keys())
        except KeyError:
            list_current_ts = []
        try:
            list_current_ts.extend(list(user_bids2ts_qty[user].keys()))
        except KeyError:
            pass
        list_current_ts = list(set(list_current_ts))  # eliminate duplicates
        for ts in list_current_ts:
            # there may be an offer with such ts but not a bid or viceversa, so we initialize the other to 0
            try:
                offer = user_offers2ts_qty[user][ts]
            except KeyError:
                offer = 0
            try:
                bid = user_bids2ts_qty[user][ts]
            except KeyError:
                bid = 0
            meter2ts_qty[meter][ts] = offer - bid

    assert list(meter2ts_qty.keys()) == list(user2meter.values()), "Meters do not match in market and meter tables"

    # we create the dataframe for the delta readings and append the information
    simulated_meter_readings_delta = pd.DataFrame(columns=[db_obj.db_param.TS_DELIVERY, db_obj.db_param.ID_METER,
                                                           db_obj.db_param.ENERGY_IN, db_obj.db_param.ENERGY_OUT])
    for meter in meter2ts_qty:
        for ts in meter2ts_qty[meter]:
            if not rand_percent_var:  # not equal to 0
                rand_factor = random.randrange(-rand_percent_var, rand_percent_var) / 100.0 + 1.0
            else:
                rand_factor = 1
            if meter2ts_qty[meter][ts] > 0:
                simulated_meter_readings_delta = simulated_meter_readings_delta.append(
                    {db_obj.db_param.TS_DELIVERY: ts, db_obj.db_param.ID_METER: meter,
                     db_obj.db_param.ENERGY_IN: int(round(meter2ts_qty[meter][ts] * rand_factor)),
                     db_obj.db_param.ENERGY_OUT: 0}, ignore_index=True)
            else:
                simulated_meter_readings_delta = simulated_meter_readings_delta.append(
                    {db_obj.db_param.TS_DELIVERY: ts, db_obj.db_param.ID_METER: meter,
                     db_obj.db_param.ENERGY_IN: 0,
                     db_obj.db_param.ENERGY_OUT: -int(round(meter2ts_qty[meter][ts] * rand_factor))}, ignore_index=True)

    simulated_meter_readings_delta.sort_values(by=[db_obj.db_param.TS_DELIVERY, db_obj.db_param.ID_METER])
    simulated_meter_readings_delta = simulated_meter_readings_delta.reset_index(drop=True)

    return simulated_meter_readings_delta, list_ts_delivery


def test_ts_uint(ts_delivery):
    timestep_size = int(15 * 60)
    horizon = int(7 * 24 * 60 * 60 / timestep_size)
    monday_00 = 1626040800  # reference unix time from Monday 12th July 2021 at 00:00 at Berlin timezone
    dist = horizon * timestep_size + 1
    div = int((ts_delivery - monday_00) / dist)
    # we transform first the ts into a ts inside a week time starting from monday_00
    new_ts = ts_delivery - div * dist
    # we then calculate the index based on the distance, being monday_00 the 0 up to 672
    rest = int(new_ts % monday_00)
    index = int(rest / timestep_size)
    return index


if __name__ == '__main__':
    # setup_test_general(generate_random_test_data=True)
    # setup_clearing_ex_ante_test(generate_random_test_data=True)
    setup_settlement_test(generate_random_test_data=True)
