import sys
import os
from lemlab.db_connection import db_connection, db_param
from lemlab.platform import blockchain_utils, lem
from pathlib import Path
import pandas as pd
import time
import yaml
# from tqdm import tqdm
from lemlab.bc_connection.bc_connection import BlockchainConnection
from lemlab.bc_connection.bc_param import Platform_dict

from current_scenario_file import scenario_file_path

project_dir = str(Path(__file__).parent.parent.parent)
sys.path.append(project_dir)


# the function had to be manually copied to avoid circular imports
def _convert_qualities_to_int(db_obj, positions, dict_types):
    dict_types_inverted = {v: k for k, v in dict_types.items()}
    positions[db_obj.db_param.QUALITY_ENERGY] = [dict_types_inverted[i] for i in
                                                 positions[db_obj.db_param.QUALITY_ENERGY]]
    return positions


# this file initializes random data on the blockchain and on the database
def init_random_data():
    yaml_file = scenario_file_path
    # load configuration file
    with open(yaml_file) as config_file:
        config = yaml.load(config_file, Loader=yaml.FullLoader)
    # Create a db connection object
    db_obj = db_connection.DatabaseConnection(db_dict=config['db_connections']['database_connection_admin'],
                                              lem_config=config['lem'])
    # Initialize database
    db_obj.init_db(clear_tables=True, reformat_tables=True)

    # Create list of random user ids and meter ids
    ids_users_random = lem.create_user_ids(num=config['prosumer']['general_number_of'])
    ids_meter_random = lem.create_user_ids(num=config['prosumer']['general_number_of'])
    ids_market_agents = lem.create_user_ids(num=config['prosumer']['general_number_of'])

    bc_obj = BlockchainConnection(Platform_dict)        # connect to the platform contract
    bc_obj.clear_temp_data()
    bc_obj.clear_permanent_data()

    # Register meters and users on database
    for z in range(len(ids_users_random)):

        cols, types = db_obj.get_table_columns(db_obj.db_param.NAME_TABLE_INFO_USER, dtype=True)
        col_data = [ids_users_random[z], 1000, 0, 10000, 100, 'green', 10, 'zi', 0, ids_market_agents[z], 0, 0]
        if any([type(data) != typ for data, typ in zip(col_data, types)]):
            raise TypeError("The types of the data and the columns do not match for the info_user")
        df_insert = pd.DataFrame(
            data=[col_data],
            columns=cols)
        db_obj.register_user(df_in=df_insert)

        bc_obj.register_user(df_insert)


        cols, types = db_obj.get_table_columns(db_obj.db_param.NAME_TABLE_INFO_METER, dtype=True)
        col_data = [ids_meter_random[z], ids_users_random[z], "0", "virtual grid meter", 'aggregator', 'green', 0, 0,
                    'test']
        if any([type(data) != typ for data, typ in zip(col_data, types)]):
            raise TypeError("The types of data and columns do not match for the id_meter")
        df_insert = pd.DataFrame(
            data=[col_data],
            columns=cols)
        db_obj.register_meter(df_in=df_insert)

        tx_hash = bc_obj.register_meter(df_insert)

    bc_obj.wait_for_transact(tx_hash)

    if len(bc_obj.get_list_all_users()) == len(db_obj.get_list_all_users()) and len(
            bc_obj.get_list_main_meters()) == len(db_obj.get_list_main_meters()):
        print("successfully stored " + str(len(db_obj.get_list_all_users())) + " user_infos and " + str(
            len(db_obj.get_list_main_meters())) + " id_meters")
    else:
        print("different number of user_infos and id_meters on blockchain and db")
        print("user_infos on db: " + str(len(db_obj.get_list_all_users())))
        print("user_infos on blockchain: " + str(len(bc_obj.get_list_all_users())))
        print("id_meters on db: " + str(len(db_obj.get_list_main_meters())))
        print("id_meters on blockchain: " + str(len(bc_obj.get_list_main_meters())))
        raise Exception("Error in inserting user_infos and id_meters")

    # Compute random market positions
    positions = lem.create_random_positions(db_obj=db_obj,
                                            config=config,
                                            ids_user=ids_users_random,
                                            n_positions=500,
                                            verbose=False)
    # Post positions to market
    db_obj.post_positions(positions)

    bids_db, offers_db = db_obj.get_open_positions()  # returns bids and offers from the database
    print("to push " + str(len(bids_db) + len(offers_db)) + " offers/bids")

    # for the blockchain, the quality of energy needs to be converted to int before storing it
    # however, before, in the DB the quality is stored as a string
    positions = _convert_qualities_to_int(db_obj, pd.concat([offers_db, bids_db]), config['lem']['types_quality'])

    temp = True  # if we wanna save the offers and bids as temporal data
    permt = False  # if we wanna save the offers and bids as permanent data

    tx_hash = bc_obj.push_all_positions(positions, temp, permt)
    bc_obj.wait_for_transact(tx_hash)

    if len(bc_obj.get_open_positions(isOffer=True, returnList=True)) == len(offers_db) and \
            len(bc_obj.get_open_positions(isOffer=False, returnList=True)) == len(bids_db):
        print("successfully stored " + str(len(offers_db)) + " offers and " + str(len(bids_db)) + " bids")
    else:
        print("different number of offers and bids on blockchain and db")
        print("offers on db: " + str(len(offers_db)))
        print("offers on blockchain: " + str(len(bc_obj.get_open_positions(isOffer=True, returnList=True))))
        print("bids on db: " + str(len(bids_db)))
        print("bids on blockchain: " + str(len(bc_obj.get_open_positions(isOffer=False, returnList=True))))


if __name__ == '__main__':
    init_random_data()
