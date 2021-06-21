import sys
import os
from lemlab.db_connection import db_connection, db_param
from lemlab.platform import blockchain_utils, lem
from pathlib import Path
import pandas as pd
import time
import yaml
from tqdm import tqdm

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
    db_obj_example = db_connection.DatabaseConnection(db_dict=config['db_connections']['database_connection_admin'],
                                                      lem_config=config['lem'])
    # Initialize database
    db_obj_example.init_db(clear_tables=True, reformat_tables=True)

    # Create list of random user ids and meter ids
    ids_users_random = lem.create_user_ids(num=config['prosumer']['number_of'])
    ids_meter_random = lem.create_user_ids(num=config['prosumer']['number_of'])

    blockchain_utils.setUpBlockchain(project_dir=project_dir)
    blockchain_utils.clearTempData()
    blockchain_utils.clearPermanentData()

    # Register meters and users on database
    for z in range(len(ids_users_random)):
        # all the users are initialized with the same balance, when de market clearing happens, their balances
        # are updated
        df_insert = pd.DataFrame(data=[[ids_users_random[z], 1000, 0, 10000, 100, 'green', 10, 'zi', 0, 0, 0]],
                                 columns=db_obj_example.get_table_columns(db_obj_example.db_param.NAME_TABLE_INFO_USER))
        db_obj_example.register_user(df_in=df_insert)

        blockchain_utils.functions.push_user_info(tuple(df_insert.values.tolist()[0])).transact(
            {'from': blockchain_utils.coinbase})

        df_insert = pd.DataFrame(
            data=[[ids_meter_random[z], ids_users_random[z], 1, "0", 'aggregator', 'green', 0, 0, 'test']],
            columns=db_obj_example.get_table_columns(db_obj_example.db_param.NAME_TABLE_INFO_METER))
        db_obj_example.register_meter(df_in=df_insert)
        blockchain_utils.functions.push_id_meters(tuple(df_insert.values.tolist()[0])).transact(
            {'from': blockchain_utils.coinbase})

    time.sleep(20)
    if len(blockchain_utils.functions.get_user_infos().call()) == len(db_obj_example.get_list_all_users()) and len(
            blockchain_utils.functions.get_id_meters().call()) == len(db_obj_example.get_list_main_meters()):
        print("successfully stored " + str(len(db_obj_example.get_list_all_users())) + " user_infos and " + str(
            len(db_obj_example.get_list_main_meters())) + " id_meters")
    else:
        print("different number of user_infos and id_meters on blockchain and db")
        print("user_infos on db: " + str(len(db_obj_example.get_list_all_users())))
        print("user_infos on blockchain: " + str(len(blockchain_utils.functions.get_user_infos().call())))
        print("id_meters on db: " + str(len(db_obj_example.get_list_main_meters())))
        print("id_meters on blockchain: " + str(len(blockchain_utils.functions.get_id_meters().call())))
        raise Exception("Error in inserting user_infos and id_meters")

    # Compute random market positions
    positions = lem.create_random_positions(db_obj=db_obj_example,
                                            config=config,
                                            ids_user=ids_users_random,
                                            n_positions=200,
                                            verbose=False)
    # Post positions to market
    db_obj_example.post_positions(positions)

    bdb, odb = db_obj_example.get_open_positions()  # returns bids and offers from the database
    print("to push " + str(len(bdb) + len(odb)) + " offers/bids")

    positions = _convert_qualities_to_int(db_obj_example, pd.concat([odb, bdb]), config['lem']['types_quality'])

    temp = True  # if we wanna save the offers and bids as temporal data
    permt = False  # if we wanna save the offers and bids as permanent data
    for position in tqdm(positions.iterrows(), total=positions.shape[0]):
        # off_bid = _convert_qualities_to_int(db_obj_example, ob[1], config['lem']['types_quality'])

        # blockchain_utils.functions.pushOfferOrBid(tuple(off_bid.values), off_bid[list(ob[1].keys()).index(db_param.TYPE_POSITION)] == 'offer', True).transact({'from': blockchain_utils.coinbase})
        # pushes offers and bids, last bool arguments are for temp and permanent data respectively

        blockchain_utils.functions.pushOfferOrBid(tuple(position[1]),
                                                  position[1][db_param.TYPE_POSITION] == 'offer',
                                                  temp, permt).transact({'from': blockchain_utils.coinbase})

    time.sleep(20)
    if len(blockchain_utils.getOffers_or_Bids()) == len(odb) and len(blockchain_utils.getOffers_or_Bids(False)) == len(
            bdb):
        print("successfully stored " + str(len(odb)) + " offers and " + str(len(bdb)) + " bids")
    else:
        print("different number of offers and bids on blockchain and db")
        print("offers on db: " + str(len(odb)))
        print("offers on blockchain: " + str(len(blockchain_utils.getOffers_or_Bids())))
        print("bids on db: " + str(len(bdb)))
        print("bids on blockchain: " + str(len(blockchain_utils.getOffers_or_Bids(False))))


if __name__ == '__main__':
    init_random_data()
