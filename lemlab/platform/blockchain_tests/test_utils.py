import os
import time
from pathlib import Path
import subprocess
import yaml
import pandas as pd

from lemlab.db_connection import db_connection, db_param
from lemlab.platform import blockchain_utils
from lemlab.platform.init_data_blockchain_random import init_random_data
from lemlab.bc_connection.bc_connection import BlockchainConnection
from lemlab.bc_connection.bc_param import Platform_dict

from current_scenario_file import scenario_file_path


# I get basic variables from the blockchain. these variables are then used in the tests
def setUp_test(generate_bids_offer, timeout=600):
    yaml_file = scenario_file_path
    if generate_bids_offer:
        init_random_data()

    # load configuration file
    with open(f"" + yaml_file) as config_file:
        config = yaml.load(config_file, Loader=yaml.FullLoader)

    # Create a db connection object
    db_obj = db_connection.DatabaseConnection(db_dict=config['db_connections']['database_connection_admin'],
                                              lem_config=config['lem'])
    # Read offers and bids from db
    bids_db_archive, offers_db_archive = db_obj.get_positions_archive()
    open_bids_db, open_offers_db = db_obj.get_open_positions()
    user_infos_db, id_meters_db = [db_obj.get_info_user(user_id) for user_id in
                                   db_obj.get_list_all_users()], db_obj.get_info_meter()
    db_obj.end_connection()
    # print('Market archive contains', str(len(offers_db_archive)), 'valid offers and',
    #       str(len(bids_db_archive)), 'valid bids.')

    bc_obj = BlockchainConnection(Platform_dict)
    # blockchain_utils.setUpBlockchain(timeout=timeout)

    offers_blockchain_archive = bc_obj.get_open_positions(isOffer=True, temp=False, returnList=True)
    bids_blockchain_archive = bc_obj.get_open_positions(isOffer=False, temp=False, returnList=True)
    open_offers_blockchain = bc_obj.get_open_positions(isOffer=True, temp=True, returnList=True)
    open_bids_blockchain = bc_obj.get_open_positions(isOffer=False, temp=True, returnList=True)

    user_infos_blockchain = bc_obj.get_list_all_users(returnList=True)
    id_meters_blockchain = bc_obj.get_list_all_meters(returnList=True)

    return offers_blockchain_archive, bids_blockchain_archive, open_offers_blockchain, open_bids_blockchain, \
           offers_db_archive, bids_db_archive, open_offers_db, open_bids_db, user_infos_blockchain, user_infos_db, \
           id_meters_blockchain, id_meters_db, config, list(open_offers_db.keys()).index(db_param.QUALITY_ENERGY), \
           list(open_offers_db.keys()).index(db_param.PRICE_ENERGY), db_obj, bc_obj


def result_test(testname, passed):
    testname = str(testname)
    date = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    res = pd.DataFrame({"test_name": testname, "result": passed, "date": date}, index=[0])
    outfile = "test_results.xlsx"
    if os.path.isfile(outfile):
        f = pd.read_excel(outfile)
        f = pd.concat([f, res])
        with pd.ExcelWriter(outfile) as writer:
            f.to_excel(writer)
    else:
        with pd.ExcelWriter(outfile) as writer:
            res.to_excel(writer)
