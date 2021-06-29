import os
import time
from pathlib import Path
import subprocess
import yaml
import pandas as pd

from lemlab.db_connection import db_connection, db_param
from lemlab.platform import blockchain_utils
from lemlab.platform.init_data_blockchain_random import init_random_data

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
    blockchain_utils.setUpBlockchain(timeout=timeout)
    offers_blockchain_archive, bids_blockchain_archive = blockchain_utils.getOffers_or_Bids(
        isOffer=True, temp=False), blockchain_utils.getOffers_or_Bids(isOffer=False, temp=False)
    open_offers_blockchain, open_bids_blockchain = blockchain_utils.getOffers_or_Bids(
        isOffer=True, temp=True), blockchain_utils.getOffers_or_Bids(isOffer=False, temp=True)
    user_infos_blockchain, id_meters_blockchain = blockchain_utils.functions.get_user_infos().call(), \
                                                  blockchain_utils.functions.get_id_meters().call()

    return offers_blockchain_archive, bids_blockchain_archive, open_offers_blockchain, open_bids_blockchain, \
           offers_db_archive, bids_db_archive, open_offers_db, open_bids_db, user_infos_blockchain, user_infos_db, \
           id_meters_blockchain, id_meters_db, config, list(open_offers_db.keys()).index(db_param.QUALITY_ENERGY), \
           list(open_offers_db.keys()).index(db_param.PRICE_ENERGY), db_obj


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
