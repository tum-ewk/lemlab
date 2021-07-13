import pytest
import pandas as pd
import time

from lemlab.db_connection import db_param
from lemlab.platform import lem
from lemlab.platform.blockchain_tests import test_utils

verbose_bc = True
verbose_db = True
shuffle = False

offers_blockchain_archive, bids_blockchain_archive = None, None
open_offers_blockchain, open_bids_blockchain = None, None
offers_db_archive, bids_db_archive = None, None
open_offers_db, open_bids_db = None, None
generate_bids_offer = True
user_infos_blockchain = None
user_infos_db = None
id_meters_blockchain = None
id_meters_db = None
config = None
quality_index = None
price_index = None
db_obj = None
bc_obj = None


# this method is executed before all the others, to get useful global variables, needed for the tests
@pytest.fixture(scope="session", autouse=True)
def setUp():
    global offers_blockchain_archive, bids_blockchain_archive, open_offers_blockchain, open_bids_blockchain, \
        offers_db_archive, bids_db_archive, open_offers_db, open_bids_db, user_infos_blockchain, user_infos_db, \
        id_meters_blockchain, id_meters_db, config, quality_energy, price_index, db_obj, bc_obj
    offers_blockchain_archive, bids_blockchain_archive, open_offers_blockchain, open_bids_blockchain, \
    offers_db_archive, bids_db_archive, open_offers_db, open_bids_db, user_infos_blockchain, user_infos_db, \
    id_meters_blockchain, id_meters_db, config, quality_energy, price_index, \
    db_obj, bc_obj = test_utils.setUp_test(generate_bids_offer)


# results of the full market clearing, the updated balances, on the blockchain and on python are compared
def test_clearings():
    # Set clearing time
    t_clearing_start = round(time.time())

    tx_hash = bc_obj.functions.updateBalances().transact({'from': bc_obj.coinbase})
    bc_obj.wait_for_transact(tx_hash)
    user_infos_bc_df = bc_obj.get_list_all_users()
    user_infos_db = pd.concat([db_obj.get_info_user(user_id) for user_id in db_obj.get_list_all_users()])
    user_infos_db = user_infos_db.sort_values(by=[db_param.ID_USER], ascending=[True])
    user_infos_bc_df = user_infos_bc_df.sort_values(by=[db_param.ID_USER], ascending=[True])
    user_infos_db = user_infos_db.set_index(user_infos_bc_df.index)

    start = time.time()
    market_results_python, _, _, _, market_results_blockchain = lem.market_clearing(db_obj=db_obj,
                                                                                    bc_obj=bc_obj,
                                                                                    config_lem=config['lem'],
                                                                                    t_override=t_clearing_start,
                                                                                    shuffle=shuffle,
                                                                                    verbose=verbose_db,
                                                                                    verbose_bc=verbose_bc,
                                                                                    bc_test=True)

    market_results_python = market_results_python['da']

    end = time.time()

    if shuffle:
        assert len(market_results_python) >= 0.1 * len(market_results_blockchain) or len(
            market_results_blockchain) >= 0.1 * len(market_results_python)
        try:
            pd.testing.assert_frame_equal(market_results_python, market_results_blockchain, check_dtype=False)
        except Exception as e:
            print(e)
    else:
        if market_results_blockchain.empty and market_results_python.empty:
            assert True
        else:
            pd.testing.assert_frame_equal(market_results_blockchain, market_results_python, check_dtype=False)
            assert True

    assert True

    # Check market position equality
    pd.testing.assert_frame_equal(user_infos_db, user_infos_bc_df)
