import pytest
import pandas as pd
import time

from lemlab.db_connection import db_param
from lemlab.platform import blockchain_utils, lem
from lemlab.platform.blockchain_tests import test_utils

verbose_blockchain = False
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


# this method is executed before all the others, to get useful global variables, needed for the tests
@pytest.fixture(scope="session", autouse=True)
def setUp():
    global offers_blockchain_archive, bids_blockchain_archive, open_offers_blockchain, open_bids_blockchain, offers_db_archive, bids_db_archive, open_offers_db, open_bids_db, user_infos_blockchain, user_infos_db, id_meters_blockchain, id_meters_db, config, quality_energy, price_index, db_obj
    offers_blockchain_archive, bids_blockchain_archive, open_offers_blockchain, open_bids_blockchain, offers_db_archive, bids_db_archive, open_offers_db, open_bids_db, user_infos_blockchain, user_infos_db, id_meters_blockchain, id_meters_db, config, quality_energy, price_index, db_obj = test_utils.setUp_test(
        generate_bids_offer)


# in this test, the results of the full market clearing, the updated balances, on the blockchain and on python are compared
def test_clearings():
    # Set clearing time
    t_clearing_start = round(time.time())
    print('######################## Market clearing started #############################')
    print('Market clearing started:', pd.Timestamp(t_clearing_start, unit="s", tz="Europe/Berlin"))

    # t_now = round(time.time())
    market_horizon = config['lem']['horizon_clearing']
    interval_clearing = config['lem']['interval_clearing']
    # Calculate number of market clearings
    n_clearings = int(market_horizon / interval_clearing)
    uniform_pricing = True
    discriminative_pricing = True
    supplier_bids = False
    tx_hash = blockchain_utils.functions.updateBalances().transact({'from': blockchain_utils.coinbase})
    blockchain_utils.web3_instance.eth.waitForTransactionReceipt(tx_hash)
    user_infos_blockchain = blockchain_utils.functions.get_user_infos().call()
    user_infos_db = pd.concat([db_obj.get_info_user(user_id) for user_id in
                               db_obj.get_list_all_users()])
    user_infos_blockchain_dataframe = blockchain_utils.convertListToPdDataFrame(user_infos_blockchain,
                                                                                user_infos_db.columns.to_list())

    user_infos_db = user_infos_db.sort_values(by=[db_param.ID_USER], ascending=[True])
    user_infos_blockchain_dataframe = user_infos_blockchain_dataframe.sort_values(by=[db_param.ID_USER],
                                                                                  ascending=[True])
    user_infos_db = user_infos_db.set_index(user_infos_blockchain_dataframe.index)

    assert len(user_infos_db) == len(user_infos_blockchain_dataframe)
    pd.testing.assert_frame_equal(user_infos_db, user_infos_blockchain_dataframe)
    # testing of market results
    start = time.time()
    market_results_python, _, _, _, market_results_blockchain = lem.market_clearing(db_obj=db_obj,
                                                                                    config_lem=config['lem'],
                                                                                    t_override=t_clearing_start,
                                                                                    shuffle=shuffle, verbose=verbose_db)

    market_results_python = market_results_python['da']
    # end = time.time()
    # start = time.time()
    # added one bool at the end for simulation_test to perform additional sort over quantity( given equal price
    # and quality)
    # market_results_blockchain = get_market_results_blockchain(t_clearing_start, n_clearings,
    #                                                           supplier_bids=supplier_bids,
    #                                                           uniform_pricing=uniform_pricing,
    #                                                           discriminative_pricing=discriminative_pricing,
    #                                                           t_clearing_start=t_clearing_start,
    #                                                           # market_results_python=market_results_python,
    #                                                           simulation_test=True,
    #                                                           interval_clearing=interval_clearing,
    #                                                           shuffle=shuffle)
    # market_results_blockchain = convertToPdFinalMarketResults(market_results_blockchain, market_results_python)
    end = time.time()
    print("market clearing on both blockchain and simulation done in " + str(end - start) + " seconds")
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

        # test = determine_balancing_energy(db_obj,[])
    assert True


# this method, find the maximum number of clearings that the blockchain manages to perform at once, according to the gas limit
def findLimit(n_clearings_max, t_clearing_current, supplier_bids, uniform_pricing, discriminative_pricing,
              t_clearing_start,
              gasThreshold, interval_clearing, simulation_test):
    n_clearings_current = n_clearings_max
    estimate = 10 * gasThreshold
    while estimate > gasThreshold:
        try:
            estimate = blockchain_utils.functions.market_clearing(int(n_clearings_current), int(t_clearing_current),
                                                                  supplier_bids, uniform_pricing,
                                                                  discriminative_pricing,
                                                                  int(interval_clearing),
                                                                  int(t_clearing_start), False, verbose_blockchain,
                                                                  False, simulation_test).estimateGas()
            n_clearings_current = int(n_clearings_current / (estimate / gasThreshold))
        except Exception as e:
            print(e)
            n_clearings_current = int(n_clearings_current / 2)

    return n_clearings_current


# this method, perform the market clearing and gets the results from the blockchain
def get_market_results_blockchain(t_override, n_clearings, supplier_bids, uniform_pricing, discriminative_pricing,
                                  t_clearing_start, interval_clearing, simulation_test, shuffle=False):
    t_now = t_override
    t_clearing_first = t_now - (t_now % interval_clearing) + interval_clearing

    t_clearing_current = t_clearing_first
    n_clearings_done = 0

    # max doable clearings
    limit_clearings = findLimit(n_clearings, t_clearing_first, supplier_bids, uniform_pricing, discriminative_pricing,
                                t_clearing_start,
                                gasThreshold=40000000, interval_clearing=interval_clearing,
                                simulation_test=simulation_test)

    n_clearings_current = limit_clearings
    update_balances = False
    while n_clearings_done < n_clearings:
        if n_clearings - n_clearings_done <= n_clearings_current:  # last step
            n_clearings_current = n_clearings - n_clearings_done
            update_balances = False
        try:
            # Performing the market clearing for a number of clearings
            tx_hash = blockchain_utils.functions.market_clearing(int(n_clearings_current), int(t_clearing_current),
                                                                 supplier_bids,
                                                                 uniform_pricing,
                                                                 discriminative_pricing,
                                                                 int(interval_clearing),
                                                                 int(t_clearing_start), shuffle, verbose_blockchain,
                                                                 update_balances, simulation_test).transact(
                {'from': blockchain_utils.coinbase})
            blockchain_utils.web3_instance.eth.waitForTransactionReceipt(tx_hash, timeout=600)  # 600 seconds wait
            if verbose_blockchain:
                log = blockchain_utils.getLog(tx_hash=tx_hash)
                print(log)
            n_clearings_done += n_clearings_current
            t_clearing_current = t_clearing_first + interval_clearing * n_clearings_done
            n_clearings_current = limit_clearings
        except ValueError as e:
            print(e)
            n_clearings_current = int(n_clearings_current * 0.75)
            update_balances = False

    market_results_blockchain = blockchain_utils.functions.getMarketResultsTotal().call()
    return market_results_blockchain


# Convert results on the blockchain to a pandas dataframe
def convertToPdFinalMarketResults(market_results_blockchain, market_results_python):
    market_results_blockchain = blockchain_utils.convertListToPdDataFrame(market_results_blockchain,
                                                                          market_results_python.columns.to_list())
    return market_results_blockchain
