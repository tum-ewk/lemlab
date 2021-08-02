import pytest
import pandas as pd
import time

from tqdm import tqdm

from lemlab.platform import lem
from lemlab.platform.blockchain_tests import test_utils
from lemlab.platform.lem import _add_supplier_bids, clearing_da

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
verbose = True


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


# this test, tests that for every ts_delivery, a single clearing produces the same results on python and blockchain
def test_clearings():
    t_now = round(time.time())
    # t_now = 1592791800
    market_horizon = config['lem']['horizon_clearing']
    interval_clearing = config['lem']['interval_clearing']
    # Calculate number of market clearings
    n_clearings = int(market_horizon / interval_clearing)
    print(f"Number of clearings: {n_clearings}")

    supplier_bids = False
    uniform_pricing = True
    discriminative_pricing = True

    # Set first clearing interval to next clearing interval period (ceil up to next clearing interval)
    t_clearing_first = t_now - (t_now % config['lem']['interval_clearing']) + config['lem']['interval_clearing']

    # Go through all specified number of clearings
    if verbose:
        iterations = tqdm(range(1, n_clearings + 1))
    else:
        iterations = range(1, n_clearings + 1)

    pos_c = 0
    for i in iterations:
        if verbose:
            print(f"Clearing step number: {i}/{n_clearings}")
        # current time for clearing
        t_clearing_current = t_clearing_first + config['lem']['interval_clearing'] * i

        t_central_clearing_start = round(time.time())
        # triggering centralized clearing
        positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = single_clearing_db_standard(
            t_clearing_current, open_offers_db, open_bids_db, verbose=False)

        t_central_clearing_end = round(time.time())
        t_central_clearing_total = t_central_clearing_end - t_central_clearing_start
        if verbose:
            print(f"Time, centralized clearing, total time: {t_central_clearing_total}")
        # added one bool at the end for simulation_test to perform additional sort over quantity( given equal price
        # and quality)

        t_decentral_clearing_start = round(time.time())
        tx_hash = bc_obj.functions.single_clearing(t_clearing_current, supplier_bids, uniform_pricing,
                                                   discriminative_pricing, t_clearing_first, True, False,
                                                   verbose, False, True).transact({'from': bc_obj.coinbase})
        # added extra timeout to wait for 10 mins
        bc_obj.wait_for_transact(tx_hash)

        t_decentral_clearing_end = round(time.time())
        t_decentral_clearing_total = t_decentral_clearing_end - t_decentral_clearing_start
        if verbose:
            print(f"Time, decentralized clearing, total time: {t_decentral_clearing_total}")
        # time.sleep(3)
        temp_market_results_blockchain = bc_obj.functions.getTempMarketResults().call()

        if positions_cleared is None:
            if verbose:
                print("No positions cleared")
            pos_c += 1
            assert len(temp_market_results_blockchain) == 0
        else:
            assert len(positions_cleared) == len(temp_market_results_blockchain)
            if len(positions_cleared) > 0 and len(temp_market_results_blockchain) > 0:
                df_results_blockchain = prepare_df_single_clearing(temp_market_results_blockchain,
                                                                   positions_cleared)
                try:
                    pd.testing.assert_frame_equal(df_results_blockchain, positions_cleared, check_dtype=False)
                    assert True
                except AssertionError:
                    assert False

    # assert False
    print(pos_c)


# got the results from the blockchain, I reformat the data to get a dataframe with the same shape as the one given in
# input
def prepare_df_single_clearing(temp_market_results_blockchain, bids_offers_cleared_python):
    df_results_blockchain = pd.DataFrame(temp_market_results_blockchain,
                                         columns=bids_offers_cleared_python.columns.to_list())

    if len(df_results_blockchain) > 0:
        df_results_blockchain.set_index(df_results_blockchain[db_obj.db_param.QTY_ENERGY_TRADED].cumsum(),
                                        inplace=True, drop=False)
        df_results_blockchain.index.name = bids_offers_cleared_python.index.name
    return df_results_blockchain


# this method performs a single clearing using python and the data from the database
def single_clearing_db_standard(t_clearing_current, offers_db, bids_db, verbose, plotting=False, config_supplier=None):
    positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = None, None, None, None, None
    # Extract data for specific time of delivery
    offers_ts_d = offers_db[offers_db[db_obj.db_param.TS_DELIVERY] == t_clearing_current]
    bids_ts_d = bids_db[bids_db[db_obj.db_param.TS_DELIVERY] == t_clearing_current]
    # t_d_last_update = get_update_time(bids_sorted, offers_sorted)

    # Check whether offers or bids are empty
    if offers_ts_d.empty or bids_ts_d.empty:
        if verbose:
            print(pd.Timestamp(t_clearing_current, unit="s", tz="Europe/Berlin"),
                  'No clearing - supply and/or bids are empty')
    # Offers and bids are not empty
    else:
        if verbose:
            print(pd.Timestamp(t_clearing_current, unit="s", tz="Europe/Berlin"), 'Clearing')
        # Check whether this is the first clearing period and whether the flag supplier bids is true
        if config_supplier is not None:
            # Insert supplier bids and offers
            bids_ts_d, offers_ts_d = _add_supplier_bids(db_obj,
                                                        config_supplier,
                                                        t_clearing_current,
                                                        bids_ts_d,
                                                        offers_ts_d)

        bids_ts_d = lem._convert_qualities_to_int(db_obj, bids_ts_d, config['lem']['types_quality'])
        offers_ts_d = lem._convert_qualities_to_int(db_obj, offers_ts_d, config['lem']['types_quality'])
        # Calculate clearing price NEW
        positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
            clearing_da(db_obj,
                        config['lem'],
                        offers_ts_d,
                        bids_ts_d,
                        plotting=plotting,
                        plotting_title=pd.Timestamp(t_clearing_current, unit="s", tz="Europe/Berlin"),
                        shuffle=False)

    return positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared
