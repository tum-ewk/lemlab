import pytest
import pandas as pd
import time

from tqdm import tqdm

from lemlab.platform import lem, lem_settlement
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

verbose_bc = True
verbose_db = True
shuffle = False


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
def test_energy_balances():
    # Set clearing time
    t_clearing_start = round(time.time())

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
            print("Both dataframes resulted empty")
            assert True
        else:
            pd.testing.assert_frame_equal(market_results_blockchain, market_results_python, check_dtype=False)
            assert True
    ## until now it was same as full market clearing

    temp_market_results_blockchain = bc_obj.functions.getTempMarketResults().call()
    list_ts_delivery = lem_settlement._get_list_ts_delivery_ready(db_obj)
    # if there are no market results, determine energies will not work
    assert (len(temp_market_results_blockchain) > 0)
    # we then take the meter readings and log them on the blockchain
    meter_readings = db_obj.get_meter_readings_delta()
    extra_bool = pd.DataFrame(True, range(len(meter_readings)))
    meter_readings.join(extra_bool)
    bc_obj.log_meter_readings_delta(meter_readings)

    balancing_energies_db = db_obj.get_energy_balancing()
    # for the blockchain
    balancing_energies_blockchain = bc_obj.determine_balancing_energy(list_ts_delivery)
    assert (len(balancing_energies_db) == len(balancing_energies_blockchain),
            "Error, the dimensions of both dataframes arent equal")
    assert (pd.testing.assert_frame_equal(balancing_energies_db, balancing_energies_blockchain))


def test():
    list_ts_delivery = lem_settlement._get_list_ts_delivery_ready(db_obj)
    # for the database
    lem_settlement.determine_balancing_energy(db_obj, list_ts_delivery)
    balancing_energies_db = db_obj.get_energy_balancing()
    # for the blockchain
    balancing_energies_blockchain = bc_obj.determine_balancing_energy(list_ts_delivery)
    assert (len(balancing_energies_db) == len(balancing_energies_blockchain),
            "Error, the dimensions of both dataframes arent equal")
    assert (pd.testing.assert_frame_equal(balancing_energies_db, balancing_energies_blockchain))
