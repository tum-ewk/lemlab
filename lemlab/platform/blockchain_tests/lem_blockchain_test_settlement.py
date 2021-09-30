import pytest
import pandas as pd
import time

from tqdm import tqdm

from lemlab.platform import lem, lem_settlement
from lemlab.platform.blockchain_tests import test_utils
from lemlab.platform.lem import _add_supplier_bids, clearing_da
from lemlab.bc_connection.bc_connection import BlockchainConnection

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
def test_market_clearing_full():
    print("Starting full market clearing test")
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

    print("Market clearing done in", (end - start) / 60, "minutes")

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
    print("Market clearing full test passed and finished")


def test_balancing_energy():
    # this test requires first to have a fully completed market clearing stored in both the DB and the Blockchain
    print("\nStarting balancing energies test")
    # for the database
    list_ts_delivery = test_utils.test_simulate_meter_readings_from_market_results(db_obj=db_obj, rand_percent_var=15)
    lem_settlement.determine_balancing_energy(db_obj, list_ts_delivery)

    meter_readings_delta = db_obj.get_meter_readings_delta().sort_values(
        by=[db_obj.db_param.TS_DELIVERY, db_obj.db_param.ID_METER])
    meter_readings_delta = meter_readings_delta.reset_index(drop=True)
    balancing_energies_db = db_obj.get_energy_balancing()

    assert not meter_readings_delta.empty, "Error: the delta meter readings are empty"
    assert not balancing_energies_db.empty, "Error: the balancing energy is empty"

    # for the blockchain
    # connect to our contract
    settlement_dict = config['db_connections']['bc_dict']
    settlement_dict["contract_name"] = "Settlement"
    bc_obj_set = BlockchainConnection(settlement_dict)
    # set the meter readings and determine balancing energy
    tx_hash = bc_obj_set.log_meter_readings_delta(meter_readings_delta)
    bc_obj_set.wait_for_transact(tx_hash)

    # first, assert that the meter_reading_detlas are the same
    delta_meters = bc_obj_set.get_meter_readings_delta().sort_values(
        by=[db_obj.db_param.TS_DELIVERY, db_obj.db_param.ID_METER])
    delta_meters = delta_meters.reset_index(drop=True)
    pd.testing.assert_frame_equal(delta_meters, meter_readings_delta, check_dtype=False)

    # assert that the meters are the same in both
    delda_ids = set(delta_meters[db_obj.db_param.ID_METER])
    delda_ids = sorted(list(delda_ids))
    market_results = bc_obj_set.get_market_results()
    meters = sorted(list(set(bc_obj.get_list_all_meters()[db_obj.db_param.ID_METER].tolist())))
    assert delda_ids == meters
    assert len(delda_ids) == 20

    # finally, we calculate the balancing energy and compare it to the one on the DB
    bc_obj_set.determine_balancing_energy(list_ts_delivery)
    balancing_energies_blockchain = bc_obj_set.get_energy_balances()

    balancing_energies_db = balancing_energies_db.sort_values(by=[bc_obj_set.bc_param.ID_METER,
                                                                  bc_obj_set.bc_param.TS_DELIVERY])
    balancing_energies_db = balancing_energies_db.reset_index(drop=True)
    balancing_energies_blockchain = balancing_energies_blockchain.sort_values(by=[bc_obj_set.bc_param.ID_METER,
                                                                                  bc_obj_set.bc_param.TS_DELIVERY])
    balancing_energies_blockchain = balancing_energies_blockchain.reset_index(drop=True)
    print("Bal energies", balancing_energies_blockchain)
    print("Bal db", balancing_energies_db)

    assert len(balancing_energies_db) == len(balancing_energies_blockchain), \
        "Error, the len of both dataframes isnt equal"
    if balancing_energies_blockchain.empty:
        print("Error, blockchain dataframe is empty")
        assert False
    else:
        pd.testing.assert_frame_equal(balancing_energies_db, balancing_energies_blockchain, check_dtype=False)

    print("Balancing energies test finished")


def test_balancing_costs():
    # this test compares the balancing costs for the db and the blockchain
    print("Starting test for the balancing cost")
    # for the DB
    balancing_db = db_obj.get_energy_balancing()
    list_ts_delivery = balancing_db["ts_delivery"].to_list()
    list_ts_delivery = sorted(list(set(list_ts_delivery)))
    sim_path = "C:/Users/ga47num/PycharmProjects/lemlab/scenarios/"
    files_path = "C:/Users/ge93sut/PycharmProjects/lemlab/input_data/"

    lem_settlement.set_prices_settlement(db_obj=db_obj, path_simulation=sim_path, files_path=files_path,
                                         list_ts_delivery=list_ts_delivery)
    settlement_prices_db = db_obj.get_prices_settlement()
    settlement_prices_db = settlement_prices_db.sort_values(by=[db_obj.db_param.TS_DELIVERY,
                                                                db_obj.db_param.PRICE_ENERGY_BALANCING_POSITIVE])
    settlement_prices_db = settlement_prices_db.reset_index(drop=True)

    # for the blockchain
    settlement_dict = config['db_connections']['bc_dict']
    settlement_dict["contract_name"] = "Settlement"
    bc_obj_set = BlockchainConnection(settlement_dict)
    bc_obj_set.set_prices_settlement(list_ts_delivery)

    settlement_prices_blockchain = bc_obj_set.get_prices_settlement()
    settlement_prices_blockchain = settlement_prices_blockchain.sort_values(by=[bc_obj_set.bc_param.TS_DELIVERY,
                                                                                bc_obj_set.bc_param.PRICE_ENERGY_BALANCING_POSITIVE])
    settlement_prices_blockchain = settlement_prices_blockchain.reset_index(drop=True)
    # asserting of both
    print(f"db: {settlement_prices_db}")
    print(f"bc: {settlement_prices_blockchain}")
    pd.testing.assert_frame_equal(settlement_prices_db, settlement_prices_blockchain)

    # Now for the log transactions
    ts_now = round(time.time())
    supplier = "supplier01"
    lem_settlement.update_balance_balancing_costs(db_obj=db_obj, t_now=ts_now, list_ts_delivery=list_ts_delivery,
                                                  id_retailer=supplier, lem_config=config["lem"])

    log_transactions_db = db_obj.get_logs_transactions()

    log_transactions_db = log_transactions_db.loc[log_transactions_db[db_obj.db_param.TYPE_TRANSACTION] == "balancing"]
    log_transactions_db = log_transactions_db.sort_values(by=[db_obj.db_param.TS_DELIVERY, db_obj.db_param.ID_USER,
                                                              db_obj.db_param.QTY_ENERGY])
    log_transactions_db = log_transactions_db.reset_index(drop=True)

    bc_obj_set.update_balance_balancing_costs(list_ts_delivery=list_ts_delivery, ts_now=ts_now, supplier_id=supplier)
    log_transactions_blockchain = bc_obj_set.get_logs_transactions()
    log_transactions_blockchain = log_transactions_blockchain.sort_values(
        by=[db_obj.db_param.TS_DELIVERY, db_obj.db_param.ID_USER, db_obj.db_param.QTY_ENERGY])
    log_transactions_blockchain = log_transactions_blockchain.reset_index(drop=True)
    print("DB", log_transactions_db)
    print("Bc", log_transactions_blockchain)
    # testing of both transactions
    pd.testing.assert_frame_equal(log_transactions_db, log_transactions_blockchain)

    bc_obj_set.get_events()  # print the emited events

    # Finally, for the updated balances
    updated_user_balances_db = db_obj.get_info_user()
    updated_user_balances_db = updated_user_balances_db.sort_values(
        by=[db_obj.db_param.BALANCE_ACCOUNT, db_obj.db_param.ID_USER, db_obj.db_param.T_UPDATE_BALANCE])
    updated_user_balances_db = updated_user_balances_db.reset_index(drop=True)

    updated_user_balances_blokchain = bc_obj.get_list_all_users()
    updated_user_balances_blokchain = updated_user_balances_blokchain.sort_values(
        by=[db_obj.db_param.BALANCE_ACCOUNT, db_obj.db_param.ID_USER, db_obj.db_param.T_UPDATE_BALANCE])
    updated_user_balances_blokchain = updated_user_balances_blokchain.reset_index(drop=True)

    pd.testing.assert_frame_equal(updated_user_balances_db, updated_user_balances_blokchain)
