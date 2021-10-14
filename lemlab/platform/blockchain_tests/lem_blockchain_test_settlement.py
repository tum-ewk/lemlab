import pytest
import pandas as pd
import time

from tqdm import tqdm

from lemlab.platform import lem, lem_settlement
from lemlab.platform.blockchain_tests import test_utils
from lemlab.platform.lem import _add_supplier_bids, clearing_da
from lemlab.bc_connection.bc_connection import BlockchainConnection

offers_bc_archive, bids_bc_archive = None, None
open_offers_bc, open_bids_bc = None, None
offers_db_archive, bids_db_archive = None, None
open_offers_db, open_bids_db = None, None
generate_bids_offer = True
user_infos_bc = None
user_infos_db = None
id_meters_bc = None
id_meters_db = None
config = None
quality_index = None
price_index = None
db_obj = None
bc_obj = None
verbose = True
ts_delivery_list = None
bc_obj_settlement = None


# this method is executed before all the others, to get useful global variables, needed for the tests
@pytest.fixture(scope="session", autouse=True)
def setUp():
    global offers_bc_archive, bids_bc_archive, open_offers_bc, open_bids_bc, offers_db_archive, bids_db_archive, \
        open_offers_db, open_bids_db, user_infos_bc, user_infos_db, id_meters_bc, id_meters_db, config, \
        quality_energy, price_index, db_obj, bc_obj, ts_delivery_list, bc_obj_settlement
    offers_bc_archive, bids_bc_archive, open_offers_bc, open_bids_bc, offers_db_archive, bids_db_archive, \
    open_offers_db, open_bids_db, user_infos_bc, user_infos_db, id_meters_bc, id_meters_db, config, quality_energy, \
    price_index, db_obj, bc_obj, ts_delivery_list, bc_obj_settlement \
        = test_utils.setup_settlement_test(generate_bids_offer)


def test_meter_readings():
    # Pre-check whether id on bc and db are equal
    pd.testing.assert_frame_equal(id_meters_db, id_meters_bc, check_dtype=False)

    # Get meter readings delta
    meter_readings_delta_bc = bc_obj_settlement.get_meter_readings_delta().sort_values(
        by=[db_obj.db_param.TS_DELIVERY, db_obj.db_param.ID_METER])
    meter_readings_delta_bc = meter_readings_delta_bc.reset_index(drop=True)
    meter_readings_delta_db = db_obj.get_meter_readings_delta().sort_values(
        by=[db_obj.db_param.TS_DELIVERY, db_obj.db_param.ID_METER])
    meter_readings_delta_db = meter_readings_delta_db.reset_index(drop=True)

    # Check whether meter_readings_delta on bc and db are equal
    pd.testing.assert_frame_equal(meter_readings_delta_bc, meter_readings_delta_db, check_dtype=False)


def test_balancing_energy():
    # Get energy balances from db and bc
    balancing_energies_db = db_obj.get_energy_balancing()
    balancing_energies_bc = bc_obj_settlement.get_energy_balances()

    # Check whether balancing energies are empty
    assert not balancing_energies_db.empty, "Error: balancing energy is empty in db"
    assert not balancing_energies_bc.empty, "Error: balancing energy is empty in bc"

    # Sort balancing energies by meter id and ts delivery
    balancing_energies_db = balancing_energies_db.sort_values(by=[bc_obj.bc_param.ID_METER,
                                                                  bc_obj.bc_param.TS_DELIVERY])
    balancing_energies_db = balancing_energies_db.reset_index(drop=True)
    balancing_energies_bc = balancing_energies_bc.sort_values(by=[bc_obj.bc_param.ID_METER,
                                                                  bc_obj.bc_param.TS_DELIVERY])
    balancing_energies_bc = balancing_energies_bc.reset_index(drop=True)

    # Check whether balancing energies are equal on bc and db
    assert len(balancing_energies_db) == len(balancing_energies_bc), \
        "Error, the len of both dataframes isnt equal"
    if balancing_energies_bc.empty:
        print("Error, blockchain dataframe is empty")
        assert False
    else:
        pd.testing.assert_frame_equal(balancing_energies_db, balancing_energies_bc, check_dtype=False)


def test_prices_settlement():
    # Get settlement prices from db and bc
    settlement_prices_db = db_obj.get_prices_settlement()
    settlement_prices_db = settlement_prices_db.sort_values(by=[db_obj.db_param.TS_DELIVERY,
                                                                db_obj.db_param.PRICE_ENERGY_BALANCING_POSITIVE])
    settlement_prices_db = settlement_prices_db.reset_index(drop=True)
    settlement_prices_bc = bc_obj_settlement.get_prices_settlement()
    settlement_prices_bc = settlement_prices_bc.sort_values(by=[bc_obj.bc_param.TS_DELIVERY,
                                                                bc_obj.bc_param.PRICE_ENERGY_BALANCING_POSITIVE])
    settlement_prices_bc = settlement_prices_bc.reset_index(drop=True)

    # Check whether settlement prices are equal on db and bc
    pd.testing.assert_frame_equal(settlement_prices_db, settlement_prices_bc)


def test_transaction_logs():
    # Get all logged transactions from db and bc
    log_transactions_db = db_obj.get_logs_transactions()
    log_transactions_db = log_transactions_db.loc[
        # log_transactions_db[db_obj.db_param.TYPE_TRANSACTION].str.contains('balancing|levies')]
        log_transactions_db[db_obj.db_param.TYPE_TRANSACTION].str.contains('balancing')]
    log_transactions_db = log_transactions_db.sort_values(
        by=[db_obj.db_param.TS_DELIVERY, db_obj.db_param.ID_USER, db_obj.db_param.QTY_ENERGY])
    log_transactions_db = log_transactions_db.reset_index(drop=True)
    log_transactions_bc = bc_obj_settlement.get_logs_transactions()
    log_transactions_bc = log_transactions_bc.sort_values(
        by=[db_obj.db_param.TS_DELIVERY, db_obj.db_param.ID_USER, db_obj.db_param.QTY_ENERGY])
    log_transactions_bc = log_transactions_bc.reset_index(drop=True)

    # Check whether all transactions on bc and db are equal
    pd.testing.assert_frame_equal(log_transactions_db, log_transactions_bc)


def test_user_info():
    info_user_db = db_obj.get_info_user()
    info_user_db = info_user_db.sort_values(
        by=[db_obj.db_param.BALANCE_ACCOUNT, db_obj.db_param.ID_USER, db_obj.db_param.T_UPDATE_BALANCE])
    info_user_db = info_user_db.reset_index(drop=True)

    info_user_bc = bc_obj.get_list_all_users()
    info_user_bc = info_user_bc.sort_values(
        by=[db_obj.db_param.BALANCE_ACCOUNT, db_obj.db_param.ID_USER, db_obj.db_param.T_UPDATE_BALANCE])
    info_user_bc = info_user_bc.reset_index(drop=True)

    pd.testing.assert_frame_equal(info_user_db, info_user_bc)
