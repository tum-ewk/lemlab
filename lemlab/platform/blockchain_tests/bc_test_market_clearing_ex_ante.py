import pytest
import pandas as pd
from lemlab.db_connection import db_param
from lemlab.platform.blockchain_tests import test_utils

config = None
db_obj = None
bc_obj_clearing_ex_ante = None


# this method is executed before all the others, to get useful global variables, needed for the tests
@pytest.fixture(scope="session", autouse=True)
def setup():
    global config, db_obj, bc_obj_clearing_ex_ante
    config, db_obj, bc_obj_clearing_ex_ante, _ = test_utils.setup_clearing_ex_ante_test(generate_random_test_data=True)


def test_clearing_results_ex_ante():
    # Get market results from db and bc
    clearing_ex_ante_results_db, _ = db_obj.get_results_market_ex_ante()
    clearing_ex_ante_results_bc = bc_obj_clearing_ex_ante.get_market_results()

    # Sort market results and reset indices
    clearing_ex_ante_results_bc = clearing_ex_ante_results_bc.sort_values(
        by=[db_param.TS_DELIVERY, db_param.QTY_ENERGY_TRADED, db_param.PRICE_ENERGY_OFFER],
        ascending=[True, True, True])
    clearing_ex_ante_results_db = clearing_ex_ante_results_db.sort_values(
        by=[db_param.TS_DELIVERY, db_param.QTY_ENERGY_TRADED, db_param.PRICE_ENERGY_OFFER],
        ascending=[True, True, True])
    clearing_ex_ante_results_bc = clearing_ex_ante_results_bc.reset_index(drop=True)
    clearing_ex_ante_results_db = clearing_ex_ante_results_db.reset_index(drop=True)
    clearing_ex_ante_results_bc = clearing_ex_ante_results_bc.reindex(sorted(clearing_ex_ante_results_bc.columns), axis=1)
    clearing_ex_ante_results_db = clearing_ex_ante_results_db.reindex(sorted(clearing_ex_ante_results_db.columns), axis=1)

    # Check whether market results are equal on db and bc
    pd.testing.assert_frame_equal(clearing_ex_ante_results_bc, clearing_ex_ante_results_db, check_dtype=False)

    # Get user infos from db and bc
    info_user_db = db_obj.get_info_user()
    info_user_db = info_user_db.sort_values(
        by=[db_obj.db_param.BALANCE_ACCOUNT, db_obj.db_param.ID_USER, db_obj.db_param.T_UPDATE_BALANCE])
    info_user_db = info_user_db.reset_index(drop=True)

    info_user_bc = bc_obj_clearing_ex_ante.get_list_all_users()
    info_user_bc = info_user_bc.sort_values(
        by=[db_obj.db_param.BALANCE_ACCOUNT, db_obj.db_param.ID_USER, db_obj.db_param.T_UPDATE_BALANCE])
    info_user_bc = info_user_bc.reset_index(drop=True)

    # Check whether balances are equal after market clearing on db and bc
    pd.testing.assert_frame_equal(info_user_db, info_user_bc)
