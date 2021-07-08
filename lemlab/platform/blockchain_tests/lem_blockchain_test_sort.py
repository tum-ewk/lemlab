import pytest
import time
from lemlab.db_connection import db_param
from lemlab.bc_connection.bc_connection import BlockchainConnection
from lemlab.platform.blockchain_tests import test_utils
from lemlab.platform.lem import _convert_qualities_to_int

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
    offers_blockchain_archive, bids_blockchain_archive, open_offers_blockchain, open_bids_blockchain, offers_db_archive, \
    bids_db_archive, open_offers_db, open_bids_db, user_infos_blockchain, user_infos_db, id_meters_blockchain, \
    id_meters_db, config, quality_energy, price_index, db_obj, bc_obj = test_utils.setUp_test(generate_bids_offer)


def test_sorting():
    global open_offers_db
    open_offers_db = _convert_qualities_to_int(db_obj, open_offers_db, config['lem']['types_quality'])
    global open_bids_db
    open_bids_db = _convert_qualities_to_int(db_obj, open_bids_db, config['lem']['types_quality'])
    start = time.time()
    sorted_offers_db = open_offers_db.sort_values(
        by=[db_param.PRICE_ENERGY, db_param.QUALITY_ENERGY, db_param.QTY_ENERGY],
        ascending=[True, False, False])
    end = time.time()
    print("offers_python_sorted done in " + str(end - start) + "seconds")
    start = time.time()
    sorted_bids_db = open_bids_db.sort_values(by=[db_param.PRICE_ENERGY, db_param.QUALITY_ENERGY, db_param.QTY_ENERGY],
                                              ascending=[True, False, False])
    end = time.time()
    print("bids_python_sorted done in " + str(end - start) + "seconds")
    sorted_offers_db_list, sorted_bids_db_list = [tuple(x) for x in sorted_offers_db.values.tolist()], [tuple(x) for x
                                                                                                        in
                                                                                                        sorted_bids_db.values.tolist()]

    # blockchain_utils.setUpBlockchain(contract_name="Sorting")
    sorting_lib = config['db_connections']['bc_dict']
    sorting_lib['contract_name'] = "Sorting"
    bc_obj_sort = BlockchainConnection(sorting_lib)

    start = time.time()
    offers_blockchain_sorted_three_keys = bc_obj_sort.functions.insertionSortOffersBidsPrice_Quality(
        open_offers_blockchain, True, False, True,
        False).call()
    end = time.time()
    print("offers_blockchain_sorted done in " + str(end - start) + "seconds")

    start = time.time()
    bids_blockchain_sorted_three_keys = bc_obj_sort.functions.insertionSortOffersBidsPrice_Quality(
        open_bids_blockchain, True, False, True, False).call()
    end = time.time()
    print("bids_blockchain_sorted done in " + str(end - start) + "seconds")
    prices_offers_blockchain = [x[price_index] for x in offers_blockchain_sorted_three_keys]
    prices_bids_blockchain = [x[price_index] for x in bids_blockchain_sorted_three_keys]

    prices_db_offers = [x[price_index] for x in sorted_offers_db_list]  # ts_deliveries
    prices_db_bids = [x[price_index] for x in sorted_bids_db_list]  # ts_deliveries

    # assert that the offers and lists are sorted on the blockchain
    assert sorted(prices_offers_blockchain) == prices_offers_blockchain and sorted(
        prices_bids_blockchain) == prices_bids_blockchain
    # assert that the lists of ts_deliveries are in the same order on db and blockchain
    assert prices_db_offers == prices_offers_blockchain and prices_db_bids == prices_bids_blockchain

    # control if the two lists are sorted in the same way and with the same values
    assert [x[1:4] for x in offers_blockchain_sorted_three_keys] == [x[1:4] for x in sorted_offers_db_list]
    assert [x[1:4] for x in bids_blockchain_sorted_three_keys] == [x[1:4] for x in sorted_bids_db_list]
