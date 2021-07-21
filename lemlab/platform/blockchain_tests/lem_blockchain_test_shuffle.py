import pytest

from lemlab.platform.blockchain_tests import test_utils
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


# I test if the shuffling produces different orders of values, but the same set of values
@pytest.fixture(scope="session", autouse=True)
def setUp():
    global offers_blockchain_archive, bids_blockchain_archive, open_offers_blockchain, open_bids_blockchain, \
        offers_db_archive, bids_db_archive, open_offers_db, open_bids_db, user_infos_blockchain, user_infos_db, \
        id_meters_blockchain, id_meters_db, config, quality_energy, price_index, db_obj, bc_obj
    offers_blockchain_archive, bids_blockchain_archive, open_offers_blockchain, open_bids_blockchain, offers_db_archive, \
    bids_db_archive, open_offers_db, open_bids_db, user_infos_blockchain, user_infos_db, id_meters_blockchain, \
    id_meters_db, config, quality_energy, price_index, db_obj, bc_obj = test_utils.setUp_test(generate_bids_offer)


def test_shuffling():
    # test_utils.result_test(__file__, False)
    offers_blockchain = open_offers_blockchain
    bids_blockchain = open_bids_blockchain

    lib_dict = config['db_connections']['bc_dict']
    lib_dict["contract_name"] = "LemLib"
    bc_obj_lib = BlockchainConnection(lib_dict)  # in this case we call the Lib contract for its shuffle functions
    offers_blockchain_shuffled = bc_obj_lib.functions.shuffle_OfferBids(offers_blockchain).call()
    bids_blockchain_shuffled = bc_obj_lib.functions.shuffle_OfferBids(bids_blockchain).call()

    assert len(offers_blockchain) == len(offers_blockchain_shuffled) and len(bids_blockchain) == len(
        bids_blockchain_shuffled)
    assert offers_blockchain != offers_blockchain_shuffled
    assert bids_blockchain != bids_blockchain_shuffled
    assert set([tuple(x) for x in offers_blockchain]) == set([tuple(x) for x in offers_blockchain_shuffled])
    assert set([tuple(x) for x in bids_blockchain]) == set([tuple(x) for x in bids_blockchain_shuffled])
    # test_utils.result_test(__file__, True)
