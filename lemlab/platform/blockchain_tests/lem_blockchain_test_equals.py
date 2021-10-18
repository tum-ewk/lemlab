import pytest

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
quality_energy=None



# this method is executed before all the others, to get useful global variables, needed for the tests
@pytest.fixture(scope="session", autouse=True)
def setUp():
    global offers_blockchain_archive, bids_blockchain_archive, open_offers_blockchain, open_bids_blockchain, \
        offers_db_archive, bids_db_archive, open_offers_db, open_bids_db, user_infos_blockchain, user_infos_db, \
        id_meters_blockchain, id_meters_db, config, quality_energy, price_index, db_obj, bc_obj
    offers_blockchain_archive, bids_blockchain_archive, open_offers_blockchain, open_bids_blockchain, offers_db_archive, \
    bids_db_archive, open_offers_db, open_bids_db, user_infos_blockchain, user_infos_db, id_meters_blockchain, \
    id_meters_db, config, quality_energy, price_index, db_obj, bc_obj = test_utils.setUp_test(generate_bids_offer)


# tests same sets of position on db and blockchain
def test_equals_offers_bids():
    # test_utils.result_test(__file__, False)
    open_offs_db, open_bds_db = [tuple(x) for x in _convert_qualities_to_int(db_obj, open_offers_db, config['lem'][
        'types_quality']).values.tolist()], [tuple(x) for x in _convert_qualities_to_int(db_obj, open_bids_db,
                                                                                         config['lem'][
                                                                                             'types_quality']).values.tolist()]
    offs_db_archive, bds_db_archive = [tuple(x) for x in _convert_qualities_to_int(db_obj, offers_db_archive,
                                                                                   config['lem'][
                                                                                       'types_quality']).values.tolist()], [
                                          tuple(x) for x in _convert_qualities_to_int(db_obj, bids_db_archive,
                                                                                      config['lem'][
                                                                                          'types_quality']).values.tolist()]

    assert len(open_offs_db) == len(open_offers_blockchain)
    assert len(open_bds_db) == len(open_bids_blockchain)

    assert set(open_offs_db) == set(open_offers_blockchain)
    assert set(open_bds_db) == set(open_bids_blockchain)

    '''assert len(offs_db_archive) == len(offers_blockchain_archive)
    assert len(bds_db_archive) == len(bids_blockchain_archive)

    assert set(offs_db_archive) == set(offers_blockchain_archive)
    assert set(bds_db_archive) == set(bids_blockchain_archive)'''


# tests same sets of user infos on db and blockchain
def test_equals_user_infos():
    assert len(user_infos_blockchain) == len(user_infos_db)
    user_infos_db_list = [tuple(user_infos_db[i].values.tolist()[0]) for i in range(len(user_infos_db))]
    assert set(user_infos_blockchain) == set(user_infos_db_list)


# tests same sets of id_meters on db and blockchain
def test_equals_id_meters():
    global id_meters_db
    assert len(id_meters_blockchain) == len(id_meters_db)
    id_meters_db_list = [tuple(x) for x in id_meters_db.values.tolist()]
    assert set(id_meters_blockchain) == set(id_meters_db_list)
    # test_utils.result_test(__file__, True)
