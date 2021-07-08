import pytest
from lemlab.db_connection import db_param
from lemlab.platform.blockchain_tests import test_utils
from lemlab.platform.lem import _aggregate_identical_positions, _convert_qualities_to_int


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

#this method is executed before all the others, to get useful global variables, needed for the tests
@pytest.fixture(scope="session", autouse=True)
def setUp():
    global offers_blockchain_archive, bids_blockchain_archive, open_offers_blockchain, open_bids_blockchain, \
        offers_db_archive, bids_db_archive, open_offers_db, open_bids_db, user_infos_blockchain, user_infos_db, \
        id_meters_blockchain, id_meters_db, config, quality_energy, price_index, db_obj, bc_obj
    offers_blockchain_archive, bids_blockchain_archive, open_offers_blockchain, open_bids_blockchain, offers_db_archive, \
    bids_db_archive, open_offers_db, open_bids_db, user_infos_blockchain, user_infos_db, id_meters_blockchain, \
    id_meters_db, config, quality_energy, price_index, db_obj, bc_obj = test_utils.setUp_test(generate_bids_offer)

#this test, tests that for every ts_delivery, the corresponding positions are the same, in the same order.
#One has to apply also aggregation and sort
def test_filtering_on_ts_delivery():
    #t_now = round(time.time())
    t_now = 1592791800
    market_horizon = config['lem']['horizon_clearing']
    interval_clearing = config['lem']['interval_clearing']
    # Calculate number of market clearings
    n_clearings = int(market_horizon / interval_clearing)
    print("n_clearings: " + str(n_clearings))
    t_clearing_first = t_now - (t_now % interval_clearing) + interval_clearing

    iterations = range(1, n_clearings + 1)

    for i in iterations:
        print("i: " + str(i))

        t_clearing_current = t_clearing_first + interval_clearing * i
        print("t_clearing_current: " + str(t_clearing_current))

        curr_clearing_offers_db = open_offers_db[open_offers_db[db_param.TS_DELIVERY] == t_clearing_current]
        curr_clearing_bids_db = open_bids_db[open_bids_db[db_param.TS_DELIVERY] == t_clearing_current]

        # Aggregate equal positions
        if not curr_clearing_bids_db.empty:
            curr_clearing_bids_db = _aggregate_identical_positions(db_obj=db_obj,
                                                                   positions=curr_clearing_bids_db,
                                                                   subset=[db_obj.db_param.PRICE_ENERGY,
                                                                           db_obj.db_param.QUALITY_ENERGY,
                                                                           db_obj.db_param.ID_USER])
        if not curr_clearing_offers_db.empty:
            curr_clearing_offers_db = _aggregate_identical_positions(db_obj=db_obj,
                                                                     positions=curr_clearing_offers_db,
                                                                     subset=[db_obj.db_param.PRICE_ENERGY,
                                                                             db_obj.db_param.QUALITY_ENERGY,
                                                                             db_obj.db_param.ID_USER])

        curr_clearing_offers_db = _convert_qualities_to_int(db_obj, curr_clearing_offers_db, config['lem']['types_quality'])
        curr_clearing_offers_db = curr_clearing_offers_db.sort_values(
            by=[db_param.PRICE_ENERGY, db_param.QUALITY_ENERGY, db_param.QTY_ENERGY],
            ascending=[True, False, False])
        curr_clearing_bids_db = _convert_qualities_to_int(db_obj, curr_clearing_bids_db,
                                                            config['lem']['types_quality'])
        curr_clearing_bids_db = curr_clearing_bids_db.sort_values(by=[db_param.PRICE_ENERGY, db_param.QUALITY_ENERGY, db_param.QTY_ENERGY],
                                                                  ascending=[False, False, False])

        curr_clearing_offers_db, curr_clearing_bids_db = [tuple(x) for x in curr_clearing_offers_db.values.tolist()], [tuple(x) for x in curr_clearing_bids_db.values.tolist()]

        print("len curr_clearing_offers_db: " + str(len(curr_clearing_offers_db)))
        print("len curr_clearing_bids_db: " + str(len(curr_clearing_bids_db)))
        curr_clearing_offers_blockchain, curr_clearing_bids_blockchain = bc_obj.functions.filter_sort_aggregate_OffersBids_memory(
            t_clearing_current, True).call()

        assert curr_clearing_offers_blockchain == curr_clearing_offers_db
        assert curr_clearing_bids_blockchain == curr_clearing_bids_db
