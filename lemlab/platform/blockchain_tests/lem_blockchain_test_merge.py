import pytest
import numpy as np
from tqdm import tqdm

from lemlab.db_connection import db_param
from lemlab.platform.blockchain_tests import test_utils
from lemlab.platform.lem import _aggregate_identical_positions, \
    _convert_qualities_to_int

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

verbose = False

#this method is executed before all the others, to get useful global variables, needed for the tests
@pytest.fixture(scope="session", autouse=True)
def setUp():
    global offers_blockchain_archive, bids_blockchain_archive, open_offers_blockchain, open_bids_blockchain, \
        offers_db_archive, bids_db_archive, open_offers_db, open_bids_db, user_infos_blockchain, user_infos_db, \
        id_meters_blockchain, id_meters_db, config, quality_energy, price_index, db_obj, bc_obj
    offers_blockchain_archive, bids_blockchain_archive, open_offers_blockchain, open_bids_blockchain, offers_db_archive, \
    bids_db_archive, open_offers_db, open_bids_db, user_infos_blockchain, user_infos_db, id_meters_blockchain, \
    id_meters_db, config, quality_energy, price_index, db_obj, bc_obj = test_utils.setUp_test(generate_bids_offer)

#this test, tests that the operation of merge produces the same results on blockchain and python, for each ts_delivery
def test_merge():
    # t_now = round(time.time())
    t_now = 1592791800
    market_horizon = config['lem']['horizon_clearing']
    interval_clearing = config['lem']['interval_clearing']
    # Calculate number of market clearings
    n_clearings = int(market_horizon / interval_clearing)
    print("n_clearings: " + str(n_clearings))

    # Set first clearing interval to next clearing interval period (ceil up to next clearing interval)
    t_clearing_first = t_now - (t_now % config['lem']['interval_clearing']) + config['lem']['interval_clearing']

    # Go through all specified number of clearings
    if verbose:
        iterations = tqdm(range(1, n_clearings + 1))
    else:
        iterations = range(1, n_clearings + 1)

    global open_bids_db, open_offers_db
    open_bids_db = _convert_qualities_to_int(db_obj, open_bids_db, config['lem']['types_quality'])
    open_offers_db = _convert_qualities_to_int(db_obj, open_offers_db, config['lem']['types_quality'])

    for i in iterations:
        print("i: " + str(i))
        t_clearing_current = t_clearing_first + config['lem']['interval_clearing'] * i
        merge_python = apply_merge_python(t_clearing_current)
        curr_clearing_offers_blockchain, curr_clearing_bids_blockchain = bc_obj.functions.filter_sort_aggregate_OffersBids_memory(
            t_clearing_current, True).call()
        merge_blockchain = bc_obj.functions.merge_offers_bids_memory(curr_clearing_offers_blockchain,
                                                                               curr_clearing_bids_blockchain).call()
        # assert merge_blockchain == merge_blockchain_second
        assert (merge_python is None and len(merge_blockchain) == 0) or (len(merge_blockchain) == len(merge_python))
        if len(merge_blockchain) > 0:
            merge_blockchain_reformatted = [list(x[:19]) + [x[9]] for x in merge_blockchain]
            assert merge_python.values.tolist() == merge_blockchain_reformatted


def apply_merge_python(t_clearing_current):
    positions_cleared = None
    curr_clearing_offers_db = open_offers_db[open_offers_db[db_param.TS_DELIVERY] == t_clearing_current]
    curr_clearing_bids_db = open_bids_db[open_bids_db[db_param.TS_DELIVERY] == t_clearing_current]

    if not curr_clearing_offers_db.empty and not curr_clearing_bids_db.empty:

        curr_clearing_bids_db = curr_clearing_bids_db[curr_clearing_bids_db[db_obj.db_param.QTY_ENERGY] > 0]
        curr_clearing_offers_db = curr_clearing_offers_db[curr_clearing_offers_db[db_obj.db_param.QTY_ENERGY] > 0]

        # Aggregate equal positions
        curr_clearing_bids_db = _aggregate_identical_positions(db_obj=db_obj,
                                                               positions=curr_clearing_bids_db,
                                                               subset=[db_obj.db_param.PRICE_ENERGY,
                                                                       db_obj.db_param.QUALITY_ENERGY,
                                                                       db_obj.db_param.ID_USER])
        curr_clearing_offers_db = _aggregate_identical_positions(db_obj=db_obj,
                                                                 positions=curr_clearing_offers_db,
                                                                 subset=[db_obj.db_param.PRICE_ENERGY,
                                                                         db_obj.db_param.QUALITY_ENERGY,
                                                                         db_obj.db_param.ID_USER])

        offers_sorted = curr_clearing_offers_db.sort_values(
            by=[db_obj.db_param.PRICE_ENERGY, db_obj.db_param.QUALITY_ENERGY, db_param.QTY_ENERGY],
            ascending=[True, False, False],
            ignore_index=True)
        bids_sorted = curr_clearing_bids_db.sort_values(
            by=[db_obj.db_param.PRICE_ENERGY, db_obj.db_param.QUALITY_ENERGY, db_param.QTY_ENERGY],
            ascending=[False, False, False],
            ignore_index=True)
        # Set index of bids and offers to cumulated energy qty sums
        bids_sorted.set_index(bids_sorted[db_obj.db_param.QTY_ENERGY].cumsum(), inplace=True)
        offers_sorted.set_index(offers_sorted[db_obj.db_param.QTY_ENERGY].cumsum(), inplace=True)
        # Merge bids and offers
        positions_merged = offers_sorted.merge(bids_sorted, how='outer', left_index=True, right_index=True,
                                               indicator=False,
                                               suffixes=[db_obj.db_param.EXTENSION_OFFER,
                                                         db_obj.db_param.EXTENSION_BID]).fillna(
            method='backfill')

        # Extract merged bids and offers for which offer price is lower or equal to bid price
        positions_cleared = positions_merged[
            positions_merged[db_obj.db_param.PRICE_ENERGY + db_obj.db_param.EXTENSION_OFFER] <=
            positions_merged[db_obj.db_param.PRICE_ENERGY + db_obj.db_param.EXTENSION_BID]].copy()

        # Convert floats (occur due to merging with NaN rows) to ints
        for column in positions_cleared.columns:
            if positions_cleared[column].dtype == np.float64:
                positions_cleared[column] = positions_cleared[column].astype(int)

    return positions_cleared
