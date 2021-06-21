import time
import pandas as pd

from lemlab.db_connection import db_param
from lemlab.platform import blockchain_utils
from lemlab.platform.blockchain_tests import test_utils
from lemlab.platform.blockchain_tests.lem_blockchain_test_market_clearing_full import get_market_results_blockchain
from lemlab.platform.blockchain_utils import convertListToPdDataFrame

generate_bids_offer = False
shuffle = False
verbose_db = False
supplier_bids = False
uniform_pricing = True
discriminative_pricing = True
timeout_blockchain = 43200#12 hours
pause = 10
global offers_blockchain, bids_blockchain, offers_db, bids_db, user_infos_blockchain, user_infos_db, id_meters_blockchain, id_meters_db, config, quality_energy, price_index, db_obj
if __name__ == '__main__':
    #Getting the initial variables, and starting a while loop which runs the market clearing on the blockchain and on python. A difficult thing is to retrieve the data
    offers_blockchain, bids_blockchain, offers_db, bids_db, user_infos_blockchain, user_infos_db, id_meters_blockchain, id_meters_db, config, quality_energy, price_index, db_obj = test_utils.setUp_test(generate_bids_offer, timeout=timeout_blockchain)
    to_process_offers_bids = len(offers_blockchain)+len(bids_blockchain)
    t_submissions = list(set(bids_db['t_submission'].unique()).union(set(offers_db['t_submission'].unique())))
    t_submissions.sort(reverse = False)
    offers_blockchain_df = convertListToPdDataFrame(offers_blockchain, offers_db.columns)
    bids_blockchain_df = convertListToPdDataFrame(bids_blockchain, bids_db.columns)
    for t_sub in t_submissions:
        blockchain_utils.clearTempData()
        offers_blockchain_df_t_sub = offers_blockchain_df[offers_blockchain_df[db_obj.db_param.T_SUBMISSION] == t_sub]
        bids_blockchain_df_t_sub = bids_blockchain_df[bids_blockchain_df[db_obj.db_param.T_SUBMISSION] == t_sub]

        i = 1
        for ob in pd.concat([offers_blockchain_df, bids_blockchain_df]).iterrows():
            blockchain_utils.functions.pushOfferOrBid(tuple(ob[1].values), ob[1][list(ob[1].keys()).index(db_param.TYPE_POSITION)] == 'offer', True).transact({'from': blockchain_utils.coinbase})
            if i % 10 == 0:
                print("pushed " + str(i) + " offers/bids")
            i += 1
        market_horizon = config['lem']['horizon_clearing']
        interval_clearing = config['lem']['interval_clearing']
        # Calculate number of market clearings
        n_clearings = int(market_horizon / interval_clearing)

        #market_results_python = lem.market_clearing(db_obj=db_obj, config_lem=config['lem'], t_override=t_sub,
        #                                            shuffle=shuffle, verbose=verbose_db)
        #market_results_python = market_results_python[0]['da']
        ts_delivery_first = t_sub
        ts_delivery_last = t_sub + interval_clearing * n_clearings
        market_results_python_total = db_obj.get_results_market_ex_ante()[0]
        market_results_python_t_sub = db_obj.get_results_market_ex_ante(ts_delivery_first = ts_delivery_first, ts_delivery_last = ts_delivery_last)[0]
        market_results_blockchain = get_market_results_blockchain(t_sub, n_clearings, supplier_bids=supplier_bids,
                                                                  uniform_pricing=uniform_pricing,
                                                                  discriminative_pricing=discriminative_pricing,
                                                                  t_clearing_start=t_sub,
                                                                  market_results_python=market_results_python_t_sub,
                                                                  interval_clearing=interval_clearing,
                                                                  shuffle=shuffle)
        tx_hash = blockchain_utils.functions.updateBalances().transact({'from': blockchain_utils.coinbase})
        blockchain_utils.web3_instance.eth.waitForTransactionReceipt(tx_hash)
        time.sleep(pause)
