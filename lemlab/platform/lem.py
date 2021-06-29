"""
The lem module contains all functions related to the market clearing.
"""

__author__ = "Michel Zadé"
__copyright__ = "Copyright 2020, RegHEE"
__credits__ = []
__license__ = ""
__version__ = "1.0"
__maintainer__ = "Michel Zadé"
__email__ = "michel.zade@tum.de"
__status__ = "Development"

from lemlab.db_connection import db_connection, db_param
from collections import OrderedDict
from tqdm import tqdm
import time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import traceback
import yaml
import random
import string

from lemlab.platform import blockchain_utils

from current_scenario_file import scenario_file_path

# variable for the blockchain function calls
verbose_blockchain = False

def market_clearing(db_obj,
                    config_lem,
                    config_supplier=None,
                    t_override=None,
                    shuffle=False,
                    plotting=False,
                    verbose=False):
    """
    Function clears all offers and bids from database and writes stores unmatched and matched bids back in database.
    @param db_obj: database connection object
    @param config_lem: configuration dictionary of local energy market
    @param config_supplier: configuration dictionary of supplier
    @param t_override: defines the current time, mostly used for simulation purpose [unix time]
    @param plotting: boolean value to visualize clearing results
    @param verbose: boolean value to print updates to console
    """
    # If t_now is not set, then current time
    if t_override is None:
        t_now = round(time.time())
    else:
        t_now = t_override

    # If market horizon is not set, then set to lem_param
    if config_lem['horizon_clearing'] is None:
        config_lem['horizon_clearing'] = 900

    # Check whether clearing types have been specified
    if config_lem['types_clearing_ex_ante'] is None:
        config_lem['types_clearing_ex_ante'] = {0: "da"}

    # Calculate number of market clearings
    n_clearings = int(config_lem['horizon_clearing'] / config_lem['interval_clearing'])
    # Read offers and bids from db
    bids, offers = db_obj.get_open_positions(clear_table=config_lem['positions_delete'],
                                             archive=config_lem['positions_archive'])

    # Jump to end of function if offers or bids are empty
    if offers.empty or bids.empty:
        if verbose:
            print('All offers and/or bids are empty. No clearing possible')
        return

    bids = _convert_qualities_to_int(db_obj, bids, config_lem['types_quality'])
    offers = _convert_qualities_to_int(db_obj, offers, config_lem['types_quality'])
    # if we want to perform the market clearing on the blockchain too
    if config_lem["clearing_blockchain"]:
        simulation_test = False  # if true, we sort the positions by price, quality, and quantity too, in order to test the results with python
        blockchain_utils.clearTempData()  # clear temporary positions and temporary market results
        i = 1
        for ob in pd.concat([offers,
                             bids]).iterrows():  # I take positions(offers, bids) from db and push them on the blockchain one by one
            blockchain_utils.functions.pushOfferOrBid(tuple(ob[1].values),
                                                      ob[1][
                                                          list(ob[1].keys()).index(db_param.TYPE_POSITION)] == 'offer',
                                                      True, config_lem['positions_archive']).transact(
                {'from': blockchain_utils.coinbase})
            if verbose:
                if i % 10 == 0:
                    print("pushed " + str(i) + " offers/bids")
            i += 1
        time.sleep(20)
        if verbose:
            print("same len positions on db and on blockchain: " + str(
                len(blockchain_utils.getOffers_or_Bids(isOffer=True)) + len(
                    blockchain_utils.getOffers_or_Bids(isOffer=False)) == len(bids) + len(offers)))
        # I perform the market clearing on the blockchain and get the results
        market_results_blockchain = get_market_results_blockchain(t_now, n_clearings,
                                                                  supplier_bids=config_supplier is not None,
                                                                  uniform_pricing=True,
                                                                  discriminative_pricing=True,
                                                                  t_clearing_start=t_now,
                                                                  interval_clearing=config_lem['interval_clearing'],
                                                                  simulation_test=simulation_test, shuffle=shuffle)
        # I update the user balances
        tx_hash = blockchain_utils.functions.updateBalances().transact({'from': blockchain_utils.coinbase})
        # added extra timeout up to 5 mins in case the connection fails or there is a block error
        blockchain_utils.web3_instance.eth.waitForTransactionReceipt(tx_hash, timeout=300)
    results_clearing_all = {}
    time_clearing_execution = {}

    # for-loop for all specified clearing types
    for j in range(len(config_lem['types_clearing_ex_ante'])):
        type_clearing = config_lem['types_clearing_ex_ante'][j]
        # Set clearing time
        # t_clearing_start = round(time.time())
        t_clearing_start = t_now
        if verbose:
            print('\n\n### MARKET CLEARING STARTED ###', pd.Timestamp(t_clearing_start, unit="s", tz="Europe/Berlin"))
            print(f'Market type: {type_clearing}')
            print('Market contains', str(len(offers)), 'valid offers and', str(len(bids)), 'valid bids.')
        # Create empty results df
        results_clearing = pd.DataFrame()
        positions_cleared = pd.DataFrame()

        # Set first clearing interval to next clearing interval period (ceil up to next clearing interval)
        t_clearing_first = t_now - (t_now % config_lem['interval_clearing']) + config_lem['interval_clearing']

        # Go through all specified number of clearings
        if verbose:
            iterations = tqdm(range(0, n_clearings))
        else:
            iterations = range(0, n_clearings)
        for i in iterations:
            # Continuous clearing time, incrementing by market period
            t_clearing_current = t_clearing_first + config_lem['interval_clearing'] * i
            # Extract data for specific time of delivery
            offers_ts_d = offers[offers[db_obj.db_param.TS_DELIVERY] == t_clearing_current]
            bids_ts_d = bids[bids[db_obj.db_param.TS_DELIVERY] == t_clearing_current]
            # t_d_last_update = get_update_time(bids_sorted, offers_sorted)

            # Check whether offers or bids are empty or last update in offers/bids is newer than last clearing time
            if offers_ts_d.empty or bids_ts_d.empty:  # or t_d_last_update < t_clearing_start
                if verbose:
                    iterations.set_description(str(pd.Timestamp(t_clearing_current, unit="s", tz="Europe/Berlin")) +
                                               ' No clearing - supply and/or bids are empty')
            # Offers and bids are not empty and last update in offers/bids is newer than last clearing time
            else:
                if verbose:
                    iterations.set_description(str(pd.Timestamp(t_clearing_current, unit="s", tz="Europe/Berlin")) +
                                               ' Clearing                                  ')
                # Check whether this is the first clearing period and whether the flag supplier bids is true
                if config_supplier is not None:
                    # Insert supplier bids and offers
                    bids_ts_d, offers_ts_d = _add_supplier_bids(db_obj,
                                                                config_supplier,
                                                                t_clearing_current,
                                                                bids_ts_d,
                                                                offers_ts_d)

                if 'da' == type_clearing:
                    positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                        clearing_da(db_obj,
                                    config_lem,
                                    offers_ts_d,
                                    bids_ts_d,
                                    plotting=plotting,
                                    plotting_title=pd.Timestamp(t_clearing_current, unit="s", tz="Europe/Berlin"),
                                    shuffle=shuffle)

                if 'pref_prio_n_to_0' == type_clearing:
                    positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                        clearing_pref_prio(db_obj,
                                           config_lem,
                                           offers_ts_d,
                                           bids_ts_d,
                                           type_prioritization='pref_n_to_0',
                                           plotting=plotting,
                                           plotting_title=pd.Timestamp(t_clearing_current,
                                                                       unit="s", tz="Europe/Berlin"))

                if 'pref_prio_0_to_n' == type_clearing:
                    positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                        clearing_pref_prio(db_obj,
                                           config_lem,
                                           offers_ts_d,
                                           bids_ts_d,
                                           type_prioritization='pref_0_to_n',
                                           plotting=plotting,
                                           plotting_title=pd.Timestamp(t_clearing_current,
                                                                       unit="s", tz="Europe/Berlin"))
                if 'pref_separation' == type_clearing:
                    positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                        clearing_pref_prio(db_obj,
                                           config_lem,
                                           offers_ts_d,
                                           bids_ts_d,
                                           type_prioritization='pref_separation',
                                           plotting=plotting,
                                           plotting_title=pd.Timestamp(t_clearing_current,
                                                                       unit="s", tz="Europe/Berlin"))

                if 'pref_prio_n_to_0_da' == type_clearing:
                    positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                        clearing_pref_prio(db_obj,
                                           config_lem,
                                           offers_ts_d,
                                           bids_ts_d,
                                           type_prioritization='pref_n_to_0',
                                           add_premium=True,
                                           plotting=plotting,
                                           plotting_title=pd.Timestamp(t_clearing_current,
                                                                       unit="s", tz="Europe/Berlin"))

                    positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                        clearing_da(db_obj,
                                    config_lem,
                                    offers_uncleared,
                                    bids_uncleared,
                                    add_premium=False,
                                    plotting=plotting,
                                    plotting_title=pd.Timestamp(t_clearing_current, unit="s", tz="Europe/Berlin"))

                    positions_cleared = positions_cleared.append(positions_cleared_da, ignore_index=True)

                if 'pref_prio_0_to_n_da' == type_clearing:
                    positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                        clearing_pref_prio(db_obj,
                                           config_lem,
                                           offers_ts_d,
                                           bids_ts_d,
                                           type_prioritization='pref_0_to_n',
                                           add_premium=True,
                                           plotting=plotting,
                                           plotting_title=pd.Timestamp(t_clearing_current,
                                                                       unit="s", tz="Europe/Berlin"))

                    positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                        clearing_da(db_obj,
                                    config_lem,
                                    offers_uncleared,
                                    bids_uncleared,
                                    add_premium=False,
                                    plotting=plotting,
                                    plotting_title=pd.Timestamp(t_clearing_current, unit="s", tz="Europe/Berlin"))

                    positions_cleared = positions_cleared.append(positions_cleared_da, ignore_index=True)

                if 'pref_separation_da' == type_clearing:
                    positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                        clearing_pref_prio(db_obj,
                                           config_lem,
                                           offers_ts_d,
                                           bids_ts_d,
                                           type_prioritization='pref_separation',
                                           add_premium=True,
                                           plotting=plotting,
                                           plotting_title=pd.Timestamp(t_clearing_current,
                                                                       unit="s", tz="Europe/Berlin"))

                    positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                        clearing_da(db_obj,
                                    config_lem,
                                    offers_uncleared,
                                    bids_uncleared,
                                    add_premium=False,
                                    plotting=plotting,
                                    plotting_title=pd.Timestamp(t_clearing_current, unit="s", tz="Europe/Berlin"))

                    positions_cleared = positions_cleared.append(positions_cleared_da, ignore_index=True)

                if 'da_pref_prio_n_to_0' == type_clearing:
                    positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                        clearing_da(db_obj,
                                    config_lem,
                                    offers_ts_d,
                                    bids_ts_d,
                                    add_premium=False,
                                    plotting=plotting,
                                    plotting_title=pd.Timestamp(t_clearing_current, unit="s", tz="Europe/Berlin"))

                    positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                        clearing_pref_prio(db_obj,
                                           config_lem,
                                           offers_uncleared_da,
                                           bids_uncleared_da,
                                           type_prioritization='pref_n_to_0',
                                           add_premium=True,
                                           plotting=plotting,
                                           plotting_title=pd.Timestamp(t_clearing_current,
                                                                       unit="s", tz="Europe/Berlin"))

                    positions_cleared = positions_cleared.append(positions_cleared_da, ignore_index=True)

                if 'da_pref_prio_0_to_n' == type_clearing:
                    positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                        clearing_da(db_obj,
                                    config_lem,
                                    offers_ts_d,
                                    bids_ts_d,
                                    add_premium=False,
                                    plotting=plotting,
                                    plotting_title=pd.Timestamp(t_clearing_current, unit="s", tz="Europe/Berlin"))

                    positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                        clearing_pref_prio(db_obj,
                                           config_lem,
                                           offers_uncleared_da,
                                           bids_uncleared_da,
                                           type_prioritization='pref_0_to_n',
                                           add_premium=True,
                                           plotting=plotting,
                                           plotting_title=pd.Timestamp(t_clearing_current,
                                                                       unit="s", tz="Europe/Berlin"))

                    positions_cleared = positions_cleared.append(positions_cleared_da, ignore_index=True)

                if 'da_pref_separation' == type_clearing:
                    positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                        clearing_da(db_obj,
                                    config_lem,
                                    offers_ts_d,
                                    bids_ts_d,
                                    add_premium=False,
                                    plotting=plotting,
                                    plotting_title=pd.Timestamp(t_clearing_current, unit="s", tz="Europe/Berlin"))

                    positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                        clearing_pref_prio(db_obj,
                                           config_lem,
                                           offers_uncleared_da,
                                           bids_uncleared_da,
                                           type_prioritization='pref_separation',
                                           add_premium=True,
                                           plotting=plotting,
                                           plotting_title=pd.Timestamp(t_clearing_current,
                                                                       unit="s", tz="Europe/Berlin"))

                    positions_cleared = positions_cleared.append(positions_cleared_da, ignore_index=True)

                if 'pref_satis_pref_n_to_0' == type_clearing:
                    positions_cleared, offers_uncleared, bids_uncleared = \
                        clearing_pref_satis(db_obj,
                                            config_lem,
                                            offers_ts_d,
                                            bids_ts_d,
                                            type_prioritization='pref_n_to_0',
                                            plotting=plotting,
                                            plotting_title=pd.Timestamp(
                                                t_clearing_current,
                                                unit="s",
                                                tz="Europe/Berlin"),
                                            verbose=verbose)

                if 'pref_satis_pref_0_to_n' == type_clearing:
                    positions_cleared, offers_uncleared, bids_uncleared = \
                        clearing_pref_satis(db_obj,
                                            config_lem,
                                            offers_ts_d,
                                            bids_ts_d,
                                            type_prioritization='pref_0_to_n',
                                            plotting=plotting,
                                            plotting_title=pd.Timestamp(t_clearing_current,
                                                                        unit="s",
                                                                        tz="Europe/Berlin"),
                                            verbose=verbose)

                if 'pref_satis_pref_separation' == type_clearing:
                    positions_cleared, offers_uncleared, bids_uncleared = \
                        clearing_pref_satis(db_obj,
                                            config_lem,
                                            offers_ts_d,
                                            bids_ts_d,
                                            type_prioritization='pref_separation',
                                            plotting=plotting,
                                            plotting_title=pd.Timestamp(t_clearing_current,
                                                                        unit="s",
                                                                        tz="Europe/Berlin"),
                                            verbose=verbose)

                if 'da_pref_satis_pref_n_to_0' == type_clearing:
                    positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                        clearing_da(db_obj,
                                    config_lem,
                                    offers_ts_d,
                                    bids_ts_d,
                                    add_premium=False,
                                    plotting=plotting,
                                    plotting_title=pd.Timestamp(t_clearing_current, unit="s", tz="Europe/Berlin"))

                    positions_cleared, offers_uncleared, bids_uncleared = \
                        clearing_pref_satis(db_obj,
                                            config_lem,
                                            offers_uncleared_da,
                                            bids_uncleared_da,
                                            type_prioritization='pref_n_to_0',
                                            add_premium=True,
                                            plotting=plotting,
                                            plotting_title=pd.Timestamp(
                                                t_clearing_current,
                                                unit="s",
                                                tz="Europe/Berlin"),
                                            verbose=verbose)

                    positions_cleared = positions_cleared.append(positions_cleared_da, ignore_index=True)

                if 'da_pref_satis_pref_0_to_n' == type_clearing:
                    positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                        clearing_da(db_obj,
                                    config_lem,
                                    offers_ts_d,
                                    bids_ts_d,
                                    add_premium=False,
                                    plotting=plotting,
                                    plotting_title=pd.Timestamp(t_clearing_current, unit="s", tz="Europe/Berlin"))

                    positions_cleared, offers_uncleared, bids_uncleared = \
                        clearing_pref_satis(db_obj,
                                            config_lem,
                                            offers_uncleared_da,
                                            bids_uncleared_da,
                                            type_prioritization='pref_0_to_n',
                                            add_premium=True,
                                            plotting=plotting,
                                            plotting_title=pd.Timestamp(
                                                t_clearing_current,
                                                unit="s",
                                                tz="Europe/Berlin"),
                                            verbose=verbose)

                    positions_cleared = positions_cleared.append(positions_cleared_da, ignore_index=True)

                if 'da_pref_satis_pref_separation' == type_clearing:
                    positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                        clearing_da(db_obj,
                                    config_lem,
                                    offers_ts_d,
                                    bids_ts_d,
                                    add_premium=False,
                                    plotting=plotting,
                                    plotting_title=pd.Timestamp(t_clearing_current, unit="s", tz="Europe/Berlin"))

                    positions_cleared, offers_uncleared, bids_uncleared = \
                        clearing_pref_satis(db_obj,
                                            config_lem,
                                            offers_uncleared_da,
                                            bids_uncleared_da,
                                            type_prioritization='pref_separation',
                                            add_premium=True,
                                            plotting=plotting,
                                            plotting_title=pd.Timestamp(
                                                t_clearing_current,
                                                unit="s",
                                                tz="Europe/Berlin"),
                                            verbose=verbose)

                    positions_cleared = positions_cleared.append(positions_cleared_da, ignore_index=True)

                if 'pref_satis_pref_n_to_0_da' == type_clearing:
                    positions_cleared, offers_uncleared, bids_uncleared = \
                        clearing_pref_satis(db_obj,
                                            config_lem,
                                            offers_ts_d,
                                            bids_ts_d,
                                            type_prioritization='pref_n_to_0',
                                            add_premium=True,
                                            plotting=plotting,
                                            plotting_title=pd.Timestamp(
                                                t_clearing_current,
                                                unit="s",
                                                tz="Europe/Berlin"),
                                            verbose=verbose)

                    positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                        clearing_da(db_obj,
                                    config_lem,
                                    offers_uncleared,
                                    bids_uncleared,
                                    add_premium=False,
                                    plotting=plotting,
                                    plotting_title=pd.Timestamp(t_clearing_current, unit="s", tz="Europe/Berlin"))

                    positions_cleared = positions_cleared.append(positions_cleared_da, ignore_index=True)

                if 'pref_satis_pref_0_to_n_da' == type_clearing:
                    positions_cleared, offers_uncleared, bids_uncleared = \
                        clearing_pref_satis(db_obj,
                                            config_lem,
                                            offers_ts_d,
                                            bids_ts_d,
                                            type_prioritization='pref_0_to_n',
                                            add_premium=True,
                                            plotting=plotting,
                                            plotting_title=pd.Timestamp(
                                                t_clearing_current,
                                                unit="s",
                                                tz="Europe/Berlin"),
                                            verbose=verbose)

                    positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                        clearing_da(db_obj,
                                    config_lem,
                                    offers_uncleared,
                                    bids_uncleared,
                                    add_premium=False,
                                    plotting=plotting,
                                    plotting_title=pd.Timestamp(t_clearing_current, unit="s", tz="Europe/Berlin"))

                    positions_cleared = positions_cleared.append(positions_cleared_da, ignore_index=True)

                if 'pref_satis_pref_separation_da' == type_clearing:
                    positions_cleared, offers_uncleared, bids_uncleared = \
                        clearing_pref_satis(db_obj,
                                            config_lem,
                                            offers_ts_d,
                                            bids_ts_d,
                                            type_prioritization='pref_separation',
                                            add_premium=True,
                                            plotting=plotting,
                                            plotting_title=pd.Timestamp(
                                                t_clearing_current,
                                                unit="s",
                                                tz="Europe/Berlin"),
                                            verbose=verbose)

                    positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                        clearing_da(db_obj,
                                    config_lem,
                                    offers_uncleared,
                                    bids_uncleared,
                                    add_premium=False,
                                    plotting=plotting,
                                    plotting_title=pd.Timestamp(t_clearing_current, unit="s", tz="Europe/Berlin"))

                    positions_cleared = positions_cleared.append(positions_cleared_da, ignore_index=True)

                # Check whether market has cleared a volume
                if not positions_cleared.empty:
                    results_clearing = results_clearing.append(positions_cleared, ignore_index=True)

        t_clearing_end = round(time.time())
        if verbose:
            iterations.set_description('Post-processing: ', pd.Timestamp(t_clearing_end, unit="s",
                                                                         tz="Europe/Berlin"))
        if results_clearing.empty and verbose:
            iterations.set_description('Empty market results: nothing has been cleared.')
        if not results_clearing.empty:
            # Perform a post-processing
            results_clearing = _post_processing_results(db_obj=db_obj, results=results_clearing,
                                                        t_clearing_start=t_clearing_start)
            # only update user balances if this is the first clearing type
            if j == 0:
                # Find column with relevant prices to update user balances
                name_column_price = \
                    [x for x in results_clearing.columns if config_lem['types_pricing_ex_ante'][0] in x][0]
                # Update user balances
                transactions_market = _log_transactions_market(db_obj=db_obj,
                                                               config_lem=config_lem,
                                                               results_market=results_clearing,
                                                               name_column_price=name_column_price,
                                                               types_quality=config_lem['types_quality'])
                _update_user_balances(db_obj=db_obj,
                                      df_transactions=transactions_market)
            # Write results back to database
            db_obj.log_results_market(results_market=results_clearing,
                                      name_table=db_obj.db_param.NAME_TABLE_RESULTS_MARKET_EX_ANTE_ + type_clearing)

        # save all results to dictionary
        results_clearing_all[type_clearing] = results_clearing

        # General Information
        t_post_processing_end = round(time.time())
        if verbose:
            print('\nTiming')
            print('Internal clearing time:', str(t_clearing_end - t_clearing_start))
            print('Post-processing time:', str(t_post_processing_end - t_clearing_end))
            print('Market clearing ended, total time:', str(t_post_processing_end - t_clearing_start), 'seconds')

        time_clearing_execution[type_clearing] = t_clearing_end - t_clearing_start

    if config_lem["clearing_blockchain"]:
        # here I compare the results from the blockchain with the results got using python
        market_results_blockchain = convertToPdFinalMarketResults(market_results_blockchain, results_clearing_all['da'])
        market_results_blockchain = market_results_blockchain.sort_values(
            by=[db_param.TS_DELIVERY, db_param.QTY_ENERGY_TRADED, db_param.PRICE_ENERGY_OFFER],
            ascending=[True, True, True])
        results_clearing_all['da'] = results_clearing_all['da'].sort_values(
            by=[db_param.TS_DELIVERY, db_param.QTY_ENERGY_TRADED, db_param.PRICE_ENERGY_OFFER],
            ascending=[True, True, True])
        # results_clearing = results_clearing_all['da']
        # b = market_results_blockchain[market_results_blockchain['id_user_offer']!='06m8761z85']
        market_results_blockchain = market_results_blockchain.reset_index(drop=True)
        # results_clearing.reset_index(drop=True)
        results_clearing_all['da'] = results_clearing_all['da'].reset_index(drop=True)
        if verbose:
            if len(market_results_blockchain) == len(results_clearing_all['da']):
                pd.testing.assert_frame_equal(results_clearing_all['da'], market_results_blockchain, check_dtype=False)
                print("successful comparison")
            else:
                print("error in comparison")
        return results_clearing_all, offers, bids, time_clearing_execution, market_results_blockchain
    return results_clearing_all, offers, bids, time_clearing_execution


def clearing_da(db_obj,
                config_lem,
                offers,
                bids,
                type_clearing=None,
                shuffle=True,
                add_premium=False,
                plotting=False,
                plotting_title=None,
                plotting_ylim=None):
    """
    Function clears offers and bids with a double sided auction
    @param db_obj: DatabaseConnection object
    @param config_lem: configuration dictionary for clearing
    @param offers: dataframe of various offers consisting of price, quantity, quality, ts_delivery, id and type
    @param bids: dataframe of various bids consisting of price, quantity, quality, ts_delivery, id and type
    @param type_clearing: clearing type that is using clearing da functionality
    @param shuffle: boolean value to shuffle bids and offers before clearing for fairness
    @param add_premium: boolean value to add premium to bid prices
    @param plotting: boolean value to plot clearing results
    @param plotting_title: title of plot, ignored if plotting is false
    @param plotting_ylim: list of two values to predefine limits of y axis
    @return: returns cleared and uncleared bids and offers in multiple dataframes
    """
    offers_uncleared = pd.DataFrame()
    bids_uncleared = pd.DataFrame()
    offers_cleared = pd.DataFrame()
    bids_cleared = pd.DataFrame()
    positions_cleared = pd.DataFrame()
    qty_energy_cleared = 0
    # Check whether bids or offers are empty
    if bids.empty or offers.empty or \
            bids[bids[db_obj.db_param.QTY_ENERGY] > 0].empty or \
            offers[offers[db_obj.db_param.QTY_ENERGY] > 0].empty:
        offers_uncleared = offers
        bids_uncleared = bids
        return positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared
    if type_clearing is None:
        type_clearing = 'da'
    # Exclude bids/offers if they have zero quantity
    bids = bids[bids[db_obj.db_param.QTY_ENERGY] > 0]
    offers = offers[offers[db_obj.db_param.QTY_ENERGY] > 0]
    # Aggregate equal positions
    bids = _aggregate_identical_positions(db_obj=db_obj,
                                          positions=bids,
                                          subset=[db_obj.db_param.PRICE_ENERGY, db_obj.db_param.QUALITY_ENERGY,
                                                  db_obj.db_param.ID_USER])
    offers = _aggregate_identical_positions(db_obj=db_obj,
                                            positions=offers,
                                            subset=[db_obj.db_param.PRICE_ENERGY, db_obj.db_param.QUALITY_ENERGY,
                                                    db_obj.db_param.ID_USER])
    if add_premium:
        bids[db_obj.db_param.PRICE_ENERGY] += (bids[db_obj.db_param.PRICE_ENERGY] *
                                               bids[db_obj.db_param.PREMIUM_PREFERENCE_QUALITY] / 100).astype(int)
    if shuffle:
        # Shuffle all bids and offers, so that submission speed does not matter
        bids = bids.sample(frac=1).reset_index(drop=True)
        offers = offers.sample(frac=1).reset_index(drop=True)
    try:
        # Sort values first by price and quality
        offers_sorted = offers.sort_values(by=[db_obj.db_param.PRICE_ENERGY, db_obj.db_param.QUALITY_ENERGY],
                                           ascending=[True, False],
                                           ignore_index=True)
        bids_sorted = bids.sort_values(by=[db_obj.db_param.PRICE_ENERGY, db_obj.db_param.QUALITY_ENERGY],
                                       ascending=[False, False],
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
        # Check whether cleared quantities are empty
        if not positions_cleared.empty:
            for i in range(len(config_lem['types_pricing_ex_ante'])):
                type_pricing = config_lem['types_pricing_ex_ante'][i]
                # Calculate uniform prices if demanded
                if 'uniform' == config_lem['types_pricing_ex_ante'][i]:
                    positions_cleared.loc[:, db_obj.db_param.PRICE_ENERGY_MARKET_ + type_pricing] = \
                        ((positions_cleared[db_obj.db_param.PRICE_ENERGY_OFFER].iloc[-1] +
                          positions_cleared[db_obj.db_param.PRICE_ENERGY_BID].iloc[-1]) / 2).astype(int)

                # Calculate discriminative prices if demanded
                if 'discriminatory' == config_lem['types_pricing_ex_ante'][i]:
                    positions_cleared.loc[:, db_obj.db_param.PRICE_ENERGY_MARKET_ + type_pricing] = \
                        ((positions_cleared[db_obj.db_param.PRICE_ENERGY_OFFER] +
                          positions_cleared[db_obj.db_param.PRICE_ENERGY_BID].iloc[:]) / 2).astype(int)
            # Calculate traded energy quantities
            positions_cleared = positions_cleared.assign(**{
                db_obj.db_param.QTY_ENERGY_TRADED: [positions_cleared.index[0]] + list(
                    np.diff(positions_cleared.index))})
            # Assign traded quantities to bid and offer quantities
            positions_cleared = positions_cleared.assign(**{
                db_obj.db_param.QTY_ENERGY + db_obj.db_param.EXTENSION_OFFER: positions_cleared[
                    db_obj.db_param.QTY_ENERGY_TRADED]})
            positions_cleared = positions_cleared.assign(**{
                db_obj.db_param.QTY_ENERGY + db_obj.db_param.EXTENSION_BID: positions_cleared[
                    db_obj.db_param.QTY_ENERGY_TRADED]})
            # Cleared energy quantity is equal to sum of cleared energy quantities
            qty_energy_cleared = positions_cleared[db_obj.db_param.QTY_ENERGY_TRADED].sum()
            # Extract cleared bids and offers
            offers_cleared = _extract_positions_by_extension(db_obj=db_obj,
                                                             positions_merged=positions_cleared,
                                                             extension=db_obj.db_param.EXTENSION_OFFER)
            bids_cleared = _extract_positions_by_extension(db_obj=db_obj,
                                                           positions_merged=positions_cleared,
                                                           extension=db_obj.db_param.EXTENSION_BID)

            # Calculate shares of labelled energy of cleared positions
            for i in range(len(config_lem['types_quality'])):
                type_quality = config_lem['types_quality'][i]
                # extract rows with certain quality
                positions_cleared_quality = positions_cleared.loc[
                    positions_cleared[db_obj.db_param.QUALITY_ENERGY_OFFER] == i]
                # Calculate sum of cleared energy of specific quality
                qty_energy_cleared_quality = positions_cleared_quality[db_obj.db_param.QTY_ENERGY_TRADED].sum()
                # Assign share to cleared market positions
                positions_cleared = positions_cleared.assign(
                    **{db_obj.db_param.SHARE_QUALITY_ + type_quality: int(
                        qty_energy_cleared_quality / qty_energy_cleared * 100)})
        # Drop duplicate ts_delivery column
        positions_cleared = positions_cleared.rename(columns={'ts_delivery_offer': 'ts_delivery'})
        positions_cleared = positions_cleared.drop(columns={'ts_delivery_bid'})
        # Extract all uncleared bids_sorted/offers_sorted
        bids_offers_uncleared = positions_merged[
            positions_merged[db_obj.db_param.PRICE_ENERGY + db_obj.db_param.EXTENSION_OFFER] >
            positions_merged[db_obj.db_param.PRICE_ENERGY + db_obj.db_param.EXTENSION_BID]]
        bids_offers_uncleared = bids_offers_uncleared.append(
            positions_merged[positions_merged.isna().any(axis=1)])
        if not bids_offers_uncleared.empty:
            # Assign merged quantities to positions
            qty_merged = [bids_offers_uncleared.index[0] - qty_energy_cleared] + \
                         list(np.diff(bids_offers_uncleared.index))
            bids_offers_uncleared = bids_offers_uncleared.assign(**{
                db_obj.db_param.QTY_ENERGY + db_obj.db_param.EXTENSION_OFFER: qty_merged})
            bids_offers_uncleared = bids_offers_uncleared.assign(**{
                db_obj.db_param.QTY_ENERGY + db_obj.db_param.EXTENSION_BID: qty_merged})
            # Extract uncleared bids and offers, drop nan rows
            offers_uncleared = _extract_positions_by_extension(db_obj=db_obj,
                                                               positions_merged=bids_offers_uncleared,
                                                               extension=db_obj.db_param.EXTENSION_OFFER)
            bids_uncleared = _extract_positions_by_extension(db_obj=db_obj,
                                                             positions_merged=bids_offers_uncleared,
                                                             extension=db_obj.db_param.EXTENSION_BID)

        if plotting:
            if plotting_title is None:
                plotting_title = f'Clearing: standard'
            else:
                plotting_title = f'{plotting_title}'
            # Plot results
            plot_clearing_results(db_obj=db_obj,
                                  offers=offers_sorted, bids=bids_sorted, positions_cleared=positions_cleared,
                                  show=True, types_pricing=config_lem['types_pricing_ex_ante'],
                                  plotting_title=plotting_title,
                                  y_lim=plotting_ylim)

    except Exception:
        traceback.print_exc()

    return positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared


def clearing_pref_satis(db_obj,
                        config_lem,
                        offers,
                        bids,
                        type_clearing=None,
                        type_prioritization=None,
                        add_premium=False,
                        plotting=False,
                        plotting_title=None,
                        verbose=False):
    """
    Function clears offers and bids according to the preference satisfaction approach. First clearing a standard
    double sided auction, then checking whether all preferences have been satisfied, removing any unsatisfied bids and
    recalculating the double sided auction. Finally, a preference prioritization is added.
    @param db_obj: DatabaseConnection object
    @param config_lem: configuration dictionary for clearing algorithm
    @param offers: dataframe of various offers consisting of price, quantity, quality, ts_delivery, id and type
    @param bids: dataframe of various bids consisting of price, quantity, quality, ts_delivery, id and type
    @param type_clearing: clearing type that is using clearing da functionality
    @param type_prioritization: type of preference prioritization
    @param add_premium: boolean value to add premium to bid price
    @param plotting: boolean value to plot clearing results
    @param plotting_title: title of plot, ignored if plotting is false
    @param verbose: boolean value to print execution information
    @return: returns cleared bids and offers dataframe
    """
    offers_uncleared = pd.DataFrame()
    bids_uncleared = pd.DataFrame()
    positions_cleared = pd.DataFrame()
    # Check whether bids or offers are empty
    if bids.empty or offers.empty or \
            bids[bids[db_obj.db_param.QTY_ENERGY] > 0].empty or \
            offers[offers[db_obj.db_param.QTY_ENERGY] > 0].empty:
        return positions_cleared
    if type_clearing is None:
        type_clearing = 'da_pref_satis'
    try:
        # Extract uniques qualities
        unique_qualities = np.unique(
            np.concatenate(
                (offers[db_obj.db_param.QUALITY_ENERGY].unique(), bids[db_obj.db_param.QUALITY_ENERGY].unique())))
        # Initiate while loop variables
        bids_unsatisfied = True
        bids_cleared_q_satisfied_all = pd.DataFrame()
        bids_cld_q_all_unsatisfied_total = pd.DataFrame()
        bids_updated = pd.DataFrame()
        counter = 0
        while bids_unsatisfied:
            # Check whether bids are empty
            if bids_updated.empty and counter > 0:
                bids_uncleared = bids
                offers_uncleared = offers
                # bids_cld_q_all_unsatisfied = pd.DataFrame()
                if verbose:
                    print('Preferences of bids can not be satisfied.')
                break
            elif counter > 0:
                bids = bids_updated
            # Reset unsatisfied bids every time!
            bids_cld_q_all_unsatisfied = pd.DataFrame()
            # Clearing
            positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                clearing_da(db_obj=db_obj, config_lem=config_lem, offers=offers, bids=bids,
                            type_clearing=type_clearing, add_premium=add_premium,
                            plotting=plotting, plotting_title=f'{plotting_title}; pref. satis. #{counter}')

            # Check if any bids and offers were cleared
            if positions_cleared.empty:
                break
            bids_cleared_ds = _downsample_positions(db_obj=db_obj, positions=bids_cleared)
            offers_cleared_ds = _downsample_positions(db_obj=db_obj, positions=offers_cleared)
            qty_offers_quality_assigned = 0  # energy quantity that has already been assigned to quality bids
            # Check preference satisfaction -> 2: Green Local, 1: Green, 0: Gray
            for quality_energy in reversed(sorted(unique_qualities)):
                offers_cleared_q = offers_cleared_ds[
                    offers_cleared_ds[db_obj.db_param.QUALITY_ENERGY] >= quality_energy]
                offers_cleared_q = offers_cleared_q.assign(CumEnQ=offers_cleared_q[db_obj.db_param.QTY_ENERGY].cumsum())
                bids_cleared_q = bids_cleared_ds[bids_cleared_ds[db_obj.db_param.QUALITY_ENERGY] == quality_energy]
                bids_cleared_q = bids_cleared_q.assign(CumEnQ=bids_cleared_q[db_obj.db_param.QTY_ENERGY].cumsum())
                qty_offers_max = offers_cleared_q[db_obj.db_param.QTY_ENERGY].sum() - qty_offers_quality_assigned
                bids_cleared_q_satisfied = bids_cleared_q[bids_cleared_q['CumEnQ'] <= qty_offers_max]
                qty_offers_quality_assigned += bids_cleared_q_satisfied[db_obj.db_param.QTY_ENERGY].sum()
                bids_cleared_q_satisfied_all = bids_cleared_q_satisfied_all.append(bids_cleared_q_satisfied)
                if not bids_cleared_q[db_obj.db_param.QTY_ENERGY].sum() <= qty_offers_max:
                    bids_cld_q_all_unsatisfied = bids_cld_q_all_unsatisfied.append(
                        bids_cleared_q[bids_cleared_q['CumEnQ'] > qty_offers_max])

            # Check whether
            if bids_cld_q_all_unsatisfied.empty:
                bids_unsatisfied = False
                break
            # Drop CumEnQ column and reset index
            bids_cld_q_all_unsatisfied = bids_cld_q_all_unsatisfied.drop(columns='CumEnQ')
            bids_cld_q_all_unsatisfied = bids_cld_q_all_unsatisfied.reset_index(drop=True)
            # Aggregate bids and unsatisfied bids
            bids_cld_q_all_unsatisfied = _aggregate_identical_positions(db_obj=db_obj,
                                                                        positions=bids_cld_q_all_unsatisfied,
                                                                        subset=[db_obj.db_param.TS_DELIVERY,
                                                                                db_obj.db_param.ID_USER,
                                                                                db_obj.db_param.PRICE_ENERGY,
                                                                                db_obj.db_param.QUALITY_ENERGY])
            bids_cld_q_all_unsatisfied_total = bids_cld_q_all_unsatisfied_total.append(bids_cld_q_all_unsatisfied)
            bids_updated = _aggregate_identical_positions(db_obj=db_obj,
                                                          positions=bids,
                                                          subset=[db_obj.db_param.TS_DELIVERY,
                                                                  db_obj.db_param.ID_USER,
                                                                  db_obj.db_param.PRICE_ENERGY,
                                                                  db_obj.db_param.QUALITY_ENERGY])
            # Remove all unsatisfied bids from all bids
            for i, row in bids_cld_q_all_unsatisfied.iterrows():
                if add_premium:
                    _df_bid_removal = bids_updated[
                        ((bids_updated[db_obj.db_param.PRICE_ENERGY] + bids_updated[db_obj.db_param.PRICE_ENERGY] *
                          bids_updated[db_obj.db_param.PREMIUM_PREFERENCE_QUALITY] / 100).astype(int) == row[
                             db_obj.db_param.PRICE_ENERGY]) &
                        (bids_updated[db_obj.db_param.ID_USER] == row[db_obj.db_param.ID_USER]) &
                        (bids_updated[db_obj.db_param.QUALITY_ENERGY] == row[db_obj.db_param.QUALITY_ENERGY]) &
                        (bids_updated[db_obj.db_param.QTY_ENERGY] >= row[db_obj.db_param.QTY_ENERGY])
                        ]
                else:
                    _df_bid_removal = bids_updated[
                        (bids_updated[db_obj.db_param.PRICE_ENERGY] == row[db_obj.db_param.PRICE_ENERGY]) &
                        (bids_updated[db_obj.db_param.ID_USER] == row[db_obj.db_param.ID_USER]) &
                        (bids_updated[db_obj.db_param.QUALITY_ENERGY] == row[db_obj.db_param.QUALITY_ENERGY]) &
                        (bids_updated[db_obj.db_param.QTY_ENERGY] >= row[db_obj.db_param.QTY_ENERGY])
                        ]
                new_qty = _df_bid_removal[db_obj.db_param.QTY_ENERGY] - row[db_obj.db_param.QTY_ENERGY]
                _df_bid_removal = _df_bid_removal.assign(**{db_obj.db_param.QTY_ENERGY: new_qty})
                bids_updated = bids_updated.drop(index=_df_bid_removal.index).append(_df_bid_removal)

            # Remove all bids with zero quantity
            bids_updated = bids_updated[bids_updated[db_obj.db_param.QTY_ENERGY] > 0]
            counter = counter + 1
        # Append unsatisfied bids with uncleared bids
        bids_uncleared = bids_uncleared.append(bids_cld_q_all_unsatisfied_total)
        if not bids_uncleared.empty:
            # Clear in a preference prioritization for remaining positions
            results_pp, offers_uncleared_pp, bids_uncleared_pp, offers_cleared_pp, bids_cleared_pp = \
                clearing_pref_prio(db_obj=db_obj,
                                   config_lem=config_lem,
                                   offers=offers_uncleared,
                                   bids=bids_uncleared,
                                   type_clearing=type_clearing,
                                   type_prioritization=type_prioritization,
                                   add_premium=add_premium,
                                   plotting=plotting,
                                   plotting_title=f'{plotting_title}; pref. '
                                                  f'satis.')
            positions_cleared = positions_cleared.append(results_pp).reset_index(drop=True)
            bids_uncleared = bids_uncleared_pp
            offers_uncleared = offers_uncleared_pp

    except Exception as e:
        print(e)
        traceback.print_exc()

    return positions_cleared, offers_uncleared, bids_uncleared


def clearing_pref_prio(db_obj,
                       config_lem,
                       offers,
                       bids,
                       type_clearing=None,
                       type_prioritization=None,
                       add_premium=False,
                       plotting=False,
                       plotting_title=None):
    """
    Function clears offers and bids according to the preference prioritization approach. First, clearing offers with the
    highest energy quality preference and the corresponding bids and then second highest preference and so on.
    Therefore, assigning a higher priority to higher energy qualities.
    @param db_obj: DatabaseConnection object
    @param config_lem: configuration dictionary for clearing algorithm
    @param offers: dataframe of various offers consisting of price, quantity, quality, ts_delivery, id and type
    @param bids: dataframe of various bids consisting of price, quantity, quality, ts_delivery, id and type
    @param type_clearing: clearing type that is using clearing da functionality
    @param type_prioritization: variable to select prioritization type ['pref_n_to_0', 'pref_0_to_n', 'pref_separation']
    @param add_premium: boolean value to add premium to bid price
    @param plotting: boolean value to plot clearing results
    @param plotting_title: title of plot, ignored if plotting is false
    @return: returns cleared and uncleared bids and offers in multiple dataframes
    """
    bids_uncleared = pd.DataFrame()
    offers_uncleared = pd.DataFrame()
    bids_cleared = pd.DataFrame()
    offers_cleared = pd.DataFrame()
    positions_cleared_all = pd.DataFrame()
    if type_prioritization is None:
        type_prioritization = 'pref_n_to_0'
    if type_clearing is None:
        type_clearing = 'da_pref_prio'
    if plotting_title is None:
        plotting_title = 'pref. prio.'
    else:
        plotting_title = f'{plotting_title}; pref. prio.'
    # Check whether offers or bids are empty
    if offers.empty or bids.empty or \
            bids[bids[db_obj.db_param.QTY_ENERGY] > 0].empty or \
            offers[offers[db_obj.db_param.QTY_ENERGY] > 0].empty:
        bids_uncleared = bids
        offers_uncleared = offers
        return positions_cleared_all, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared
    # Extract, sort, and optionally flip unique qualities
    unique_preferences = np.unique(np.concatenate((offers[db_obj.db_param.QUALITY_ENERGY].unique(),
                                                   bids[db_obj.db_param.QUALITY_ENERGY].unique())))
    preferences_sorted = np.sort(unique_preferences)
    if type_prioritization == 'pref_n_to_0':
        preferences_sorted = np.flip(preferences_sorted)

    # Calculate clearing prices for every quality
    for preference in preferences_sorted:
        # Filter bids and offers by preference
        bids_temp = bids[bids[db_obj.db_param.QUALITY_ENERGY] == preference]
        offers_temp = pd.DataFrame()
        if type_prioritization == 'pref_n_to_0':
            if preference == preferences_sorted[0]:
                offers_temp = offers[offers[db_obj.db_param.QUALITY_ENERGY] == preference]
            else:
                offers_temp = offers[offers[db_obj.db_param.QUALITY_ENERGY] == preference].append(offers_uncleared)
        if type_prioritization == 'pref_0_to_n':
            if preference == preferences_sorted[0]:
                offers_temp = offers[offers[db_obj.db_param.QUALITY_ENERGY] >= preference]
            else:
                offers_temp = offers_uncleared[offers_uncleared[db_obj.db_param.QUALITY_ENERGY] >= preference]
        if type_prioritization == 'pref_separation':
            offers_temp = offers[offers[db_obj.db_param.QUALITY_ENERGY] == preference]

        if offers_temp.empty or bids_temp.empty:
            offers_uncleared = offers_temp
            bids_uncleared = bids_temp
            continue
        # Calculate clearing prices
        positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
            clearing_da(db_obj=db_obj, config_lem=config_lem, offers=offers_temp, bids=bids_temp,
                        type_clearing=type_clearing, add_premium=add_premium,
                        plotting=plotting, plotting_title=f'{plotting_title} #{preference}')
        positions_cleared_all = positions_cleared_all.append(positions_cleared).reset_index(drop=True)

    return positions_cleared_all, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared


def _add_supplier_bids(db_obj,
                       config_supplier,
                       t_clearing_current,
                       sorted_bids_t_d,
                       sorted_offers_t_d):
    """
    Adds supplier bid and offer to offers and bids.
    @param db_obj: DatabaseConnection object
    @param t_clearing_current: time of delivery for which bid and offer are inserted
    @param sorted_bids_t_d: dataframe of bids in which supplier bid is inserted
    @param sorted_offers_t_d: dataframe of offers in which supplier offer is inserted
    @return: dataframes of offers and bids
    """
    temp_df = pd.DataFrame(columns=db_obj.get_table_columns(db_obj.db_param.NAME_TABLE_POSITIONS_MARKET_EX_ANTE))
    temp_df.at[0, db_obj.db_param.T_SUBMISSION] = round(time.time())
    temp_df.at[0, db_obj.db_param.ID_USER] = config_supplier['id_user']
    temp_df.at[0, db_obj.db_param.QTY_ENERGY] = config_supplier['qty_energy_offer']
    temp_df.at[0, db_obj.db_param.TYPE_POSITION] = 0
    temp_df.at[0, db_obj.db_param.PRICE_ENERGY] = int(
        config_supplier['price_sell'] * db_obj.db_param.EURO_TO_SIGMA / 1000)
    temp_df.at[0, db_obj.db_param.QUALITY_ENERGY] = 0
    temp_df.at[0, db_obj.db_param.NUMBER_POSITION] = 0
    temp_df.at[0, db_obj.db_param.STATUS_POSITION] = 0
    temp_df.at[0, db_obj.db_param.PREMIUM_PREFERENCE_QUALITY] = 0
    temp_df.at[0, db_obj.db_param.TS_DELIVERY] = t_clearing_current
    sorted_offers_t_d = sorted_offers_t_d.append(temp_df, ignore_index=True)

    temp_df.at[0, db_obj.db_param.TYPE_POSITION] = 1
    temp_df.at[0, db_obj.db_param.PRICE_ENERGY] = int(
        config_supplier['price_buy'] * db_obj.db_param.EURO_TO_SIGMA / 1000)
    temp_df.at[0, db_obj.db_param.QTY_ENERGY] = config_supplier['qty_energy_bid']
    sorted_bids_t_d = sorted_bids_t_d.append(temp_df, ignore_index=True)

    return sorted_bids_t_d, sorted_offers_t_d


def _post_processing_results(db_obj, results, t_clearing_start):
    # Add clearing time and drop matching id
    results[db_obj.db_param.T_CLEARED] = t_clearing_start
    # Drop all unnecessary columns
    results = results.drop(columns={db_obj.db_param.QTY_ENERGY + db_obj.db_param.EXTENSION_OFFER,
                                    db_obj.db_param.QTY_ENERGY + db_obj.db_param.EXTENSION_BID,
                                    db_obj.db_param.TYPE_POSITION + db_obj.db_param.EXTENSION_OFFER,
                                    db_obj.db_param.TYPE_POSITION + db_obj.db_param.EXTENSION_BID,
                                    db_obj.db_param.STATUS_POSITION + db_obj.db_param.EXTENSION_OFFER,
                                    db_obj.db_param.STATUS_POSITION + db_obj.db_param.EXTENSION_BID,
                                    db_obj.db_param.T_SUBMISSION + db_obj.db_param.EXTENSION_OFFER,
                                    db_obj.db_param.T_SUBMISSION + db_obj.db_param.EXTENSION_BID,
                                    db_obj.db_param.QUALITY_ENERGY_BID,
                                    db_obj.db_param.QUALITY_ENERGY_OFFER,
                                    db_obj.db_param.PREMIUM_PREFERENCE_QUALITY + db_obj.db_param.EXTENSION_OFFER,
                                    db_obj.db_param.PREMIUM_PREFERENCE_QUALITY + db_obj.db_param.EXTENSION_BID
                                    })

    return results


def _aggregate_identical_positions(db_obj,
                                   positions,
                                   subset):
    """
    Function aggregates identical positions based on the subset of columns passed as a variable.
    @param db_obj: DatabaseConnection object
    @param positions: dataframe of positions that are aggregated
    @param subset: subset of columns based on which the aggregation is performed
    @return: dataframe with non-identical positions
    """
    # Sort positions by price, quality and id
    positions = positions.sort_values(by=subset, ignore_index=True)
    # reset index to cumulated energy quantities
    positions = positions.set_index(positions[db_obj.db_param.QTY_ENERGY].cumsum())
    # Drop duplicates that contain the same price, quality and id and keep the last of duplicates
    positions = positions.drop_duplicates(subset=subset, keep='last')
    # reassign new quantities to qty_energy
    positions = positions.assign(**{db_obj.db_param.QTY_ENERGY: [positions.index[0]] + list(np.diff(positions.index))})
    # Reset index of positions
    positions = positions.reset_index(drop=True)

    return positions


def _downsample_positions(db_obj,
                          positions):
    """
    Function downsamples positions to singular energy quantities
    @param db_obj: DatabaseConnection object
    @param positions: dataframe of positions
    @return: dataframe of downsampled positions
    """
    # Set cumulated energy quantity as index
    positions = positions.set_index(positions[db_obj.db_param.QTY_ENERGY].cumsum())
    # Reindex df
    positions = positions.reindex(pd.RangeIndex(1, positions.index[-1] + 1)).bfill()
    # reassign new quantities to qty_energy
    positions = positions.assign(**{db_obj.db_param.QTY_ENERGY: [positions.index[0]] + list(np.diff(positions.index))})
    # Reset index of positions
    positions = positions.reset_index(drop=True)

    return positions


def _extract_positions_by_extension(db_obj,
                                    positions_merged,
                                    extension):
    """
    Filters/extracts all columns with a certain extension (e.g. _bid or _offer)
    @param db_obj: DatabaseConnection object
    @param positions_merged: dataframe of merged positions
    @param extension: extension by which the columns are filtered
    @return: dataframe of extracted columns
    """
    # Extract/filter positions by extension
    positions_filtered = positions_merged.filter(regex=extension, axis=1).dropna(axis=0)
    # Remove suffixes from columns
    positions_filtered.columns = [i.replace(extension, '') for i in positions_filtered.columns]
    if not positions_filtered.empty:
        # Aggregate equal positions
        positions_filtered = _aggregate_identical_positions(db_obj=db_obj,
                                                            positions=positions_filtered,
                                                            subset=[db_obj.db_param.PRICE_ENERGY,
                                                                    db_obj.db_param.QUALITY_ENERGY,
                                                                    db_obj.db_param.ID_USER])
    return positions_filtered


def _log_transactions_market(db_obj, config_lem, results_market, name_column_price, types_quality):
    """
    @param db_obj: DatabaseConnection object

    """
    mapping_to_user = db_obj.get_mapping_to_user()
    df_transactions_all = pd.DataFrame()

    # Create transaction df for seller credit
    df_transactions = pd.DataFrame()
    df_transactions = df_transactions.assign(
        **{db_obj.db_param.ID_USER: [mapping_to_user[meter_or_user] for meter_or_user in
                                     results_market[db_obj.db_param.ID_USER_OFFER]]})
    df_transactions = df_transactions.assign(
        **{db_obj.db_param.TS_DELIVERY: results_market[db_obj.db_param.TS_DELIVERY]})
    df_transactions = df_transactions.assign(
        **{db_obj.db_param.PRICE_ENERGY_MARKET: results_market[name_column_price]})
    df_transactions = df_transactions.assign(**{db_obj.db_param.TYPE_TRANSACTION: config_lem['types_transaction'][0]})
    df_transactions = df_transactions.assign(
        **{db_obj.db_param.QTY_ENERGY: results_market[db_obj.db_param.QTY_ENERGY_TRADED]})
    df_transactions = df_transactions.assign(
        **{db_obj.db_param.DELTA_BALANCE: results_market[db_obj.db_param.QTY_ENERGY_TRADED] * results_market[
            name_column_price]})
    df_transactions = df_transactions.assign(
        **{db_obj.db_param.T_UPDATE_BALANCE: results_market[db_obj.db_param.T_CLEARED]})
    for type_quality in types_quality.values():
        df_transactions = df_transactions.assign(
            **{db_obj.db_param.SHARE_QUALITY_ + type_quality: results_market[
                db_obj.db_param.SHARE_QUALITY_ + type_quality]})

    # Log all credit transactions
    db_obj.log_transactions(df_transactions)
    df_transactions_all = df_transactions_all.append(df_transactions, ignore_index=True)

    # Create transaction df for consumer debit
    df_transactions = df_transactions.assign(
        **{db_obj.db_param.DELTA_BALANCE: -1 * results_market[db_obj.db_param.QTY_ENERGY_TRADED] *
                                          results_market[name_column_price]})
    df_transactions = df_transactions.assign(
        **{db_obj.db_param.ID_USER: [mapping_to_user[meter_or_user] for meter_or_user in
                                     results_market[db_obj.db_param.ID_USER_BID]]})
    df_transactions = df_transactions.assign(
        **{db_obj.db_param.QTY_ENERGY: -1 * results_market[db_obj.db_param.QTY_ENERGY_TRADED]})

    # Log all debit transactions
    db_obj.log_transactions(df_transactions)
    df_transactions_all = df_transactions_all.append(df_transactions, ignore_index=True)

    return df_transactions_all


def _update_user_balances(db_obj, df_transactions):
    balance_update_df = pd.DataFrame()
    df_transactions_grouped = df_transactions.groupby(db_obj.db_param.ID_USER).sum()
    balance_update_df = balance_update_df.assign(**{db_obj.db_param.ID_USER: df_transactions_grouped.index})
    balance_update_df = balance_update_df.assign(
        **{db_obj.db_param.DELTA_BALANCE: list(df_transactions_grouped[db_obj.db_param.DELTA_BALANCE])})
    balance_update_df = balance_update_df.assign(
        **{db_obj.db_param.T_UPDATE_BALANCE: df_transactions[db_obj.db_param.T_UPDATE_BALANCE][0]})

    # Update balances
    db_obj.update_balance_user(balance_update_df)


def _convert_qualities_to_int(db_obj, positions, dict_types):
    dict_types_inverted = {v: k for k, v in dict_types.items()}
    # if type(positions[db_obj.db_param.QUALITY_ENERGY]) != list:
    #    positions[db_obj.db_param.QUALITY_ENERGY] = [dict_types_inverted[i] for i in
    #                                                 [positions[db_obj.db_param.QUALITY_ENERGY]]]
    # else:
    # if len(positions) > 1:
    positions[db_obj.db_param.QUALITY_ENERGY] = [dict_types_inverted[i] for i in
                                                 positions[db_obj.db_param.QUALITY_ENERGY]]
    return positions


def plot_clearing_results(db_obj,
                          offers,
                          bids,
                          positions_cleared,
                          show=True,
                          y_lim=None,
                          x_lim=None,
                          style_dict_quality=None,
                          types_pricing=None,
                          plotting_title=None):
    """
    Function visualizes clearing results with qualities and clearing prices.
    @param db_obj: DatabaseConnection object
    @param offers: dataframe of various offers consisting of price, quantity, quality, ts_delivery, id and type
    @param bids: dataframe of various bids consisting of price, quantity, quality, ts_delivery, id and type
    @param positions_cleared: dataframe of cleared offers and bids
    @param show: boolean to show the figure
    @param y_lim: limits of y-axis as list
    @param x_lim: limits for x-axis as list
    @param style_dict_quality: dictionary for quality styles
    @param types_pricing: list of strings specifying the pricing methods
    @param plotting_title: plot title
    """
    try:
        # plt.xkcd()
        color_dict = {'offer': '#0059b3', 'bid': '#669999'}
        if style_dict_quality is None:
            style_dict_quality = {0: {'color': 'gray', 'style': '-', 'width': 2},
                                  1: {'color': 'gray', 'style': ':', 'width': 3},
                                  2: {'color': 'gray', 'style': '--', 'width': 4},
                                  3: {'color': 'gray', 'style': '-.', 'width': 5},
                                  'uniform': {'color': 'k', 'style': '--', 'width': 2},
                                  'discriminatory': {'color': 'k', 'style': ':', 'width': 2}}
        plt.figure(figsize=(8, 6))
        # Plot for control purpose
        plt.plot([0] + list(bids.index),
                 [x / db_obj.db_param.EURO_TO_SIGMA * 1000 for x in
                  ([bids[db_obj.db_param.PRICE_ENERGY].iloc[0]] + list(bids[db_obj.db_param.PRICE_ENERGY]))],
                 drawstyle='steps', linewidth=1, color=color_dict['bid'], label='Bids')
        plt.plot([0] + list(offers.index),
                 [x / db_obj.db_param.EURO_TO_SIGMA * 1000 for x in
                  [offers[db_obj.db_param.PRICE_ENERGY].iloc[0]] + list(offers[db_obj.db_param.PRICE_ENERGY])],
                 drawstyle='steps', linewidth=1, color=color_dict['offer'], label='Offers')

        if not positions_cleared.empty:
            if x_lim is None:
                x_lim = [0, 2 * positions_cleared[db_obj.db_param.QTY_ENERGY_TRADED].sum()]
            plt.plot([0] + list(positions_cleared.index),
                     [x / db_obj.db_param.EURO_TO_SIGMA * 1000 for x in
                      [positions_cleared[db_obj.db_param.PRICE_ENERGY + db_obj.db_param.EXTENSION_OFFER].iloc[0]] +
                      list(positions_cleared[db_obj.db_param.PRICE_ENERGY + db_obj.db_param.EXTENSION_OFFER])],
                     linewidth=2, drawstyle='steps', color=color_dict['offer'], label='Cleared bids')
            plt.plot([0] + list(positions_cleared.index),
                     [x / db_obj.db_param.EURO_TO_SIGMA * 1000 for x in
                      [positions_cleared[db_obj.db_param.PRICE_ENERGY + db_obj.db_param.EXTENSION_BID].iloc[0]] +
                      list(positions_cleared[db_obj.db_param.PRICE_ENERGY + db_obj.db_param.EXTENSION_BID])],
                     linewidth=2, drawstyle='steps', color=color_dict['bid'], label='Cleared offers')
            for type_pricing in types_pricing.values():
                # Plot for control purpose
                plt.plot([0] + list(positions_cleared.index),
                         [x / db_obj.db_param.EURO_TO_SIGMA * 1000 for x in
                          [positions_cleared[db_obj.db_param.PRICE_ENERGY_MARKET_ + type_pricing].iloc[
                               0]] + list(
                              positions_cleared[db_obj.db_param.PRICE_ENERGY_MARKET_ + type_pricing])],
                         drawstyle='steps',
                         linestyle=style_dict_quality[type_pricing]['style'],
                         color=style_dict_quality[type_pricing]['color'],
                         label=type_pricing)

        # Extract uniques qualities
        pref = np.unique(np.concatenate((offers[db_obj.db_param.QUALITY_ENERGY].unique(),
                                         bids[db_obj.db_param.QUALITY_ENERGY].unique())))

        if pref.any():
            for _pref in pref:
                for bid_or_offer, _df in {'bid': bids, 'offer': offers}.items():
                    plot_df = _df.assign(index_before=[0] + list(_df.index[:-1]))
                    plot_df = plot_df[plot_df[db_obj.db_param.QUALITY_ENERGY] == _pref]
                    for i in plot_df.index:
                        plt.plot([plot_df.loc[i, 'index_before']] + [i],
                                 [x / db_obj.db_param.EURO_TO_SIGMA * 1000 for x in
                                  [plot_df.loc[i, db_obj.db_param.PRICE_ENERGY]] + [
                                      plot_df.loc[i, db_obj.db_param.PRICE_ENERGY]]],
                                 drawstyle='steps', linewidth=style_dict_quality[_pref]['width'],
                                 linestyle=style_dict_quality[_pref]['style'],
                                 color=color_dict[bid_or_offer], label=f'{bid_or_offer} preference: {_pref}')
                    # plt.title(f'Plot for quality_energy {_pref}')

        if show:
            if plotting_title:
                plt.title(plotting_title)
            plt.ylabel('Energy price [€/kWh]')
            plt.xlabel('Energy [Wh]')
            plt.grid()
            if y_lim is not None:
                plt.ylim(y_lim)
            if x_lim is not None:
                plt.xlim(x_lim)
            handles, labels = plt.gca().get_legend_handles_labels()
            by_label = OrderedDict(zip(labels, handles))
            plt.legend(by_label.values(), by_label.keys(), loc='lower right')
            plt.tight_layout()
            plt.show()
    except Exception:
        traceback.print_exc()


def calc_random_position(db_obj, config, t_d, user_id):
    position = pd.DataFrame(columns=db_obj.get_table_columns(db_obj.db_param.NAME_TABLE_POSITIONS_MARKET_EX_ANTE))
    position.at[0, db_obj.db_param.T_SUBMISSION] = round(time.time())
    position.at[0, db_obj.db_param.ID_USER] = user_id
    position.at[0, db_obj.db_param.QTY_ENERGY] = np.random.randint(1, 1000)
    _, position_type = random.choice(list(config['lem']['types_position'].items()))
    position.at[0, db_obj.db_param.TYPE_POSITION] = position_type
    _, quality_type = random.choice(list(config['lem']['types_quality'].items()))
    position.at[0, db_obj.db_param.QUALITY_ENERGY] = quality_type
    if position_type == 'offer':
        position.at[0, db_obj.db_param.PRICE_ENERGY] = int(random.uniform(config['retailer']['price_buy'],
                                                                          config['retailer']['price_sell'])
                                                           * db_obj.db_param.EURO_TO_SIGMA / 1000)
        position.at[0, db_obj.db_param.PREMIUM_PREFERENCE_QUALITY] = 0
    elif position_type == 'bid':
        position.at[0, db_obj.db_param.PRICE_ENERGY] = int(random.uniform(config['retailer']['price_buy'],
                                                                          config['retailer']['price_sell'])
                                                           * db_obj.db_param.EURO_TO_SIGMA / 1000)
        position.at[0, db_obj.db_param.PREMIUM_PREFERENCE_QUALITY] = random.randint(0, 50)
    position.at[0, db_obj.db_param.NUMBER_POSITION] = int(0)
    position.at[0, db_obj.db_param.STATUS_POSITION] = int(0)
    position.at[0, db_obj.db_param.TS_DELIVERY] = int(t_d)

    return position


def create_random_positions(db_obj, config, ids_user, n_positions=None, verbose=False):
    if n_positions is None:
        n_positions = 100
    t_start = round(time.time()) - (
            round(time.time()) % config['lem']['interval_clearing']) + config['lem']['interval_clearing']
    t_end = t_start + config['lem']['horizon_clearing']
    # Range of time steps
    t_d_range = np.arange(t_start, t_end, config['lem']['interval_clearing'])
    # Create bid df
    positions = pd.DataFrame()
    for i in range(0, n_positions):
        # Select random time step and calculate random offer and demand
        positions = positions.append(calc_random_position(db_obj, config,
                                                          t_d=int(random.sample(list(t_d_range), 1)[0]),
                                                          user_id=random.sample(ids_user, 1)[0]),
                                     ignore_index=True)
    # Drop duplicates
    positions = positions.drop_duplicates(
        subset=[db_obj.db_param.ID_USER, db_obj.db_param.NUMBER_POSITION, db_obj.db_param.TYPE_POSITION,
                db_obj.db_param.TS_DELIVERY])
    if verbose:
        print(pd.Timestamp.now(), 'Positions successfully written to DB')

    return positions


# Create random user ids
def create_user_ids(num=30):
    user_id_list = list()
    for i in range(num):
        # Create random user id in the form of 1234ABDS
        user_id_int = np.random.randint(1000, 10000)
        user_id_str = ''.join(random.sample(string.ascii_uppercase, 4))
        user_id_random = str(user_id_int) + user_id_str
        # Append user id to list
        user_id_list.append(user_id_random)

    return user_id_list


# this method, find the maximum number of clearings that the blockchain manages to perform at once, according to the gas limit
def findLimit(n_clearings_max, t_clearing_current, supplier_bids, uniform_pricing, discriminative_pricing,
              t_clearing_start,
              gasThreshold, interval_clearing, simulation_test):
    n_clearings_current = n_clearings_max
    estimate = 10 * gasThreshold
    while estimate > gasThreshold:
        try:
            estimate = blockchain_utils.functions.market_clearing(int(n_clearings_current), int(t_clearing_current),
                                                                  supplier_bids, uniform_pricing,
                                                                  discriminative_pricing,
                                                                  int(interval_clearing),
                                                                  int(t_clearing_start), False, verbose_blockchain,
                                                                  False, simulation_test).estimateGas()
            n_clearings_current = int(n_clearings_current / (estimate / gasThreshold))
        except Exception as e:
            print(e)
            n_clearings_current = int(n_clearings_current / 2)

    return n_clearings_current


# this method, perform the market clearing and gets the results from the blockchain
def get_market_results_blockchain(t_override, n_clearings, supplier_bids, uniform_pricing, discriminative_pricing,
                                  t_clearing_start, interval_clearing, simulation_test, shuffle=False):
    t_now = t_override
    t_clearing_first = t_now - (t_now % interval_clearing) + interval_clearing

    t_clearing_current = t_clearing_first
    n_clearings_done = 0

    # max doable clearings
    limit_clearings = findLimit(n_clearings, t_clearing_first, supplier_bids, uniform_pricing, discriminative_pricing,
                                t_clearing_start,
                                gasThreshold=40000000, interval_clearing=interval_clearing,
                                simulation_test=simulation_test)

    n_clearings_current = limit_clearings
    update_balances = False
    while n_clearings_done < n_clearings:
        if n_clearings - n_clearings_done <= n_clearings_current:  # last step
            n_clearings_current = n_clearings - n_clearings_done
            update_balances = False
        try:
            # Performing the market clearing for a number of clearings
            tx_hash = blockchain_utils.functions.market_clearing(int(n_clearings_current), int(t_clearing_current),
                                                                 supplier_bids,
                                                                 uniform_pricing,
                                                                 discriminative_pricing,
                                                                 int(interval_clearing),
                                                                 int(t_clearing_start), shuffle, verbose_blockchain,
                                                                 update_balances, simulation_test).transact(
                {'from': blockchain_utils.coinbase})
            blockchain_utils.web3_instance.eth.waitForTransactionReceipt(tx_hash, timeout=600)  # 600 seconds wait
            if verbose_blockchain:
                log = blockchain_utils.getLog(tx_hash=tx_hash)
                print(log)
            n_clearings_done += n_clearings_current
            t_clearing_current = t_clearing_first + interval_clearing * n_clearings_done
            n_clearings_current = limit_clearings
        except ValueError as e:
            print(e)
            n_clearings_current = int(n_clearings_current * 0.75)
            update_balances = False

    market_results_blockchain = blockchain_utils.functions.getMarketResultsTotal().call()
    return market_results_blockchain


# Convert results on the blockchain to a pandas dataframe
def convertToPdFinalMarketResults(market_results_blockchain, market_results_python):
    market_results_blockchain = blockchain_utils.convertListToPdDataFrame(market_results_blockchain,
                                                                          market_results_python.columns.to_list())
    return market_results_blockchain


if __name__ == '__main__':

    # load configuration file
    with open(scenario_file_path) as config_file:
        config_example = yaml.load(config_file, Loader=yaml.FullLoader)
    # Create a db connection object
    db_obj_example = db_connection.DatabaseConnection(
        db_dict=config_example['db_connections']['database_connection_admin'],
        lem_config=config_example['lem'])
    # Initialize database
    db_obj_example.init_db(clear_tables=True, reformat_tables=True)
    # Create list of random user ids and meter ids
    ids_users_random = create_user_ids(num=config_example['prosumer']['general_number_of'])
    ids_meter_random = create_user_ids(num=config_example['prosumer']['general_number_of'])
    # Register meters and users on database
    for z in range(len(ids_users_random)):
        df_insert = pd.DataFrame(data=[[ids_users_random[z], 1000, 0, 10000, 100, 'green', 10, 'zi', 0, 0, 0]],
                                 columns=db_obj_example.get_table_columns(db_obj_example.db_param.NAME_TABLE_INFO_USER))
        db_obj_example.register_user(df_in=df_insert)
        df_insert = pd.DataFrame(
            data=[[ids_meter_random[z], ids_users_random[z], 1, 0, 'aggregator', 'green', 0, 0, 'test']],
            columns=db_obj_example.get_table_columns(db_obj_example.db_param.NAME_TABLE_INFO_METER))
        db_obj_example.register_meter(df_in=df_insert)
    # Compute random market positions
    df_positions = create_random_positions(db_obj=db_obj_example,
                                           config=config_example,
                                           ids_user=ids_users_random,
                                           n_positions=1000,
                                           verbose=False)
    # Post positions to market
    db_obj_example.post_positions(df_positions)
    # run clearings and save to files
    _, _, _, timing = market_clearing(db_obj=db_obj_example,
                                      config_lem=config_example['lem'],
                                      config_supplier=config_example['supplier'],
                                      plotting=False,
                                      verbose=True
                                      )

    print(timing)
