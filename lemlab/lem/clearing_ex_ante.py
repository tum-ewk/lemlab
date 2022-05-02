"""
The lem module contains all functions related to the market clearing.
"""

__author__ = "michelzade"
__credits__ = []
__license__ = ""
__maintainer__ = "michelzade"
__email__ = "michel.zade@tum.de"

from lemlab.db_connection import db_connection
from collections import OrderedDict
from tqdm import tqdm
from ruamel.yaml import YAML
import time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import traceback
import random
import string


def market_clearing(db_obj,
                    config_lem,
                    config_retailer=None,
                    t_override=None,
                    plotting=False,
                    verbose=False):
    """
    Function clears all offers and bids from database and writes stores unmatched and matched bids back in database.
    @param db_obj: database connection object
    @param config_lem: configuration dictionary of local energy market
    @param config_retailer: configuration dictionary of retailer
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
        config_lem['types_clearing_ex_ante'] = {0: "pda"}

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

    bids = convert_qualities_to_int(db_obj, bids, config_lem['types_quality'])
    offers = convert_qualities_to_int(db_obj, offers, config_lem['types_quality'])
    results_clearing_all = {}
    time_clearing_execution = {}

    # for-loop for all specified clearing types
    for j in range(len(config_lem['types_clearing_ex_ante'])):
        type_clearing = config_lem['types_clearing_ex_ante'][j]
        # Set clearing time
        t_clearing_start = round(time.time())
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
                # Check whether this is the first clearing period and whether the flag retailer bids is true
                if config_retailer is not None:
                    # Insert retailer bids and offers
                    bids_ts_d, offers_ts_d = _add_retailer_bids(db_obj,
                                                                config_retailer,
                                                                t_clearing_current,
                                                                bids_ts_d,
                                                                offers_ts_d)

                plotting_title = pd.Timestamp(t_clearing_current, unit="s", tz="Europe/Berlin")

                # Combinations WITHOUT consideration of quality premium
                if 'pda' == type_clearing:
                    positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                        clearing_pda(db_obj,
                                     config_lem,
                                     offers_ts_d,
                                     bids_ts_d,
                                     plotting=plotting,
                                     plotting_title=plotting_title)

                if 'h2l' == type_clearing:
                    positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                        clearing_pp(db_obj,
                                    config_lem,
                                    offers_ts_d,
                                    bids_ts_d,
                                    type_prioritization='h2l',
                                    plotting=plotting,
                                    plotting_title=plotting_title)

                if 'l2h' == type_clearing:
                    positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                        clearing_pp(db_obj,
                                    config_lem,
                                    offers_ts_d,
                                    bids_ts_d,
                                    type_prioritization='l2h',
                                    plotting=plotting,
                                    plotting_title=plotting_title)

                if 'sep' == type_clearing:
                    positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                        clearing_pp(db_obj,
                                    config_lem,
                                    offers_ts_d,
                                    bids_ts_d,
                                    type_prioritization='sep',
                                    plotting=plotting,
                                    plotting_title=plotting_title)

                if 'cc' == type_clearing:
                    positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                        clearing_cc(db_obj,
                                    config_lem,
                                    offers_ts_d,
                                    bids_ts_d,
                                    plotting=plotting,
                                    plotting_title=plotting_title,
                                    verbose=verbose)

                if 'cc_h2l' == type_clearing:
                    positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                        clearing_cc(db_obj,
                                    config_lem,
                                    offers_ts_d,
                                    bids_ts_d,
                                    plotting=plotting,
                                    plotting_title=plotting_title,
                                    verbose=verbose)

                    if not bids_uncleared.empty and not offers_uncleared.empty:
                        # Clear in a preference prioritization for remaining positions
                        results_pp, offers_uncleared_pp, bids_uncleared_pp, offers_cleared_pp, bids_cleared_pp = \
                            clearing_pp(db_obj=db_obj,
                                        config_lem=config_lem,
                                        offers=offers_uncleared,
                                        bids=bids_uncleared,
                                        type_clearing=type_clearing,
                                        type_prioritization='h2l',
                                        plotting=plotting,
                                        plotting_title=f'{plotting_title}; pref. satis.')
                        positions_cleared = pd.concat([positions_cleared, results_pp]).reset_index(drop=True)

                if 'cc_l2h' == type_clearing:
                    positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                        clearing_cc(db_obj,
                                    config_lem,
                                    offers_ts_d,
                                    bids_ts_d,
                                    plotting=plotting,
                                    plotting_title=plotting_title,
                                    verbose=verbose)

                    if not bids_uncleared.empty and not offers_uncleared.empty:
                        # Clear in a preference prioritization for remaining positions
                        results_pp, offers_uncleared_pp, bids_uncleared_pp, offers_cleared_pp, bids_cleared_pp = \
                            clearing_pp(db_obj=db_obj,
                                        config_lem=config_lem,
                                        offers=offers_uncleared,
                                        bids=bids_uncleared,
                                        type_clearing=type_clearing,
                                        type_prioritization='l2h',
                                        plotting=plotting,
                                        plotting_title=f'{plotting_title}; pref. satis.')
                        positions_cleared = pd.concat([positions_cleared, results_pp]).reset_index(drop=True)

                if 'cc_sep' == type_clearing:
                    positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                        clearing_cc(db_obj,
                                    config_lem,
                                    offers_ts_d,
                                    bids_ts_d,
                                    plotting=plotting,
                                    plotting_title=plotting_title,
                                    verbose=verbose)

                    if not bids_uncleared.empty and not offers_uncleared.empty:
                        # Clear in a preference prioritization for remaining positions
                        results_pp, offers_uncleared_pp, bids_uncleared_pp, offers_cleared_pp, bids_cleared_pp = \
                            clearing_pp(db_obj=db_obj,
                                        config_lem=config_lem,
                                        offers=offers_uncleared,
                                        bids=bids_uncleared,
                                        type_clearing=type_clearing,
                                        type_prioritization='sep',
                                        plotting=plotting,
                                        plotting_title=f'{plotting_title}; pref. satis.')
                        positions_cleared = pd.concat([positions_cleared, results_pp]).reset_index(drop=True)

                if 'sep_cc' == type_clearing:
                    positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                        clearing_pp(db_obj,
                                    config_lem,
                                    offers_ts_d,
                                    bids_ts_d,
                                    type_prioritization='sep',
                                    plotting=plotting,
                                    plotting_title=plotting_title)

                    if not bids_uncleared.empty and not offers_uncleared.empty:
                        results_ps, offers_uncleared_ps, bids_uncleared_ps, offers_cleared_ps, bids_cleared_ps = \
                            clearing_cc(db_obj,
                                        config_lem,
                                        offers_uncleared,
                                        bids_uncleared,
                                        plotting=plotting,
                                        plotting_title=plotting_title,
                                        verbose=verbose)
                        positions_cleared = pd.concat([positions_cleared, results_ps]).reset_index(drop=True)

                if 'l2h_cc' == type_clearing:
                    positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                        clearing_pp(db_obj,
                                    config_lem,
                                    offers_ts_d,
                                    bids_ts_d,
                                    type_prioritization='l2h',
                                    plotting=plotting,
                                    plotting_title=plotting_title)

                    if not bids_uncleared.empty and not offers_uncleared.empty:
                        results_ps, offers_uncleared_ps, bids_uncleared_ps, offers_cleared_ps, bids_cleared_ps = clearing_cc(
                            db_obj,
                            config_lem,
                            offers_uncleared,
                            bids_uncleared,
                            plotting=plotting,
                            plotting_title=plotting_title,
                            verbose=verbose)
                        positions_cleared = pd.concat([positions_cleared, results_ps]).reset_index(drop=True)

                if 'h2l_cc' == type_clearing:
                    positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                        clearing_pp(db_obj,
                                    config_lem,
                                    offers_ts_d,
                                    bids_ts_d,
                                    type_prioritization='h2l',
                                    plotting=plotting,
                                    plotting_title=plotting_title)

                    if not bids_uncleared.empty and not offers_uncleared.empty:
                        results_ps, offers_uncleared_ps, bids_uncleared_ps, offers_cleared_ps, bids_cleared_ps = \
                            clearing_cc(db_obj,
                                        config_lem,
                                        offers_uncleared,
                                        bids_uncleared,
                                        plotting=plotting,
                                        plotting_title=plotting_title,
                                        verbose=verbose)
                        positions_cleared = pd.concat([positions_cleared, results_ps]).reset_index(drop=True)

                # Combinations WITH consideration of quality premium ###
                # Standard da AFTER advanced clearing
                if 'h2l_pda' == type_clearing:
                    positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                        clearing_pp(db_obj,
                                    config_lem,
                                    offers_ts_d,
                                    bids_ts_d,
                                    type_prioritization='h2l',
                                    add_premium=True,
                                    plotting=plotting,
                                    plotting_title=plotting_title)

                    positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                        clearing_pda(db_obj,
                                     config_lem,
                                     offers_uncleared,
                                     bids_uncleared,
                                     add_premium=False,
                                     plotting=plotting,
                                     plotting_title=plotting_title)

                    positions_cleared = pd.concat([positions_cleared, positions_cleared_da], ignore_index=True)

                if 'l2h_pda' == type_clearing:
                    positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                        clearing_pp(db_obj,
                                    config_lem,
                                    offers_ts_d,
                                    bids_ts_d,
                                    type_prioritization='l2h',
                                    add_premium=True,
                                    plotting=plotting,
                                    plotting_title=plotting_title)

                    positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                        clearing_pda(db_obj,
                                     config_lem,
                                     offers_uncleared,
                                     bids_uncleared,
                                     add_premium=False,
                                     plotting=plotting,
                                     plotting_title=plotting_title)

                    positions_cleared = pd.concat([positions_cleared, positions_cleared_da], ignore_index=True)

                if 'sep_pda' == type_clearing:
                    positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                        clearing_pp(db_obj,
                                    config_lem,
                                    offers_ts_d,
                                    bids_ts_d,
                                    type_prioritization='sep',
                                    add_premium=True,
                                    plotting=plotting,
                                    plotting_title=plotting_title)

                    positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                        clearing_pda(db_obj,
                                     config_lem,
                                     offers_uncleared,
                                     bids_uncleared,
                                     add_premium=False,
                                     plotting=plotting,
                                     plotting_title=plotting_title)

                    positions_cleared = pd.concat([positions_cleared, positions_cleared_da], ignore_index=True)

                if 'cc_pda' == type_clearing:
                    positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                        clearing_cc(db_obj,
                                    config_lem,
                                    offers_ts_d,
                                    bids_ts_d,
                                    add_premium=True,
                                    plotting=plotting,
                                    plotting_title=plotting_title,
                                    verbose=verbose)

                    positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                        clearing_pda(db_obj,
                                     config_lem,
                                     offers_uncleared,
                                     bids_uncleared,
                                     add_premium=False,
                                     plotting=plotting,
                                     plotting_title=plotting_title)

                    positions_cleared = pd.concat([positions_cleared, positions_cleared_da], ignore_index=True)

                if 'cc_h2l_pda' == type_clearing:
                    positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                        clearing_cc(db_obj,
                                    config_lem,
                                    offers_ts_d,
                                    bids_ts_d,
                                    add_premium=True,
                                    plotting=plotting,
                                    plotting_title=plotting_title,
                                    verbose=verbose)

                    if not bids_uncleared.empty and not offers_uncleared.empty:
                        # Clear in a preference prioritization for remaining positions
                        results_pp, offers_uncleared_pp, bids_uncleared_pp, offers_cleared_pp, bids_cleared_pp = \
                            clearing_pp(db_obj=db_obj,
                                        config_lem=config_lem,
                                        offers=offers_uncleared,
                                        bids=bids_uncleared,
                                        type_clearing=type_clearing,
                                        add_premium=True,
                                        type_prioritization='h2l',
                                        plotting=plotting,
                                        plotting_title=f'{plotting_title}; pref. satis.')
                        positions_cleared = pd.concat([positions_cleared, results_pp]).reset_index(drop=True)

                        positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                            clearing_pda(db_obj,
                                         config_lem,
                                         offers_uncleared_pp,
                                         bids_uncleared_pp,
                                         add_premium=False,
                                         plotting=plotting,
                                         plotting_title=plotting_title)

                        positions_cleared = pd.concat([positions_cleared, positions_cleared_da], ignore_index=True)

                if 'cc_l2h_pda' == type_clearing:
                    positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                        clearing_cc(db_obj,
                                    config_lem,
                                    offers_ts_d,
                                    bids_ts_d,
                                    add_premium=True,
                                    plotting=plotting,
                                    plotting_title=plotting_title,
                                    verbose=verbose)

                    if not bids_uncleared.empty and not offers_uncleared.empty:
                        # Clear in a preference prioritization for remaining positions
                        results_pp, offers_uncleared_pp, bids_uncleared_pp, offers_cleared_pp, bids_cleared_pp = \
                            clearing_pp(db_obj=db_obj,
                                        config_lem=config_lem,
                                        offers=offers_uncleared,
                                        bids=bids_uncleared,
                                        type_clearing=type_clearing,
                                        add_premium=True,
                                        type_prioritization='l2h',
                                        plotting=plotting,
                                        plotting_title=f'{plotting_title}; pref. satis.')
                        positions_cleared = pd.concat([positions_cleared, results_pp]).reset_index(drop=True)

                        positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                            clearing_pda(db_obj,
                                         config_lem,
                                         offers_uncleared_pp,
                                         bids_uncleared_pp,
                                         add_premium=False,
                                         plotting=plotting,
                                         plotting_title=plotting_title)

                        positions_cleared = pd.concat([positions_cleared, positions_cleared_da], ignore_index=True)

                if 'cc_sep_pda' == type_clearing:
                    positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                        clearing_cc(db_obj,
                                    config_lem,
                                    offers_ts_d,
                                    bids_ts_d,
                                    add_premium=True,
                                    plotting=plotting,
                                    plotting_title=plotting_title,
                                    verbose=verbose)

                    if not bids_uncleared.empty and not offers_uncleared.empty:
                        # Clear in a preference prioritization for remaining positions
                        results_pp, offers_uncleared_pp, bids_uncleared_pp, offers_cleared_pp, bids_cleared_pp = \
                            clearing_pp(db_obj=db_obj,
                                        config_lem=config_lem,
                                        offers=offers_uncleared,
                                        bids=bids_uncleared,
                                        type_clearing=type_clearing,
                                        add_premium=True,
                                        type_prioritization='sep',
                                        plotting=plotting,
                                        plotting_title=f'{plotting_title}; pref. satis.')
                        positions_cleared = pd.concat([positions_cleared, results_pp]).reset_index(drop=True)

                        positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                            clearing_pda(db_obj,
                                         config_lem,
                                         offers_uncleared_pp,
                                         bids_uncleared_pp,
                                         add_premium=False,
                                         plotting=plotting,
                                         plotting_title=plotting_title)

                        positions_cleared = pd.concat([positions_cleared, positions_cleared_da], ignore_index=True)

                if 'sep_cc_pda' == type_clearing:
                    positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                        clearing_pp(db_obj,
                                    config_lem,
                                    offers_ts_d,
                                    bids_ts_d,
                                    type_prioritization='sep',
                                    plotting=plotting,
                                    plotting_title=plotting_title)

                    if not bids_uncleared.empty and not offers_uncleared.empty:
                        results_ps, offers_uncleared_ps, bids_uncleared_ps, offers_cleared_ps, bids_cleared_ps = \
                            clearing_cc(db_obj,
                                        config_lem,
                                        offers_uncleared,
                                        bids_uncleared,
                                        plotting=plotting,
                                        plotting_title=plotting_title,
                                        verbose=verbose)
                        positions_cleared = pd.concat([positions_cleared, results_ps]).reset_index(drop=True)

                        positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                            clearing_pda(db_obj,
                                         config_lem,
                                         offers_uncleared_ps,
                                         bids_uncleared_ps,
                                         add_premium=False,
                                         plotting=plotting,
                                         plotting_title=plotting_title)

                        positions_cleared = pd.concat([positions_cleared, positions_cleared_da], ignore_index=True)

                if 'l2h_cc_pda' == type_clearing:
                    positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                        clearing_pp(db_obj,
                                    config_lem,
                                    offers_ts_d,
                                    bids_ts_d,
                                    type_prioritization='l2h',
                                    plotting=plotting,
                                    plotting_title=plotting_title)

                    if not bids_uncleared.empty and not offers_uncleared.empty:
                        results_ps, offers_uncleared_ps, bids_uncleared_ps, offers_cleared_ps, bids_cleared_ps = \
                            clearing_cc(db_obj,
                                        config_lem,
                                        offers_uncleared,
                                        bids_uncleared,
                                        plotting=plotting,
                                        plotting_title=plotting_title,
                                        verbose=verbose)
                        positions_cleared = pd.concat([positions_cleared, results_ps]).reset_index(drop=True)

                        positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                            clearing_pda(db_obj,
                                         config_lem,
                                         offers_uncleared_ps,
                                         bids_uncleared_ps,
                                         add_premium=False,
                                         plotting=plotting,
                                         plotting_title=plotting_title)

                        positions_cleared = pd.concat([positions_cleared, positions_cleared_da], ignore_index=True)

                if 'h2l_cc_pda' == type_clearing:
                    positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                        clearing_pp(db_obj,
                                    config_lem,
                                    offers_ts_d,
                                    bids_ts_d,
                                    type_prioritization='h2l',
                                    plotting=plotting,
                                    plotting_title=plotting_title)

                    if not bids_uncleared.empty and not offers_uncleared.empty:
                        results_ps, offers_uncleared_ps, bids_uncleared_ps, offers_cleared_ps, bids_cleared_ps = \
                            clearing_cc(db_obj,
                                        config_lem,
                                        offers_uncleared,
                                        bids_uncleared,
                                        plotting=plotting,
                                        plotting_title=plotting_title,
                                        verbose=verbose)
                        positions_cleared = pd.concat([positions_cleared, results_ps]).reset_index(drop=True)

                        positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                            clearing_pda(db_obj,
                                         config_lem,
                                         offers_uncleared_ps,
                                         bids_uncleared_ps,
                                         add_premium=False,
                                         plotting=plotting,
                                         plotting_title=plotting_title)

                        positions_cleared = pd.concat([positions_cleared, positions_cleared_da], ignore_index=True)

                # Standard pda BEFORE advanced clearing
                if 'pda_h2l' == type_clearing:
                    positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                        clearing_pda(db_obj,
                                     config_lem,
                                     offers_ts_d,
                                     bids_ts_d,
                                     add_premium=False,
                                     plotting=plotting,
                                     plotting_title=plotting_title)

                    positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                        clearing_pp(db_obj,
                                    config_lem,
                                    offers_uncleared_da,
                                    bids_uncleared_da,
                                    type_prioritization='pref_h2l',
                                    add_premium=True,
                                    plotting=plotting,
                                    plotting_title=plotting_title)

                    positions_cleared = pd.concat([positions_cleared, positions_cleared_da], ignore_index=True)

                if 'pda_l2h' == type_clearing:
                    positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                        clearing_pda(db_obj,
                                     config_lem,
                                     offers_ts_d,
                                     bids_ts_d,
                                     add_premium=False,
                                     plotting=plotting,
                                     plotting_title=plotting_title)

                    positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                        clearing_pp(db_obj,
                                    config_lem,
                                    offers_uncleared_da,
                                    bids_uncleared_da,
                                    type_prioritization='l2h',
                                    add_premium=True,
                                    plotting=plotting,
                                    plotting_title=plotting_title)

                    positions_cleared = pd.concat([positions_cleared, positions_cleared_da], ignore_index=True)

                if 'pda_sep' == type_clearing:
                    positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                        clearing_pda(db_obj,
                                     config_lem,
                                     offers_ts_d,
                                     bids_ts_d,
                                     add_premium=False,
                                     plotting=plotting,
                                     plotting_title=plotting_title)

                    positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                        clearing_pp(db_obj,
                                    config_lem,
                                    offers_uncleared_da,
                                    bids_uncleared_da,
                                    type_prioritization='sep',
                                    add_premium=True,
                                    plotting=plotting,
                                    plotting_title=plotting_title)

                    positions_cleared = pd.concat([positions_cleared, positions_cleared_da], ignore_index=True)

                if 'pda_cc' == type_clearing:
                    positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                        clearing_pda(db_obj,
                                     config_lem,
                                     offers_ts_d,
                                     bids_ts_d,
                                     add_premium=False,
                                     plotting=plotting,
                                     plotting_title=plotting_title)

                    positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                        clearing_cc(db_obj,
                                    config_lem,
                                    offers=offers_uncleared_da,
                                    bids=bids_uncleared_da,
                                    add_premium=True,
                                    plotting=plotting,
                                    plotting_title=plotting_title,
                                    verbose=verbose)

                    positions_cleared = pd.concat([positions_cleared, positions_cleared_da], ignore_index=True)

                if 'pda_cc_h2l' == type_clearing:
                    positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                        clearing_pda(db_obj,
                                     config_lem,
                                     offers_ts_d,
                                     bids_ts_d,
                                     add_premium=False,
                                     plotting=plotting,
                                     plotting_title=plotting_title)

                    if not offers_uncleared_da.empty and bids_uncleared_da.empty:
                        results_ps, offers_uncleared_ps, bids_uncleared_ps, offers_cleared_ps, bids_cleared_ps = \
                            clearing_cc(db_obj,
                                        config_lem,
                                        offers=offers_uncleared_da,
                                        bids=bids_uncleared_da,
                                        add_premium=True,
                                        plotting=plotting,
                                        plotting_title=plotting_title,
                                        verbose=verbose)

                        positions_cleared = pd.concat([results_ps, positions_cleared_da], ignore_index=True)

                        results_pp, offers_uncleared_pp, bids_uncleared_pp, offers_cleared_pp, bids_cleared_pp = \
                            clearing_pp(db_obj=db_obj,
                                        config_lem=config_lem,
                                        offers=offers_uncleared_ps,
                                        bids=bids_uncleared_ps,
                                        type_clearing=type_clearing,
                                        add_premium=True,
                                        type_prioritization='h2l',
                                        plotting=plotting,
                                        plotting_title=f'{plotting_title}; pref. satis.')
                        positions_cleared = pd.concat([positions_cleared, results_pp]).reset_index(drop=True)

                if 'pda_cc_l2h' == type_clearing:
                    positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                        clearing_pda(db_obj,
                                     config_lem,
                                     offers_ts_d,
                                     bids_ts_d,
                                     add_premium=False,
                                     plotting=plotting,
                                     plotting_title=plotting_title)

                    if not offers_uncleared_da.empty and bids_uncleared_da.empty:
                        results_ps, offers_uncleared_ps, bids_uncleared_ps, offers_cleared_ps, bids_cleared_ps = \
                            clearing_cc(db_obj,
                                        config_lem,
                                        offers=offers_uncleared_da,
                                        bids=bids_uncleared_da,
                                        add_premium=True,
                                        plotting=plotting,
                                        plotting_title=plotting_title,
                                        verbose=verbose)

                        positions_cleared = pd.concat([results_ps, positions_cleared_da], ignore_index=True)

                        results_pp, offers_uncleared_pp, bids_uncleared_pp, offers_cleared_pp, bids_cleared_pp = \
                            clearing_pp(db_obj=db_obj,
                                        config_lem=config_lem,
                                        offers=offers_uncleared_ps,
                                        bids=bids_uncleared_ps,
                                        type_clearing=type_clearing,
                                        add_premium=True,
                                        type_prioritization='l2h',
                                        plotting=plotting,
                                        plotting_title=f'{plotting_title}; pref. satis.')
                        positions_cleared = pd.concat([positions_cleared, results_pp]).reset_index(drop=True)

                if 'pda_cc_sep' == type_clearing:
                    positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                        clearing_pda(db_obj,
                                     config_lem,
                                     offers_ts_d,
                                     bids_ts_d,
                                     add_premium=False,
                                     plotting=plotting,
                                     plotting_title=plotting_title)

                    if not offers_uncleared_da.empty and bids_uncleared_da.empty:
                        results_ps, offers_uncleared_ps, bids_uncleared_ps, offers_cleared_ps, bids_cleared_ps = \
                            clearing_cc(db_obj,
                                        config_lem,
                                        offers=offers_uncleared_da,
                                        bids=bids_uncleared_da,
                                        add_premium=True,
                                        plotting=plotting,
                                        plotting_title=plotting_title,
                                        verbose=verbose)

                        positions_cleared = pd.concat([results_ps, positions_cleared_da], ignore_index=True)

                        results_pp, offers_uncleared_pp, bids_uncleared_pp, offers_cleared_pp, bids_cleared_pp = \
                            clearing_pp(db_obj=db_obj,
                                        config_lem=config_lem,
                                        offers=offers_uncleared_ps,
                                        bids=bids_uncleared_ps,
                                        type_clearing=type_clearing,
                                        add_premium=True,
                                        type_prioritization='sep',
                                        plotting=plotting,
                                        plotting_title=f'{plotting_title}; pref. satis.')
                        positions_cleared = pd.concat([positions_cleared, results_pp]).reset_index(drop=True)

                if 'pda_h2l_cc' == type_clearing:
                    positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                        clearing_pda(db_obj,
                                     config_lem,
                                     offers_ts_d,
                                     bids_ts_d,
                                     add_premium=False,
                                     plotting=plotting,
                                     plotting_title=plotting_title)

                    if not offers_uncleared_da.empty and bids_uncleared_da.empty:
                        results_pp, offers_uncleared_pp, bids_uncleared_pp, offers_cleared_pp, bids_cleared_pp = \
                            clearing_pp(db_obj=db_obj,
                                        config_lem=config_lem,
                                        offers=offers_uncleared_da,
                                        bids=bids_uncleared_da,
                                        type_clearing=type_clearing,
                                        add_premium=True,
                                        type_prioritization='h2l',
                                        plotting=plotting,
                                        plotting_title=f'{plotting_title}; pref. satis.')
                        positions_cleared = pd.concat([positions_cleared_da, results_pp]).reset_index(drop=True)

                        results_ps, offers_uncleared_ps, bids_uncleared_ps, offers_cleared_ps, bids_cleared_ps = \
                            clearing_cc(db_obj,
                                        config_lem,
                                        offers=offers_uncleared_pp,
                                        bids=bids_uncleared_pp,
                                        add_premium=True,
                                        plotting=plotting,
                                        plotting_title=plotting_title,
                                        verbose=verbose)

                        positions_cleared = pd.concat([positions_cleared, results_ps], ignore_index=True)

                if 'pda_l2h_cc' == type_clearing:
                    positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                        clearing_pda(db_obj,
                                     config_lem,
                                     offers_ts_d,
                                     bids_ts_d,
                                     add_premium=False,
                                     plotting=plotting,
                                     plotting_title=plotting_title)

                    if not offers_uncleared_da.empty and bids_uncleared_da.empty:
                        results_pp, offers_uncleared_pp, bids_uncleared_pp, offers_cleared_pp, bids_cleared_pp = \
                            clearing_pp(db_obj=db_obj,
                                        config_lem=config_lem,
                                        offers=offers_uncleared_da,
                                        bids=bids_uncleared_da,
                                        type_clearing=type_clearing,
                                        add_premium=True,
                                        type_prioritization='l2h',
                                        plotting=plotting,
                                        plotting_title=f'{plotting_title}; pref. satis.')
                        positions_cleared = pd.concat([positions_cleared_da, results_pp]).reset_index(drop=True)

                        results_ps, offers_uncleared_ps, bids_uncleared_ps, offers_cleared_ps, bids_cleared_ps = \
                            clearing_cc(db_obj,
                                        config_lem,
                                        offers=offers_uncleared_pp,
                                        bids=bids_uncleared_pp,
                                        add_premium=True,
                                        plotting=plotting,
                                        plotting_title=plotting_title,
                                        verbose=verbose)

                        positions_cleared = pd.concat([positions_cleared, results_ps], ignore_index=True)

                if 'pda_sep_cc' == type_clearing:
                    positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                        clearing_pda(db_obj,
                                     config_lem,
                                     offers_ts_d,
                                     bids_ts_d,
                                     add_premium=False,
                                     plotting=plotting,
                                     plotting_title=plotting_title)

                    if not offers_uncleared_da.empty and bids_uncleared_da.empty:
                        results_pp, offers_uncleared_pp, bids_uncleared_pp, offers_cleared_pp, bids_cleared_pp = \
                            clearing_pp(db_obj=db_obj,
                                        config_lem=config_lem,
                                        offers=offers_uncleared_da,
                                        bids=bids_uncleared_da,
                                        type_clearing=type_clearing,
                                        add_premium=True,
                                        type_prioritization='sep',
                                        plotting=plotting,
                                        plotting_title=f'{plotting_title}; pref. satis.')
                        positions_cleared = pd.concat([positions_cleared_da, results_pp]).reset_index(drop=True)

                        results_ps, offers_uncleared_ps, bids_uncleared_ps, offers_cleared_ps, bids_cleared_ps = \
                            clearing_cc(db_obj,
                                        config_lem,
                                        offers=offers_uncleared_pp,
                                        bids=bids_uncleared_pp,
                                        add_premium=True,
                                        plotting=plotting,
                                        plotting_title=plotting_title,
                                        verbose=verbose)

                        positions_cleared = pd.concat([positions_cleared, results_ps], ignore_index=True)

                # Check whether market has cleared a volume
                if not positions_cleared.empty:
                    if config_lem['share_quality_logging_extended']:
                        positions_cleared = calc_market_position_shares(db_obj, config_lem,
                                                                        offers_ts_d, bids_ts_d, positions_cleared)
                    results_clearing = pd.concat([results_clearing, positions_cleared], ignore_index=True)

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
    return results_clearing_all, offers, bids, time_clearing_execution


def clearing_pda(db_obj,
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
            for i in config_lem['types_quality']:
                type_quality = config_lem['types_quality'][i]
                # Shares of offers in cleared positions
                positions_cleared_quality_offer = positions_cleared.loc[
                    positions_cleared[db_obj.db_param.QUALITY_ENERGY_OFFER] == i]
                qty_energy_cleared_quality_offer = positions_cleared_quality_offer[
                    db_obj.db_param.QTY_ENERGY_TRADED].sum()
                positions_cleared = positions_cleared.assign(
                    **{db_obj.db_param.SHARE_QUALITY_OFFERS_CLEARED_ + type_quality: round(
                        qty_energy_cleared_quality_offer / qty_energy_cleared * 100)})
                if config_lem['share_quality_logging_extended']:
                    # Shares of preferences in cleared positions
                    positions_cleared_preference_bid = positions_cleared.loc[
                        positions_cleared[db_obj.db_param.QUALITY_ENERGY_BID] == i]
                    qty_energy_cleared_preference_bid = positions_cleared_preference_bid[
                        db_obj.db_param.QTY_ENERGY_TRADED].sum()
                    positions_cleared = positions_cleared.assign(
                        **{db_obj.db_param.SHARE_PREFERENCE_BIDS_CLEARED_ + type_quality: round(
                            qty_energy_cleared_preference_bid / qty_energy_cleared * 100)})

        # Drop duplicate ts_delivery column
        positions_cleared = positions_cleared.rename(columns={'ts_delivery_offer': 'ts_delivery'})
        positions_cleared = positions_cleared.drop(columns={'ts_delivery_bid'})
        # Extract all uncleared bids_sorted/offers_sorted
        bids_offers_uncleared = positions_merged[
            positions_merged[db_obj.db_param.PRICE_ENERGY + db_obj.db_param.EXTENSION_OFFER] >
            positions_merged[db_obj.db_param.PRICE_ENERGY + db_obj.db_param.EXTENSION_BID]]
        bids_offers_uncleared = pd.concat([bids_offers_uncleared,
            positions_merged[positions_merged.isna().any(axis=1)]])
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


def clearing_cc(db_obj,
                config_lem,
                offers,
                bids,
                type_clearing=None,
                max_while_executions=None,
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
    @param max_while_executions: maximum number of while loop iterations
    @param add_premium: boolean value to add premium to bid price
    @param plotting: boolean value to plot clearing results
    @param plotting_title: title of plot, ignored if plotting is false
    @param verbose: boolean value to print execution information
    @return: returns cleared bids and offers dataframe
    """
    offers_uncleared = pd.DataFrame()
    bids_uncleared = pd.DataFrame()
    offers_cleared = pd.DataFrame()
    bids_cleared = pd.DataFrame()
    positions_cleared = pd.DataFrame()
    # Check whether bids or offers are empty
    if bids.empty or offers.empty or \
            bids[bids[db_obj.db_param.QTY_ENERGY] > 0].empty or \
            offers[offers[db_obj.db_param.QTY_ENERGY] > 0].empty:
        return positions_cleared
    if type_clearing is None:
        type_clearing = 'cc'
    if max_while_executions is None:
        max_while_executions = 1000
    try:
        # Extract uniques qualities
        unique_qualities = np.unique(
            np.concatenate(
                (offers[db_obj.db_param.QUALITY_ENERGY].unique(), bids[db_obj.db_param.QUALITY_ENERGY].unique())))
        # Initiate while loop variables
        bids_unsatisfied = True
        bids_cleared_q_satisfied_all = pd.DataFrame()
        bids_cld_q_all_unsatisfied_total = pd.DataFrame()
        bids_remaining = pd.DataFrame()
        counter = 0
        while bids_unsatisfied:
            t_while_start = time.time()
            # Check whether remaining bids are empty and whether counter has exceeded maximum while executions
            if bids_remaining.empty and counter > 0 or counter > max_while_executions:
                bids_uncleared = bids
                offers_uncleared = offers
                positions_cleared = pd.DataFrame()
                offers_cleared = pd.DataFrame()
                bids_cleared = pd.DataFrame()
                # bids_cld_q_all_unsatisfied = pd.DataFrame()
                if verbose:
                    print('Preferences of bids can not be satisfied.')
                break
            elif counter == 0:
                bids_remaining = bids
            # Reset unsatisfied bids every time!
            bids_cld_q_all_unsatisfied = pd.DataFrame()
            # Clearing
            positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                clearing_pda(db_obj=db_obj, config_lem=config_lem, offers=offers, bids=bids_remaining,
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
                bids_cleared_q_satisfied_all = pd.concat([bids_cleared_q_satisfied_all, bids_cleared_q_satisfied])
                if not bids_cleared_q[db_obj.db_param.QTY_ENERGY].sum() <= qty_offers_max:
                    bids_cld_q_all_unsatisfied = pd.concat([bids_cld_q_all_unsatisfied,
                        bids_cleared_q[bids_cleared_q['CumEnQ'] > qty_offers_max]])

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
            bids_cld_q_all_unsatisfied_total = pd.concat([bids_cld_q_all_unsatisfied_total, bids_cld_q_all_unsatisfied])
            bids_remaining = _aggregate_identical_positions(db_obj=db_obj,
                                                            positions=bids_remaining,
                                                            subset=[db_obj.db_param.TS_DELIVERY,
                                                                    db_obj.db_param.ID_USER,
                                                                    db_obj.db_param.PRICE_ENERGY,
                                                                    db_obj.db_param.QUALITY_ENERGY])
            # Remove all unsatisfied bids from all bids
            for i, row in bids_cld_q_all_unsatisfied.iterrows():
                if add_premium:
                    _df_bid_removal = bids_remaining[
                        ((bids_remaining[db_obj.db_param.PRICE_ENERGY] + bids_remaining[db_obj.db_param.PRICE_ENERGY] *
                          bids_remaining[db_obj.db_param.PREMIUM_PREFERENCE_QUALITY] / 100).astype(int) == row[
                             db_obj.db_param.PRICE_ENERGY]) &
                        (bids_remaining[db_obj.db_param.ID_USER] == row[db_obj.db_param.ID_USER]) &
                        (bids_remaining[db_obj.db_param.QUALITY_ENERGY] == row[db_obj.db_param.QUALITY_ENERGY]) &
                        (bids_remaining[db_obj.db_param.QTY_ENERGY] >= row[db_obj.db_param.QTY_ENERGY])
                        ]
                else:
                    _df_bid_removal = bids_remaining[
                        (bids_remaining[db_obj.db_param.PRICE_ENERGY] == row[db_obj.db_param.PRICE_ENERGY]) &
                        (bids_remaining[db_obj.db_param.ID_USER] == row[db_obj.db_param.ID_USER]) &
                        (bids_remaining[db_obj.db_param.QUALITY_ENERGY] == row[db_obj.db_param.QUALITY_ENERGY]) &
                        (bids_remaining[db_obj.db_param.QTY_ENERGY] >= row[db_obj.db_param.QTY_ENERGY])
                        ]
                new_qty = _df_bid_removal[db_obj.db_param.QTY_ENERGY] - row[db_obj.db_param.QTY_ENERGY]
                _df_bid_removal = _df_bid_removal.assign(**{db_obj.db_param.QTY_ENERGY: new_qty})
                bids_remaining = pd.concat([bids_remaining.drop(index=_df_bid_removal.index), _df_bid_removal])

            # Remove all bids with zero quantity
            bids_remaining = bids_remaining[bids_remaining[db_obj.db_param.QTY_ENERGY] > 0]

            print(f"While loop iteration: {counter}, time: {time.time() - t_while_start}")
            counter = counter + 1
        # Append unsatisfied bids with uncleared bids
        bids_uncleared = pd.concat([bids_uncleared, bids_cld_q_all_unsatisfied_total])
    except Exception as e:
        print(e)
        traceback.print_exc()

    return positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared


def clearing_pp(db_obj,
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
    @param type_prioritization: variable to select prioritization type ['h2l', 'l2h', 'sep']
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
        type_prioritization = 'h2l'
    if type_clearing is None:
        type_clearing = 'pp'
    if plotting_title is None:
        plotting_title = 'pp'
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
    if type_prioritization == 'h2l':
        preferences_sorted = np.flip(preferences_sorted)

    # Calculate clearing prices for every quality
    for preference in preferences_sorted:
        # Filter bids and offers by preference
        bids_temp = bids[bids[db_obj.db_param.QUALITY_ENERGY] == preference]
        offers_temp = pd.DataFrame()
        if type_prioritization == 'h2l':
            if preference == preferences_sorted[0]:
                offers_temp = offers[offers[db_obj.db_param.QUALITY_ENERGY] == preference]
            else:
                offers_temp = pd.concat([offers[offers[db_obj.db_param.QUALITY_ENERGY] == preference], offers_uncleared])
        if type_prioritization == 'l2h':
            if preference == preferences_sorted[0]:
                offers_temp = offers[offers[db_obj.db_param.QUALITY_ENERGY] >= preference]
            else:
                offers_temp = offers_uncleared[offers_uncleared[db_obj.db_param.QUALITY_ENERGY] >= preference]
        if type_prioritization == 'sep':
            offers_temp = offers[offers[db_obj.db_param.QUALITY_ENERGY] == preference]

        if offers_temp.empty or bids_temp.empty:
            offers_uncleared = offers_temp
            bids_uncleared = bids_temp
            continue
        # Calculate clearing prices
        positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
            clearing_pda(db_obj=db_obj, config_lem=config_lem, offers=offers_temp, bids=bids_temp,
                         type_clearing=type_clearing, add_premium=add_premium,
                         plotting=plotting, plotting_title=f'{plotting_title} #{preference}')
        positions_cleared_all = pd.concat([positions_cleared_all, positions_cleared]).reset_index(drop=True)

    return positions_cleared_all, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared


def calc_market_position_shares(db_obj, config_lem, offers, bids, positions_cleared):
    qty_energy_bids = bids[db_obj.db_param.QTY_ENERGY].sum()
    qty_energy_offers = offers[db_obj.db_param.QTY_ENERGY].sum()
    qty_energy_cleared = positions_cleared[db_obj.db_param.QTY_ENERGY_TRADED].sum()
    positions_cleared = positions_cleared.assign(
        **{db_obj.db_param.QTY_ENERGY_TRADED_CUM: qty_energy_cleared})
    positions_cleared = positions_cleared.assign(
        **{db_obj.db_param.QTY_ENERGY_BIDS_CUM: qty_energy_bids})
    positions_cleared = positions_cleared.assign(
        **{db_obj.db_param.QTY_ENERGY_OFFERS_CUM: qty_energy_offers})
    # Calculate shares of labelled energy of cleared positions
    for i in config_lem['types_quality']:
        type_quality = config_lem['types_quality'][i]
        # Shares of preferences in all positions
        bids_preference = bids.loc[bids[db_obj.db_param.QUALITY_ENERGY] == i]
        qty_energy_preference_bid = bids_preference[db_obj.db_param.QTY_ENERGY].sum()
        positions_cleared = positions_cleared.assign(
            **{db_obj.db_param.SHARE_PREFERENCE_BIDS_ + type_quality: round(
                qty_energy_preference_bid / qty_energy_bids * 100)})
        # Shares of qualities in all positions
        offers_quality = offers.loc[offers[db_obj.db_param.QUALITY_ENERGY] == i]
        qty_energy_quality_offer = offers_quality[db_obj.db_param.QTY_ENERGY].sum()
        positions_cleared = positions_cleared.assign(
            **{db_obj.db_param.SHARE_QUALITY_OFFERS_ + type_quality: round(
                qty_energy_quality_offer / qty_energy_offers * 100)})

    return positions_cleared


def _add_retailer_bids(db_obj,
                       config_retailer,
                       t_clearing_current,
                       sorted_bids_t_d,
                       sorted_offers_t_d):
    """
    Adds retailer bid and offer to offers and bids.
    @param db_obj: DatabaseConnection object
    @param t_clearing_current: time of delivery for which bid and offer are inserted
    @param sorted_bids_t_d: dataframe of bids in which retailer bid is inserted
    @param sorted_offers_t_d: dataframe of offers in which retailer offer is inserted
    @return: dataframes of offers and bids
    """
    temp_df = pd.DataFrame(columns=db_obj.get_table_columns(db_obj.db_param.NAME_TABLE_POSITIONS_MARKET_EX_ANTE))
    temp_df.at[0, db_obj.db_param.T_SUBMISSION] = round(time.time())
    temp_df.at[0, db_obj.db_param.ID_USER] = config_retailer['id_user']
    temp_df.at[0, db_obj.db_param.QTY_ENERGY] = config_retailer['qty_energy_offer']
    temp_df.at[0, db_obj.db_param.TYPE_POSITION] = 0
    temp_df.at[0, db_obj.db_param.PRICE_ENERGY] = int(
        config_retailer['price_sell'] * db_obj.db_param.EURO_TO_SIGMA / 1000)
    temp_df.at[0, db_obj.db_param.QUALITY_ENERGY] = 0
    temp_df.at[0, db_obj.db_param.NUMBER_POSITION] = 0
    temp_df.at[0, db_obj.db_param.STATUS_POSITION] = 0
    temp_df.at[0, db_obj.db_param.PREMIUM_PREFERENCE_QUALITY] = 0
    temp_df.at[0, db_obj.db_param.TS_DELIVERY] = t_clearing_current
    sorted_offers_t_d = pd.concat([sorted_offers_t_d, temp_df], ignore_index=True)

    temp_df.at[0, db_obj.db_param.TYPE_POSITION] = 1
    temp_df.at[0, db_obj.db_param.PRICE_ENERGY] = int(
        config_retailer['price_buy'] * db_obj.db_param.EURO_TO_SIGMA / 1000)
    temp_df.at[0, db_obj.db_param.QTY_ENERGY] = config_retailer['qty_energy_bid']
    sorted_bids_t_d = pd.concat([sorted_bids_t_d, temp_df], ignore_index=True)

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
    positions.sort_values(by=subset, ignore_index=True, inplace=True, kind="mergesort")
    # reset index to cumulated energy quantities
    positions.set_index(positions[db_obj.db_param.QTY_ENERGY].cumsum(), inplace=True)
    # Drop duplicates that contain the same price, quality and id and keep the last of duplicates
    positions.drop_duplicates(subset=subset, keep='last', inplace=True)
    # reassign new quantities to qty_energy
    positions.assign(**{db_obj.db_param.QTY_ENERGY: [positions.index[0]] + list(np.diff(positions.index))})
    # Reset index of positions
    positions.reset_index(drop=True, inplace=True)

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
    positions_filtered = positions_merged.filter(regex=extension, axis=1).dropna(axis=0).copy()
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
                db_obj.db_param.SHARE_QUALITY_OFFERS_CLEARED_ + type_quality]})

    # Log all credit transactions
    db_obj.log_transactions(df_transactions)
    df_transactions_all = pd.concat([df_transactions_all, df_transactions], ignore_index=True)

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
    df_transactions_all = pd.concat([df_transactions_all, df_transactions], ignore_index=True)

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


def convert_qualities_to_int(db_obj, positions, dict_types):
    dict_types_inverted = {v: k for k, v in dict_types.items()}
    positions = positions.assign(**{db_obj.db_param.QUALITY_ENERGY: [dict_types_inverted[i] for i in
                                                                     positions[db_obj.db_param.QUALITY_ENERGY]]})

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


if __name__ == '__main__':

    def insert_random_positions(db_obj, config, positions, n_positions, t_d_range, ids_user):
        positions.loc[:, db_obj.db_param.ID_USER] = random.choices(ids_user, k=n_positions)
        positions.loc[:, db_obj.db_param.T_SUBMISSION] = [round(time.time())] * n_positions
        positions.loc[:, db_obj.db_param.QTY_ENERGY] = random.choices(range(1, 1000, 1), k=n_positions)
        positions.loc[:, db_obj.db_param.TYPE_POSITION] = random.choices(list(config['lem']['types_position'].values()),
                                                                         k=n_positions)
        positions.loc[:, db_obj.db_param.QUALITY_ENERGY] = random.choices(list(config['lem']['types_quality'].values()),
                                                                          k=n_positions)
        positions.loc[:, db_obj.db_param.TS_DELIVERY] = random.choices(t_d_range, k=n_positions)
        positions.loc[:, db_obj.db_param.NUMBER_POSITION] = int(0)
        positions.loc[:, db_obj.db_param.STATUS_POSITION] = int(0)
        positions.loc[positions[db_obj.db_param.TYPE_POSITION] == 'offer', db_obj.db_param.PRICE_ENERGY] = [
            int(x * db_obj.db_param.EURO_TO_SIGMA / 1000)
            for x in random.choices(np.arange(config['retailer']['price_buy'],
                                              config['retailer']['price_sell'], 0.0001),
                                    k=len(positions.loc[positions[db_obj.db_param.TYPE_POSITION] == 'offer', :]))]
        positions.loc[positions[db_obj.db_param.TYPE_POSITION] == 'offer',
                      db_obj.db_param.PREMIUM_PREFERENCE_QUALITY] = int(0)
        positions.loc[positions[db_obj.db_param.TYPE_POSITION] == 'bid', db_obj.db_param.PRICE_ENERGY] = [
            int(x * db_obj.db_param.EURO_TO_SIGMA / 1000)
            for x in random.choices(np.arange(config['retailer']['price_buy'],
                                              config['retailer']['price_sell'], 0.0001),
                                    k=len(positions.loc[positions[db_obj.db_param.TYPE_POSITION] == 'bid', :]))]
        positions.loc[positions[db_obj.db_param.TYPE_POSITION] == 'bid',
                      db_obj.db_param.PREMIUM_PREFERENCE_QUALITY] = random.choices(range(0, 50, 1), k=len(
            positions.loc[positions[db_obj.db_param.TYPE_POSITION] == 'bid', :]))

        return positions


    def create_random_positions(db_obj, config, ids_user, n_positions=None, verbose=False):
        if n_positions is None:
            n_positions = 100
        t_start = round(time.time()) - (
                round(time.time()) % config['lem']['interval_clearing']) + config['lem']['interval_clearing']
        t_end = t_start + config['lem']['horizon_clearing']
        # Range of time steps
        t_d_range = np.arange(t_start, t_end, config['lem']['interval_clearing'])
        # Create bid df
        positions = pd.DataFrame(columns=db_obj.get_table_columns(db_obj.db_param.NAME_TABLE_POSITIONS_MARKET_EX_ANTE))
        positions = insert_random_positions(db_obj, config, positions, n_positions, t_d_range, ids_user)

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


    # load configuration file
    with open(f"..\\..\\code_examples\\sim_0_config.yaml") as config_file:
        config_example = YAML().load(config_file)
    # Create a db connection object
    db_obj_example = db_connection.DatabaseConnection(
        db_dict=config_example['db_connections']['database_connection_admin'],
        lem_config=config_example['lem'])
    # Initialize database
    db_obj_example.init_db(clear_tables=True, reformat_tables=True)
    # Create list of random user ids and meter ids
    ids_users_random = create_user_ids(num=config_example['prosumer']['general_number_of'])
    ids_meter_random = create_user_ids(num=config_example['prosumer']['general_number_of'])
    ids_market_agents_random = create_user_ids(num=config_example['prosumer']['general_number_of'])
    # Register meters and users on database
    for z in range(len(ids_users_random)):
        df_insert = pd.DataFrame(data=[[ids_users_random[z], 1000, 0, 10000, 100, 'green', 10, 'zi', 0,
                                        ids_market_agents_random[z], 0, 0]],
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
                                           n_positions=10000,
                                           verbose=False)
    # Post positions to market
    db_obj_example.post_positions(df_positions)
    # run clearings and save to files
    _, _, _, timing = market_clearing(db_obj=db_obj_example,
                                      config_lem=config_example['lem'],
                                      plotting=False,
                                      verbose=True
                                      )

    print(timing)
