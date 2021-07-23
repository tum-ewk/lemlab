import multiprocessing as mp
import os
import pickle
import shutil
import time
import traceback
from pathlib import Path

import numpy as np
import pandas as pd
import yaml
from matplotlib import pyplot as plt
from tqdm import tqdm

from lem_analysis.random_lem_fcts import create_random_positions, create_user_ids
from lemlab.db_connection.db_connection import DatabaseConnection
from lemlab.platform.lem import clearing_da, clearing_pref_prio, clearing_pref_satis, calc_market_position_shares, \
    _convert_qualities_to_int


def run_clearings(db_obj,
                  config_lem,
                  offers,
                  bids,
                  path_results,
                  n_test_case=0,
                  verbose=False
                  ):
    if verbose:
        print(f'\nTest case #{n_test_case}')

    t_clearings = pd.DataFrame()
    positions_cleared = pd.DataFrame()

    for i in range(len(config_lem['types_clearing_ex_ante'])):
        t_clearing_start = round(time.time())
        type_clearing = config_lem['types_clearing_ex_ante'][i]
        # print(type_clearing)

        # Combinations WITHOUT consideration of quality premium
        if 'da' == type_clearing:
            positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                clearing_da(db_obj,
                            config_lem,
                            offers,
                            bids)

        if 'pref_prio_n_to_0' == type_clearing:
            positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                clearing_pref_prio(db_obj,
                                   config_lem,
                                   offers,
                                   bids,
                                   type_prioritization='n_to_0')

        if 'pref_prio_0_to_n' == type_clearing:
            positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                clearing_pref_prio(db_obj,
                                   config_lem,
                                   offers,
                                   bids,
                                   type_prioritization='0_to_n')

        if 'pref_prio_sep' == type_clearing:
            positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                clearing_pref_prio(db_obj,
                                   config_lem,
                                   offers,
                                   bids,
                                   type_prioritization='sep')

        if 'pref_satis' == type_clearing:
            positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                clearing_pref_satis(db_obj,
                                    config_lem,
                                    offers,
                                    bids,
                                    verbose=verbose)

        if 'pref_satis_pref_prio_n_to_0' == type_clearing:
            positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                clearing_pref_satis(db_obj,
                                    config_lem,
                                    offers,
                                    bids,
                                    verbose=verbose)

            if not bids_uncleared.empty and not offers_uncleared.empty:
                # Clear in a preference prioritization for remaining positions
                results_pp, offers_uncleared_pp, bids_uncleared_pp, offers_cleared_pp, bids_cleared_pp = \
                    clearing_pref_prio(db_obj=db_obj,
                                       config_lem=config_lem,
                                       offers=offers_uncleared,
                                       bids=bids_uncleared,
                                       type_clearing=type_clearing,
                                       type_prioritization='n_to_0')
                positions_cleared = positions_cleared.append(results_pp).reset_index(drop=True)

        if 'pref_satis_pref_prio_0_to_n' == type_clearing:
            positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                clearing_pref_satis(db_obj,
                                    config_lem,
                                    offers,
                                    bids,
                                    verbose=verbose)

            if not bids_uncleared.empty and not offers_uncleared.empty:
                # Clear in a preference prioritization for remaining positions
                results_pp, offers_uncleared_pp, bids_uncleared_pp, offers_cleared_pp, bids_cleared_pp = \
                    clearing_pref_prio(db_obj=db_obj,
                                       config_lem=config_lem,
                                       offers=offers_uncleared,
                                       bids=bids_uncleared,
                                       type_clearing=type_clearing,
                                       type_prioritization='0_to_n')
                positions_cleared = positions_cleared.append(results_pp).reset_index(drop=True)

        if 'pref_satis_pref_prio_sep' == type_clearing:
            positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                clearing_pref_satis(db_obj,
                                    config_lem,
                                    offers,
                                    bids,
                                    verbose=verbose)

            if not bids_uncleared.empty and not offers_uncleared.empty:
                # Clear in a preference prioritization for remaining positions
                results_pp, offers_uncleared_pp, bids_uncleared_pp, offers_cleared_pp, bids_cleared_pp = \
                    clearing_pref_prio(db_obj=db_obj,
                                       config_lem=config_lem,
                                       offers=offers_uncleared,
                                       bids=bids_uncleared,
                                       type_clearing=type_clearing,
                                       type_prioritization='sep')
                positions_cleared = positions_cleared.append(results_pp).reset_index(drop=True)

        if 'pref_prio_sep_pref_satis' == type_clearing:
            positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                clearing_pref_prio(db_obj,
                                   config_lem,
                                   offers,
                                   bids,
                                   type_prioritization='sep')

            if not bids_uncleared.empty and not offers_uncleared.empty:
                results_ps, offers_uncleared_ps, bids_uncleared_ps, offers_cleared_ps, bids_cleared_ps = \
                    clearing_pref_satis(db_obj,
                                        config_lem,
                                        offers_uncleared,
                                        bids_uncleared,
                                        verbose=verbose)
                positions_cleared = positions_cleared.append(results_ps).reset_index(drop=True)

        if 'pref_prio_0_to_n_pref_satis' == type_clearing:
            positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                clearing_pref_prio(db_obj,
                                   config_lem,
                                   offers,
                                   bids,
                                   type_prioritization='0_to_n')

            if not bids_uncleared.empty and not offers_uncleared.empty:
                results_ps, offers_uncleared_ps, bids_uncleared_ps, offers_cleared_ps, bids_cleared_ps = clearing_pref_satis(
                    db_obj,
                    config_lem,
                    offers_uncleared,
                    bids_uncleared,
                    verbose=verbose)
                positions_cleared = positions_cleared.append(results_ps).reset_index(drop=True)

        if 'pref_prio_n_to_0_pref_satis' == type_clearing:
            positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                clearing_pref_prio(db_obj,
                                   config_lem,
                                   offers,
                                   bids,
                                   type_prioritization='n_to_0')

            if not bids_uncleared.empty and not offers_uncleared.empty:
                results_ps, offers_uncleared_ps, bids_uncleared_ps, offers_cleared_ps, bids_cleared_ps = \
                    clearing_pref_satis(db_obj,
                                        config_lem,
                                        offers_uncleared,
                                        bids_uncleared,
                                        verbose=verbose)
                positions_cleared = positions_cleared.append(results_ps).reset_index(drop=True)

        # Combinations WITH consideration of quality premium ###
        # Standard da AFTER advanced clearing
        if 'pref_prio_n_to_0_da' == type_clearing:
            positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                clearing_pref_prio(db_obj,
                                   config_lem,
                                   offers,
                                   bids,
                                   type_prioritization='n_to_0',
                                   add_premium=True)

            positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                clearing_da(db_obj,
                            config_lem,
                            offers_uncleared,
                            bids_uncleared,
                            add_premium=False)

            positions_cleared = positions_cleared.append(positions_cleared_da, ignore_index=True)

        if 'pref_prio_0_to_n_da' == type_clearing:
            positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                clearing_pref_prio(db_obj,
                                   config_lem,
                                   offers,
                                   bids,
                                   type_prioritization='0_to_n',
                                   add_premium=True)

            positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                clearing_da(db_obj,
                            config_lem,
                            offers_uncleared,
                            bids_uncleared,
                            add_premium=False)

            positions_cleared = positions_cleared.append(positions_cleared_da, ignore_index=True)

        if 'pref_prio_sep_da' == type_clearing:
            positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                clearing_pref_prio(db_obj,
                                   config_lem,
                                   offers,
                                   bids,
                                   type_prioritization='sep',
                                   add_premium=True)

            positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                clearing_da(db_obj,
                            config_lem,
                            offers_uncleared,
                            bids_uncleared,
                            add_premium=False)

            positions_cleared = positions_cleared.append(positions_cleared_da, ignore_index=True)

        if 'pref_satis_da' == type_clearing:
            positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                clearing_pref_satis(db_obj,
                                    config_lem,
                                    offers,
                                    bids,
                                    add_premium=True,
                                    verbose=verbose)

            positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                clearing_da(db_obj,
                            config_lem,
                            offers_uncleared,
                            bids_uncleared,
                            add_premium=False)

            positions_cleared = positions_cleared.append(positions_cleared_da, ignore_index=True)

        if 'pref_satis_pref_prio_n_to_0_da' == type_clearing:
            positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                clearing_pref_satis(db_obj,
                                    config_lem,
                                    offers,
                                    bids,
                                    add_premium=True,
                                    verbose=verbose)

            if not bids_uncleared.empty and not offers_uncleared.empty:
                # Clear in a preference prioritization for remaining positions
                results_pp, offers_uncleared_pp, bids_uncleared_pp, offers_cleared_pp, bids_cleared_pp = \
                    clearing_pref_prio(db_obj=db_obj,
                                       config_lem=config_lem,
                                       offers=offers_uncleared,
                                       bids=bids_uncleared,
                                       type_clearing=type_clearing,
                                       add_premium=True,
                                       type_prioritization='n_to_0')
                positions_cleared = positions_cleared.append(results_pp).reset_index(drop=True)

                positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                    clearing_da(db_obj,
                                config_lem,
                                offers_uncleared_pp,
                                bids_uncleared_pp,
                                add_premium=False, )

                positions_cleared = positions_cleared.append(positions_cleared_da, ignore_index=True)

        if 'pref_satis_pref_prio_0_to_n_da' == type_clearing:
            positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                clearing_pref_satis(db_obj,
                                    config_lem,
                                    offers,
                                    bids,
                                    add_premium=True,
                                    verbose=verbose)

            if not bids_uncleared.empty and not offers_uncleared.empty:
                # Clear in a preference prioritization for remaining positions
                results_pp, offers_uncleared_pp, bids_uncleared_pp, offers_cleared_pp, bids_cleared_pp = \
                    clearing_pref_prio(db_obj=db_obj,
                                       config_lem=config_lem,
                                       offers=offers_uncleared,
                                       bids=bids_uncleared,
                                       type_clearing=type_clearing,
                                       add_premium=True,
                                       type_prioritization='0_to_n')
                positions_cleared = positions_cleared.append(results_pp).reset_index(drop=True)

                positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                    clearing_da(db_obj,
                                config_lem,
                                offers_uncleared_pp,
                                bids_uncleared_pp,
                                add_premium=False)

                positions_cleared = positions_cleared.append(positions_cleared_da, ignore_index=True)

        if 'pref_satis_pref_prio_sep_da' == type_clearing:
            positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                clearing_pref_satis(db_obj,
                                    config_lem,
                                    offers,
                                    bids,
                                    add_premium=True,
                                    verbose=verbose)

            if not bids_uncleared.empty and not offers_uncleared.empty:
                # Clear in a preference prioritization for remaining positions
                results_pp, offers_uncleared_pp, bids_uncleared_pp, offers_cleared_pp, bids_cleared_pp = \
                    clearing_pref_prio(db_obj=db_obj,
                                       config_lem=config_lem,
                                       offers=offers_uncleared,
                                       bids=bids_uncleared,
                                       type_clearing=type_clearing,
                                       add_premium=True,
                                       type_prioritization='sep')
                positions_cleared = positions_cleared.append(results_pp).reset_index(drop=True)

                positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                    clearing_da(db_obj,
                                config_lem,
                                offers_uncleared_pp,
                                bids_uncleared_pp,
                                add_premium=False)

                positions_cleared = positions_cleared.append(positions_cleared_da, ignore_index=True)

        if 'pref_prio_sep_pref_satis_da' == type_clearing:
            positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                clearing_pref_prio(db_obj,
                                   config_lem,
                                   offers,
                                   bids,
                                   type_prioritization='sep')

            if not bids_uncleared.empty and not offers_uncleared.empty:
                results_ps, offers_uncleared_ps, bids_uncleared_ps, offers_cleared_ps, bids_cleared_ps = \
                    clearing_pref_satis(db_obj,
                                        config_lem,
                                        offers_uncleared,
                                        bids_uncleared,
                                        verbose=verbose)
                positions_cleared = positions_cleared.append(results_ps).reset_index(drop=True)

                positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                    clearing_da(db_obj,
                                config_lem,
                                offers_uncleared_ps,
                                bids_uncleared_ps,
                                add_premium=False)

                positions_cleared = positions_cleared.append(positions_cleared_da, ignore_index=True)

        if 'pref_prio_0_to_n_pref_satis_da' == type_clearing:
            positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                clearing_pref_prio(db_obj,
                                   config_lem,
                                   offers,
                                   bids,
                                   type_prioritization='0_to_n')

            if not bids_uncleared.empty and not offers_uncleared.empty:
                results_ps, offers_uncleared_ps, bids_uncleared_ps, offers_cleared_ps, bids_cleared_ps = \
                    clearing_pref_satis(db_obj,
                                        config_lem,
                                        offers_uncleared,
                                        bids_uncleared,
                                        verbose=verbose)
                positions_cleared = positions_cleared.append(results_ps).reset_index(drop=True)

                positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                    clearing_da(db_obj,
                                config_lem,
                                offers_uncleared_ps,
                                bids_uncleared_ps,
                                add_premium=False)

                positions_cleared = positions_cleared.append(positions_cleared_da, ignore_index=True)

        if 'pref_prio_n_to_0_pref_satis_da' == type_clearing:
            positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                clearing_pref_prio(db_obj,
                                   config_lem,
                                   offers,
                                   bids,
                                   type_prioritization='n_to_0')

            if not bids_uncleared.empty and not offers_uncleared.empty:
                results_ps, offers_uncleared_ps, bids_uncleared_ps, offers_cleared_ps, bids_cleared_ps = \
                    clearing_pref_satis(db_obj,
                                        config_lem,
                                        offers_uncleared,
                                        bids_uncleared,
                                        verbose=verbose)
                positions_cleared = positions_cleared.append(results_ps).reset_index(drop=True)

                positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                    clearing_da(db_obj,
                                config_lem,
                                offers_uncleared_ps,
                                bids_uncleared_ps,
                                add_premium=False)

                positions_cleared = positions_cleared.append(positions_cleared_da, ignore_index=True)

        # Standard da BEFORE advanced clearing
        if 'da_pref_prio_n_to_0' == type_clearing:
            positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                clearing_da(db_obj,
                            config_lem,
                            offers,
                            bids,
                            add_premium=False)

            positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                clearing_pref_prio(db_obj,
                                   config_lem,
                                   offers_uncleared_da,
                                   bids_uncleared_da,
                                   type_prioritization='pref_n_to_0',
                                   add_premium=True)

            positions_cleared = positions_cleared.append(positions_cleared_da, ignore_index=True)

        if 'da_pref_prio_0_to_n' == type_clearing:
            positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                clearing_da(db_obj,
                            config_lem,
                            offers,
                            bids,
                            add_premium=False)

            positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                clearing_pref_prio(db_obj,
                                   config_lem,
                                   offers_uncleared_da,
                                   bids_uncleared_da,
                                   type_prioritization='0_to_n',
                                   add_premium=True)

            positions_cleared = positions_cleared.append(positions_cleared_da, ignore_index=True)

        if 'da_pref_prio_sep' == type_clearing:
            positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                clearing_da(db_obj,
                            config_lem,
                            offers,
                            bids,
                            add_premium=False)

            positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                clearing_pref_prio(db_obj,
                                   config_lem,
                                   offers_uncleared_da,
                                   bids_uncleared_da,
                                   type_prioritization='sep',
                                   add_premium=True)

            positions_cleared = positions_cleared.append(positions_cleared_da, ignore_index=True)

        if 'da_pref_satis' == type_clearing:
            positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                clearing_da(db_obj,
                            config_lem,
                            offers,
                            bids,
                            add_premium=False)

            positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
                clearing_pref_satis(db_obj,
                                    config_lem,
                                    offers=offers_uncleared_da,
                                    bids=bids_uncleared_da,
                                    add_premium=True,
                                    verbose=verbose)

            positions_cleared = positions_cleared.append(positions_cleared_da, ignore_index=True)

        if 'da_pref_satis_pref_prio_n_to_0' == type_clearing:
            positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                clearing_da(db_obj,
                            config_lem,
                            offers,
                            bids,
                            add_premium=False)

            if not offers_uncleared_da.empty and bids_uncleared_da.empty:
                results_ps, offers_uncleared_ps, bids_uncleared_ps, offers_cleared_ps, bids_cleared_ps = \
                    clearing_pref_satis(db_obj,
                                        config_lem,
                                        offers=offers_uncleared_da,
                                        bids=bids_uncleared_da,
                                        add_premium=True,
                                        verbose=verbose)

                positions_cleared = results_ps.append(positions_cleared_da, ignore_index=True)

                results_pp, offers_uncleared_pp, bids_uncleared_pp, offers_cleared_pp, bids_cleared_pp = \
                    clearing_pref_prio(db_obj=db_obj,
                                       config_lem=config_lem,
                                       offers=offers_uncleared_ps,
                                       bids=bids_uncleared_ps,
                                       type_clearing=type_clearing,
                                       add_premium=True,
                                       type_prioritization='n_to_0')
                positions_cleared = positions_cleared.append(results_pp).reset_index(drop=True)

        if 'da_pref_satis_pref_prio_0_to_n' == type_clearing:
            positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                clearing_da(db_obj,
                            config_lem,
                            offers,
                            bids,
                            add_premium=False)

            if not offers_uncleared_da.empty and bids_uncleared_da.empty:
                results_ps, offers_uncleared_ps, bids_uncleared_ps, offers_cleared_ps, bids_cleared_ps = \
                    clearing_pref_satis(db_obj,
                                        config_lem,
                                        offers=offers_uncleared_da,
                                        bids=bids_uncleared_da,
                                        add_premium=True,
                                        verbose=verbose)

                positions_cleared = results_ps.append(positions_cleared_da, ignore_index=True)

                results_pp, offers_uncleared_pp, bids_uncleared_pp, offers_cleared_pp, bids_cleared_pp = \
                    clearing_pref_prio(db_obj=db_obj,
                                       config_lem=config_lem,
                                       offers=offers_uncleared_ps,
                                       bids=bids_uncleared_ps,
                                       type_clearing=type_clearing,
                                       add_premium=True,
                                       type_prioritization='0_to_n')
                positions_cleared = positions_cleared.append(results_pp).reset_index(drop=True)

        if 'da_pref_satis_pref_prio_sep' == type_clearing:
            positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                clearing_da(db_obj,
                            config_lem,
                            offers,
                            bids,
                            add_premium=False)

            if not offers_uncleared_da.empty and bids_uncleared_da.empty:
                results_ps, offers_uncleared_ps, bids_uncleared_ps, offers_cleared_ps, bids_cleared_ps = \
                    clearing_pref_satis(db_obj,
                                        config_lem,
                                        offers=offers_uncleared_da,
                                        bids=bids_uncleared_da,
                                        add_premium=True,
                                        verbose=verbose)

                positions_cleared = results_ps.append(positions_cleared_da, ignore_index=True)

                results_pp, offers_uncleared_pp, bids_uncleared_pp, offers_cleared_pp, bids_cleared_pp = \
                    clearing_pref_prio(db_obj=db_obj,
                                       config_lem=config_lem,
                                       offers=offers_uncleared_ps,
                                       bids=bids_uncleared_ps,
                                       type_clearing=type_clearing,
                                       add_premium=True,
                                       type_prioritization='sep')
                positions_cleared = positions_cleared.append(results_pp).reset_index(drop=True)

        if 'da_pref_prio_n_to_0_pref_satis' == type_clearing:
            positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                clearing_da(db_obj,
                            config_lem,
                            offers,
                            bids,
                            add_premium=False)

            if not offers_uncleared_da.empty and bids_uncleared_da.empty:
                results_pp, offers_uncleared_pp, bids_uncleared_pp, offers_cleared_pp, bids_cleared_pp = \
                    clearing_pref_prio(db_obj=db_obj,
                                       config_lem=config_lem,
                                       offers=offers_uncleared_da,
                                       bids=bids_uncleared_da,
                                       type_clearing=type_clearing,
                                       add_premium=True,
                                       type_prioritization='n_to_0')
                positions_cleared = positions_cleared_da.append(results_pp).reset_index(drop=True)

                results_ps, offers_uncleared_ps, bids_uncleared_ps, offers_cleared_ps, bids_cleared_ps = \
                    clearing_pref_satis(db_obj,
                                        config_lem,
                                        offers=offers_uncleared_pp,
                                        bids=bids_uncleared_pp,
                                        add_premium=True,
                                        verbose=verbose)

                positions_cleared = positions_cleared.append(results_ps, ignore_index=True)

        if 'da_pref_prio_0_to_n_pref_satis' == type_clearing:
            positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                clearing_da(db_obj,
                            config_lem,
                            offers,
                            bids,
                            add_premium=False)

            if not offers_uncleared_da.empty and bids_uncleared_da.empty:
                results_pp, offers_uncleared_pp, bids_uncleared_pp, offers_cleared_pp, bids_cleared_pp = \
                    clearing_pref_prio(db_obj=db_obj,
                                       config_lem=config_lem,
                                       offers=offers_uncleared_da,
                                       bids=bids_uncleared_da,
                                       type_clearing=type_clearing,
                                       add_premium=True,
                                       type_prioritization='0_to_n')
                positions_cleared = positions_cleared_da.append(results_pp).reset_index(drop=True)

                results_ps, offers_uncleared_ps, bids_uncleared_ps, offers_cleared_ps, bids_cleared_ps = \
                    clearing_pref_satis(db_obj,
                                        config_lem,
                                        offers=offers_uncleared_pp,
                                        bids=bids_uncleared_pp,
                                        add_premium=True,
                                        verbose=verbose)

                positions_cleared = positions_cleared.append(results_ps, ignore_index=True)

        if 'da_pref_prio_sep_pref_satis' == type_clearing:
            positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                clearing_da(db_obj,
                            config_lem,
                            offers,
                            bids,
                            add_premium=False)

            if not offers_uncleared_da.empty and bids_uncleared_da.empty:
                results_pp, offers_uncleared_pp, bids_uncleared_pp, offers_cleared_pp, bids_cleared_pp = \
                    clearing_pref_prio(db_obj=db_obj,
                                       config_lem=config_lem,
                                       offers=offers_uncleared_da,
                                       bids=bids_uncleared_da,
                                       type_clearing=type_clearing,
                                       add_premium=True,
                                       type_prioritization='sep')
                positions_cleared = positions_cleared_da.append(results_pp).reset_index(drop=True)

                results_ps, offers_uncleared_ps, bids_uncleared_ps, offers_cleared_ps, bids_cleared_ps = \
                    clearing_pref_satis(db_obj,
                                        config_lem,
                                        offers=offers_uncleared_pp,
                                        bids=bids_uncleared_pp,
                                        add_premium=True,
                                        verbose=verbose)

                positions_cleared = positions_cleared.append(results_ps, ignore_index=True)

        if not positions_cleared.empty and config_lem['share_quality_logging_extended']:
            positions_cleared = calc_market_position_shares(db_obj, config_lem, offers, bids, positions_cleared)

        t_clearing_end = round(time.time())
        t_clearings.at[n_test_case, type_clearing] = t_clearing_end - t_clearing_start
        positions_cleared.reset_index().to_feather(path=f'{path_results}/{type_clearing}_{n_test_case}.ft')

    return t_clearings


def create_simulation_folder(simulation_path='test/'):
    # Delete existing output folder
    if os.path.exists(simulation_path):
        shutil.rmtree(path=simulation_path, ignore_errors=True)
    # Create simulation folder
    Path(simulation_path).mkdir(parents=True, exist_ok=False)
    path_in = f'{simulation_path}/input'
    path_out = f'{simulation_path}/output'
    path_res = f'{simulation_path}/results'
    path_fig = f'{simulation_path}/figures'
    # Create input and results folder
    Path(path_in).mkdir(parents=True, exist_ok=False)
    Path(path_out).mkdir(parents=True, exist_ok=False)
    Path(path_res).mkdir(parents=True, exist_ok=False)
    Path(path_fig).mkdir(parents=True, exist_ok=False)

    return simulation_path, path_in, path_out, path_res, path_fig


def _init_db_obj_workers(function, db_dict, config, path_input, path_output):
    """initialize database connections for parallel workers

    :param function: parallelized function for which the db connection is to be created
    :param db_dict: db_dict to be used for the parallel db connections

    """
    function.db_obj = DatabaseConnection(db_dict=db_dict, lem_config=config['lem'])
    function.config = config
    function.path_input = path_input
    function.path_output = path_output


def single_lem_simulation(test_case_number):
    # Create list of random user ids
    ids_users_random = create_user_ids(num=single_lem_simulation.config['prosumer']['general_number_of'])
    # Compute random market positions
    positions = create_random_positions(db_obj=single_lem_simulation.db_obj,
                                        config=single_lem_simulation.config,
                                        ids_user=ids_users_random,
                                        n_positions=single_lem_simulation.config['monte_carlo'][
                                            'n_positions_per_iteration'],
                                        verbose=False)
    # Store positions as input to file
    positions.reset_index().to_feather(path=f'{single_lem_simulation.path_input}/positions_{test_case_number}.ft')
    # Extract bids and offers
    bids = positions[positions['type_position'] == 'bid']
    offers = positions[positions['type_position'] == 'offer']
    bids = _convert_qualities_to_int(single_lem_simulation.db_obj,
                                     bids, single_lem_simulation.config['lem']['types_quality'])
    offers = _convert_qualities_to_int(single_lem_simulation.db_obj,
                                       offers, single_lem_simulation.config['lem']['types_quality'])
    # run clearings and save to files
    run_clearings(db_obj=single_lem_simulation.db_obj,
                  config_lem=single_lem_simulation.config['lem'],
                  offers=offers,
                  bids=bids,
                  path_results=single_lem_simulation.path_output,
                  n_test_case=test_case_number,
                  verbose=False)


def analyze_lem_simulations(config_mc, path_output):
    # Perform mc computations
    dict_general_mc = {'df_qty_traded': pd.DataFrame(), 'df_price_wavg': pd.DataFrame(), 'df_welfare': pd.DataFrame()}
    dict_shares_mc = {}
    if config_mc['lem']['share_quality_logging_extended']:
        dict_shares_mc['shares_offers'] = {}
        dict_shares_mc['shares_bids'] = {}
    dict_shares_mc['shares_offers_cleared'] = {}
    if config_mc['lem']['share_quality_logging_extended']:
        dict_shares_mc['shares_bids_cleared'] = {}
        dict_shares_mc['shares_bids_cleared_all'] = {}
        dict_shares_mc['shares_offers_cleared_all'] = {}
        dict_shares_mc['shares_bids_satis'] = {}
    dict_shares_mc['prices_quality'] = {}
    for i in range(len(config_mc['lem']['types_quality'])):
        type_quality = config_mc['lem']['types_quality'][i]
        for key in dict_shares_mc:
            dict_shares_mc[key][type_quality] = pd.DataFrame()

    try:
        # Set up all columns
        for i in range(len(config_mc['lem']['types_clearing_ex_ante'])):
            type_clearing = config_mc['lem']['types_clearing_ex_ante'][i]
            for key in dict_general_mc:
                dict_general_mc[key][f'{type_clearing}'] = None
                dict_general_mc[key][f'mc_mean_{type_clearing}'] = None
            for j in range(len(config_mc['lem']['types_quality'])):
                type_quality = config_mc['lem']['types_quality'][j]
                for key in dict_shares_mc:
                    dict_shares_mc[key][type_quality][f'{type_clearing}'] = None
                    dict_shares_mc[key][type_quality][f'mc_mean_{type_clearing}'] = None

        result_file_names = os.listdir(f'{path_output}')
        for result_name in tqdm(result_file_names):
            result_df = pd.read_feather(path=f'{path_output}/{result_name}')
            qty_traded = result_df['qty_energy_traded'].sum()
            price_wavg = np.average(result_df['price_energy_market_uniform'],
                                    weights=result_df['qty_energy_traded'])
            cons_surplus = ((result_df['price_energy_bid'] - result_df['price_energy_market_uniform']) *
                            result_df['qty_energy_traded']).sum()
            prod_surplus = ((result_df['price_energy_market_uniform'] - result_df['price_energy_offer']) *
                            result_df['qty_energy_traded']).sum()

            type_clearing = result_name.rsplit('_', 1)[0]
            n_test_case = int(result_name.rsplit('_', 1)[1].split('.', 1)[0])

            dict_general_mc['df_welfare'].at[n_test_case, type_clearing] = cons_surplus + prod_surplus
            dict_general_mc['df_qty_traded'].at[n_test_case, type_clearing] = qty_traded
            dict_general_mc['df_price_wavg'].at[n_test_case, type_clearing] = price_wavg

            share_quality_offers_cleared_excess = 0  # initialize excess offer share with zero
            for j in reversed(range(len(config_mc['lem']['types_quality']))):
                type_quality = config_mc['lem']['types_quality'][j]
                if config_mc['lem']['share_quality_logging_extended']:
                    # Share preference bids in all positions
                    share_preference_bids = np.average(
                        result_df.loc[:, f'share_preference_bids_{type_quality}'],
                        weights=result_df.loc[:, 'qty_energy_traded'])
                    dict_shares_mc['shares_bids'][type_quality].at[
                        n_test_case, type_clearing] = share_preference_bids
                    # Share quality offers in all positions
                    share_quality_offers = np.average(
                        result_df.loc[:, f'share_quality_offers_{type_quality}'],
                        weights=result_df.loc[:, 'qty_energy_traded'])
                    dict_shares_mc['shares_offers'][type_quality].at[
                        n_test_case, type_clearing] = share_quality_offers
                # Share quality offers in cleared positions
                share_quality_offers_cleared = np.average(
                    result_df.loc[:, f'share_quality_offers_cleared_{type_quality}'],
                    weights=result_df.loc[:, 'qty_energy_traded'])
                dict_shares_mc['shares_offers_cleared'][type_quality].at[
                    n_test_case, type_clearing] = share_quality_offers_cleared

                if config_mc['lem']['share_quality_logging_extended']:
                    # Share preference bids in cleared positions
                    share_preference_bids_cleared = np.average(
                        result_df.loc[:, f'share_preference_bids_cleared_{type_quality}'],
                        weights=result_df.loc[:, 'qty_energy_traded'])
                    dict_shares_mc['shares_bids_cleared'][type_quality].at[
                        n_test_case, type_clearing] = share_preference_bids_cleared
                    # Share quality offers cleared of all quality offers
                    share_quality_offers_cleared_all = np.average(
                        result_df.loc[:, f'share_quality_offers_cleared_{type_quality}'] /
                        result_df.loc[:, f'share_quality_offers_{type_quality}'],
                        weights=result_df.loc[:, 'qty_energy_traded'])
                    dict_shares_mc['shares_offers_cleared_all'][type_quality].at[
                        n_test_case, type_clearing] = share_quality_offers_cleared_all
                    # Share preference bids cleared of all preference bids
                    share_preference_bids_cleared_all = np.average(
                        result_df.loc[:, f'share_preference_bids_cleared_{type_quality}'] /
                        result_df.loc[:, f'share_preference_bids_{type_quality}'],
                        weights=result_df.loc[:, 'qty_energy_traded'])
                    dict_shares_mc['shares_bids_cleared_all'][type_quality].at[
                        n_test_case, type_clearing] = share_preference_bids_cleared_all
                    # Share preference bids satisfied by offers in cleared positions
                    if share_preference_bids_cleared > 0:
                        dict_shares_mc['shares_bids_satis'][type_quality].at[n_test_case, type_clearing] = \
                            (
                                    share_quality_offers_cleared + share_quality_offers_cleared_excess) / share_preference_bids_cleared
                        if share_quality_offers_cleared + share_quality_offers_cleared_excess - share_preference_bids_cleared > 0:
                            share_quality_offers_cleared_excess = share_quality_offers_cleared_excess + share_quality_offers_cleared - share_preference_bids_cleared
                        else:
                            share_quality_offers_cleared_excess = 0

                if result_df.loc[:, f'share_quality_offers_cleared_{type_quality}'].sum() == 0:
                    price_quality = 0
                else:
                    price_quality = np.average(result_df['price_energy_market_uniform'],
                                               weights=result_df.loc[:, f'share_quality_offers_cleared_{type_quality}']
                                                       * result_df.loc[:, 'qty_energy_traded'])
                dict_shares_mc['prices_quality'][type_quality].at[n_test_case, type_clearing] = price_quality

        # Important, sort rows by index in order to maintain order of iterations
        for key in dict_general_mc:
            dict_general_mc[key] = dict_general_mc[key].sort_index()
        for i in range(len(config_mc['lem']['types_quality'])):
            type_quality = config_mc['lem']['types_quality'][i]
            for key in dict_shares_mc:
                dict_shares_mc[key][type_quality] = dict_shares_mc[key][type_quality].sort_index()

        # Calculate mean of iterations
        for i in dict_general_mc['df_welfare'].index:
            for j in range(len(config_mc['lem']['types_clearing_ex_ante'])):
                type_clearing = config_mc['lem']['types_clearing_ex_ante'][j]
                for key in dict_general_mc:
                    dict_general_mc[key].at[i, f'mc_mean_{type_clearing}'] = int(
                        dict_general_mc[key][f'{type_clearing}'].iloc[:i + 1].mean())
                for k in range(len(config_mc['lem']['types_quality'])):
                    type_quality = config_mc['lem']['types_quality'][k]
                    for key in dict_shares_mc:
                        dict_shares_mc[key][type_quality].at[i, f'mc_mean_{type_clearing}'] = \
                            dict_shares_mc[key][type_quality][f'{type_clearing}'].iloc[:i + 1].mean()

    except Exception as e:
        print(e)
        traceback.print_exc()

    return dict_general_mc, dict_shares_mc


def plot_mc_results_with_welfare(config,
                                 dict_results_general=None,
                                 path_figure=None,
                                 plotting_title=None,
                                 dict_plot=None):
    fig, axs = plt.subplots(ncols=1, nrows=3, sharex='col', figsize=(12, 15))
    for i in range(len(config['lem']['types_clearing_ex_ante'])):
        type_clearing = config['lem']['types_clearing_ex_ante'][i]
        axs[0].plot(dict_results_general['df_qty_traded'][f'mc_mean_{type_clearing}'] / 1000,
                    label=type_clearing, color=dict_plot[type_clearing]['color'])
        axs[1].plot(dict_results_general['df_price_wavg'][f'mc_mean_{type_clearing}'] / 1e9 * 1000,
                    label=type_clearing, color=dict_plot[type_clearing]['color'])
        axs[2].plot(dict_results_general['df_welfare'][f'mc_mean_{type_clearing}'] / 1e9,
                    label=type_clearing, color=dict_plot[type_clearing]['color'])
    axs[0].set_ylabel('Average cleared energy [kWh]')
    axs[0].grid()
    axs[0].set_title(plotting_title)
    axs[1].set_ylabel('Average cleared energy price [€/kWh]')
    axs[1].grid()
    axs[2].set_ylabel('Average welfare [€]')
    axs[2].set_xlabel('Number of simulations')
    axs[2].grid()
    plt.tight_layout()
    if path_figure is None:
        plt.show()
        return

    fig.subplots_adjust(wspace=0.6, top=0.95, bottom=0.06, left=0.08, right=.95)
    handles, labels = axs[2].get_legend_handles_labels()
    fig.legend(handles, labels, bbox_to_anchor=(0.5, .0), loc='lower center', frameon=False, ncol=5)

    plt.savefig(f'{path_figure}/mc_mean_energy_price_welfare.png')
    plt.show()


def plot_mc_results_bar_algorithms(config,
                                   dict_results_general,
                                   dict_results_quality_shares,
                                   path_figure,
                                   dict_plot=None):
    fig = plt.figure(constrained_layout=False, figsize=(15, 10))
    gs = fig.add_gridspec(3, 6)
    axs02 = fig.add_subplot(gs[0, 2:])
    axs12 = fig.add_subplot(gs[1, 2:], sharex=axs02)
    axs22 = fig.add_subplot(gs[2, 2:], sharex=axs02)

    for i in range(len(config['lem']['types_quality'])):
        type_quality = config['lem']['types_quality'][i]
        for j in range(len(config['lem']['types_clearing_ex_ante'])):
            type_clearing = config['lem']['types_clearing_ex_ante'][j]
            if i == 0:
                axs02.bar((i * (len(config['lem']['types_clearing_ex_ante']) + 1)) + j,
                          dict_results_quality_shares['shares_offers_cleared'][type_quality][
                              f'mc_mean_{type_clearing}'].iloc[-1],
                          color=dict_plot[type_clearing]['color'], label=type_clearing)
                axs12.bar((i * (len(config['lem']['types_clearing_ex_ante']) + 1)) + j,
                          dict_results_quality_shares['shares_bids_cleared'][type_quality][
                              f'mc_mean_{type_clearing}'].iloc[-1],
                          color=dict_plot[type_clearing]['color'], label=type_clearing)
                axs22.bar((i * (len(config['lem']['types_clearing_ex_ante']) + 1)) + j,
                          dict_results_quality_shares['prices_quality'][type_quality][
                              f'mc_mean_{type_clearing}'].iloc[-1] / 1e9 * 1000,
                          color=dict_plot[type_clearing]['color'], label=type_clearing)
            else:
                axs02.bar((i * (len(config['lem']['types_clearing_ex_ante']) + 1)) + j,
                          dict_results_quality_shares['shares_offers_cleared'][type_quality][
                              f'mc_mean_{type_clearing}'].iloc[-1],
                          color=dict_plot[type_clearing]['color'])
                axs12.bar((i * (len(config['lem']['types_clearing_ex_ante']) + 1)) + j,
                          dict_results_quality_shares['shares_bids_cleared'][type_quality][
                              f'mc_mean_{type_clearing}'].iloc[-1],
                          color=dict_plot[type_clearing]['color'])
                axs22.bar((i * (len(config['lem']['types_clearing_ex_ante']) + 1)) + j,
                          dict_results_quality_shares['prices_quality'][type_quality][
                              f'mc_mean_{type_clearing}'].iloc[-1] / 1e9 * 1000,
                          color=dict_plot[type_clearing]['color'])

    tick_range = np.arange((len(config['lem']['types_clearing_ex_ante']) - 1) / 2,
                           len(config['lem']['types_quality']) * len(
                               config['lem']['types_clearing_ex_ante']) + (
                                   len(config['lem']['types_quality']) - 1),
                           len(config['lem']['types_clearing_ex_ante']) + 1)
    axs02.set_xticks(tick_range)
    axs02.set_xticklabels(config['lem']['types_quality'].values())
    axs02.set_ylabel('Share offers cleared [%]')
    axs12.set_ylabel('Share bids cleared [%]')
    axs22.set_ylabel('Weighted average prices [€/kWh]')
    axs02.grid()
    axs12.grid()
    axs22.grid()

    axs00 = fig.add_subplot(gs[:-1, :-4])
    axs20 = fig.add_subplot(gs[2, :-4], sharex=axs00, sharey=axs22)
    for j in range(len(config['lem']['types_clearing_ex_ante'])):
        type_clearing = config['lem']['types_clearing_ex_ante'][j]
        axs00.bar(j, dict_results_general['df_qty_traded'][f'mc_mean_{type_clearing}'].iloc[-1] / 1000,
                  label=type_clearing, color=dict_plot[type_clearing]['color'])
        axs20.bar(j, dict_results_general['df_price_wavg'][f'mc_mean_{type_clearing}'].iloc[-1] / 1e9 * 1000,
                  label=type_clearing, color=dict_plot[type_clearing]['color'])
    axs00.grid()
    axs00.set_ylabel('Average cleared energy [kWh]')
    axs00.set_xticklabels([])
    axs20.set_xticklabels([])
    axs20.grid()
    axs20.set_ylabel('Weighted average price [€/kWh]')

    fig.subplots_adjust(wspace=0.6, top=0.95, bottom=0.07, left=0.08, right=.95)
    handles, labels = axs20.get_legend_handles_labels()
    fig.legend(handles, labels, bbox_to_anchor=(0.5, .0), loc='lower center', frameon=False, ncol=5)

    plt.savefig(f'{path_figure}/mc_bar_results_energy_prices_shares.png')
    plt.show()


def plot_mc_results_bar_qualities(config,
                                  dict_results_general,
                                  dict_results_quality_shares,
                                  path_figure,
                                  dict_plot=None):
    # Flip qualities and algorithms ###
    fig = plt.figure(constrained_layout=False, figsize=(15, 10))
    gs = fig.add_gridspec(3, 7)

    for i in range(len(config['lem']['types_clearing_ex_ante'])):
        if i == 0:
            axs02 = fig.add_subplot(gs[0, 2 + i])
            axs12 = fig.add_subplot(gs[1, 2 + i], sharex=axs02)
            axs22 = fig.add_subplot(gs[2, 2 + i], sharex=axs02)
            axs02.set_xticks(
                [-len(config['lem']['types_clearing_ex_ante']), len(config['lem']['types_clearing_ex_ante']) * 2])
            axs02.set_xticklabels([])
            axs02.set_ylabel('Share offers cleared [%]')
            axs12.set_ylabel('Share bids cleared [%]')
            axs22.set_ylabel('Weighted average prices [€/kWh]')
        else:
            axs02 = fig.add_subplot(gs[0, 2 + i], sharex=axs02, sharey=axs02)
            axs12 = fig.add_subplot(gs[1, 2 + i], sharex=axs02, sharey=axs12)
            axs22 = fig.add_subplot(gs[2, 2 + i], sharex=axs02, sharey=axs22)
        type_clearing = config['lem']['types_clearing_ex_ante'][i]
        axs02.set_title(type_clearing)
        for j in range(len(config['lem']['types_quality'])):
            type_quality = config['lem']['types_quality'][j]
            axs02.bar(j, dict_results_quality_shares['shares_offers_cleared'][type_quality][
                f'mc_mean_{type_clearing}'].iloc[-1],
                      color=dict_plot[type_quality]['color'], label=type_quality)
            axs12.bar(j, dict_results_quality_shares['shares_bids_cleared'][type_quality][
                f'mc_mean_{type_clearing}'].iloc[-1],
                      color=dict_plot[type_quality]['color'], label=type_quality)
            axs22.bar(j, dict_results_quality_shares['prices_quality'][type_quality][
                f'mc_mean_{type_clearing}'].iloc[-1] / 1e9 * 1000,
                      color=dict_plot[type_quality]['color'], label=type_quality)
        axs02.grid()
        axs12.grid()
        axs22.grid()

    axs00 = fig.add_subplot(gs[0, :-5])
    axs10 = fig.add_subplot(gs[1, :-5], sharex=axs00)
    axs20 = fig.add_subplot(gs[2, :-5], sharex=axs00, sharey=axs22)
    axs00.set_xticks([-10, len(config['lem']['types_clearing_ex_ante']) * 2])
    axs00.set_xticklabels([])
    axs00.set_ylabel('Average cleared energy [kWh]')
    axs10.set_ylabel('Average cleared energy [kWh]')
    axs20.set_ylabel('Weighted average price [€/kWh]')
    bottom_value_00 = [0] * len(config['lem']['types_clearing_ex_ante'])
    bottom_value_10 = [0] * len(config['lem']['types_clearing_ex_ante'])
    for q in config['lem']['types_quality']:
        type_quality = config['lem']['types_quality'][q]
        for j in range(len(config['lem']['types_clearing_ex_ante'])):
            type_clearing = config['lem']['types_clearing_ex_ante'][j]
            axs00.bar(j, dict_results_general['df_qty_traded'][f'mc_mean_{type_clearing}'].iloc[-1] *
                      dict_results_quality_shares['shares_offers_cleared'][type_quality][
                          f'mc_mean_{type_clearing}'].iloc[-1] / 100 / 1000,
                      bottom=bottom_value_00[j], label=type_clearing, color=dict_plot[type_quality]['color'])
            bottom_value_00[j] += dict_results_general['df_qty_traded'][f'mc_mean_{type_clearing}'].iloc[-1] * \
                                  dict_results_quality_shares['shares_offers_cleared'][type_quality][
                                      f'mc_mean_{type_clearing}'].iloc[-1] / 100 / 1000
            axs10.bar(j, dict_results_general['df_qty_traded'][f'mc_mean_{type_clearing}'].iloc[-1] *
                      dict_results_quality_shares['shares_bids_cleared'][type_quality][
                          f'mc_mean_{type_clearing}'].iloc[-1] / 100 / 1000,
                      bottom=bottom_value_10[j], label=type_clearing, color=dict_plot[type_quality]['color'])
            bottom_value_10[j] += dict_results_general['df_qty_traded'][f'mc_mean_{type_clearing}'].iloc[-1] * \
                                  dict_results_quality_shares['shares_bids_cleared'][type_quality][
                                      f'mc_mean_{type_clearing}'].iloc[-1] / 100 / 1000
            axs20.bar(j, dict_results_general['df_price_wavg'][f'mc_mean_{type_clearing}'].iloc[-1] / 1e9 * 1000,
                      label=type_clearing, color=dict_plot[type_clearing]['color'])

    axs20.grid()
    axs10.grid()
    axs00.grid()

    fig.subplots_adjust(wspace=0.6, top=0.95, bottom=0.07, left=0.08, right=.95)
    handles, labels = axs02.get_legend_handles_labels()
    fig.legend(handles, labels, bbox_to_anchor=(0.5, .0), loc='lower center', frameon=False, ncol=5)

    plt.savefig(f'{path_figure}/mc_bar_results_energy_prices_shares.png')
    plt.show()


def plot_stacked_bar_results(config,
                             dict_results_quality_shares,
                             path_figure,
                             dict_plot=None):
    # Stacked 100 % bar plot ###
    fig = plt.figure(constrained_layout=False, figsize=(15, 10))
    gs = fig.add_gridspec(2, 1)

    axs0 = fig.add_subplot(gs[0])
    axs1 = fig.add_subplot(gs[1], sharex=axs0)
    axs0.set_xticks(range(len(config['lem']['types_clearing_ex_ante'])))
    axs0.set_xticklabels(config['lem']['types_clearing_ex_ante'].values())
    axs0.set_ylabel('Share of cleared energy [%]')
    axs1.set_ylabel('Share of cleared energy [%]')
    bottom_value_0 = [0] * len(config['lem']['types_clearing_ex_ante'])
    bottom_value_1 = [0] * len(config['lem']['types_clearing_ex_ante'])
    for q in config['lem']['types_quality']:
        type_quality = config['lem']['types_quality'][q]
        for j in range(len(config['lem']['types_clearing_ex_ante'])):
            type_clearing = config['lem']['types_clearing_ex_ante'][j]
            if j == 0:
                axs0.bar(j, dict_results_quality_shares['shares_offers_cleared'][type_quality][
                    f'mc_mean_{type_clearing}'].iloc[-1],
                         bottom=bottom_value_0[j], label=type_quality, color=dict_plot[type_quality]['color'])
                axs1.bar(j, dict_results_quality_shares['shares_bids_cleared'][type_quality][
                    f'mc_mean_{type_clearing}'].iloc[-1],
                         bottom=bottom_value_1[j], label=type_quality, color=dict_plot[type_quality]['color'])
            else:
                axs0.bar(j, dict_results_quality_shares['shares_offers_cleared'][type_quality][
                    f'mc_mean_{type_clearing}'].iloc[-1],
                         bottom=bottom_value_0[j], color=dict_plot[type_quality]['color'])
                axs1.bar(j, dict_results_quality_shares['shares_bids_cleared'][type_quality][
                    f'mc_mean_{type_clearing}'].iloc[-1],
                         bottom=bottom_value_1[j], color=dict_plot[type_quality]['color'])
            bottom_value_1[j] += dict_results_quality_shares['shares_bids_cleared'][type_quality][
                f'mc_mean_{type_clearing}'].iloc[-1]
            bottom_value_0[j] += dict_results_quality_shares['shares_offers_cleared'][type_quality][
                f'mc_mean_{type_clearing}'].iloc[-1]
    axs1.grid()
    axs0.grid()

    fig.subplots_adjust(wspace=0.6, top=0.95, bottom=0.07, left=0.08, right=.95)
    handles, labels = axs0.get_legend_handles_labels()
    fig.legend(handles, labels, bbox_to_anchor=(0.5, .0), loc='lower center', frameon=False, ncol=5)

    plt.savefig(f'{path_figure}/mc_bar_results_energy_prices_shares.png')
    plt.show()


def plot_bars_all_dicts(config,
                        dict_results_quality_shares,
                        path_figure,
                        dict_plot=None):
    # Plotting all dictionaries ###
    fig, axs = plt.subplots(ncols=1, nrows=2, sharex='col', sharey='row', figsize=(15, 10))
    grid = plt.GridSpec(len(dict_results_quality_shares), len(config['lem']['types_quality']))
    dict_axs = {}

    tick_range = np.arange((len(config['lem']['types_clearing_ex_ante']) - 1) / 2,
                           len(config['lem']['types_quality']) * len(
                               config['lem']['types_clearing_ex_ante']) + (
                                   len(config['lem']['types_quality']) - 1),
                           len(config['lem']['types_clearing_ex_ante']) + 1)

    for idx, results_key in zip(range(len(dict_results_quality_shares)), dict_results_quality_shares):
        if idx == 0:
            results_key_0 = results_key
        if dict_axs == {}:
            dict_axs[results_key] = plt.subplot(grid[idx, :])
        else:
            dict_axs[results_key] = plt.subplot(grid[idx, :], sharex=dict_axs[results_key_0])

        for i in range(len(config['lem']['types_quality'])):
            type_quality = config['lem']['types_quality'][i]
            for j in range(len(config['lem']['types_clearing_ex_ante'])):
                type_clearing = config['lem']['types_clearing_ex_ante'][j]
                if i == 0:
                    dict_axs[results_key].bar((i * (len(config['lem']['types_clearing_ex_ante']) + 1)) + j,
                                              dict_results_quality_shares[results_key][type_quality][
                                                  f'mc_mean_{type_clearing}'].iloc[-1],
                                              color=dict_plot[type_clearing]['color'], label=type_clearing)
                else:
                    dict_axs[results_key].bar((i * (len(config['lem']['types_clearing_ex_ante']) + 1)) + j,
                                              dict_results_quality_shares[results_key][type_quality][
                                                  f'mc_mean_{type_clearing}'].iloc[
                                                  -1],
                                              color=dict_plot[type_clearing]['color'])

                if i == 0 and j == 0:
                    dict_axs[results_key].set_ylabel(f'{results_key}')

            dict_axs[results_key].set_xticks(tick_range)
            dict_axs[results_key].set_xticklabels(config['lem']['types_quality'].values())
        dict_axs[results_key].grid()

    dict_axs[results_key].legend(bbox_to_anchor=(0.5, -0.45), loc='lower center', frameon=False, ncol=5)
    plt.tight_layout()
    plt.savefig(f'{path_figure}/mc_bar_results_all_dicts.png')
    plt.show()


def plot_mc_results(config,
                    dict_results_general,
                    dict_results_quality_shares,
                    path_figure, dict_plot=None):
    fig = plt.figure(constrained_layout=False, figsize=(15, 10))
    gs = fig.add_gridspec(3, 6)

    for i in range(len(config['lem']['types_quality'])):
        type_quality = config['lem']['types_quality'][i]
        if i == 0:
            axs01 = fig.add_subplot(gs[0, i + 2])
            axs02 = fig.add_subplot(gs[1, i + 2], sharex=axs01, sharey=axs01)
            axs03 = fig.add_subplot(gs[2, i + 2], sharex=axs01)
        else:
            axs01 = fig.add_subplot(gs[0, i + 2], sharey=axs01)
            axs02 = fig.add_subplot(gs[1, i + 2], sharex=axs01, sharey=axs01)
            axs03 = fig.add_subplot(gs[2, i + 2], sharex=axs01, sharey=axs03)
        axs01.grid()
        axs02.grid()
        axs03.grid()
        for j in range(len(config['lem']['types_clearing_ex_ante'])):
            type_clearing = config['lem']['types_clearing_ex_ante'][j]
            axs01.plot(dict_results_quality_shares['shares_offers_cleared'][type_quality][f'mc_mean_{type_clearing}'],
                       label=type_clearing, color=dict_plot[type_clearing]['color'])
            axs02.plot(dict_results_quality_shares['shares_bids_cleared'][type_quality][f'mc_mean_{type_clearing}'],
                       label=type_clearing, color=dict_plot[type_clearing]['color'])
            axs03.plot(dict_results_quality_shares['prices_quality'][type_quality][
                           f'mc_mean_{type_clearing}'] / 1e9 * 1000,
                       label=type_clearing, color=dict_plot[type_clearing]['color'])

            axs03.set_xlabel('Number of simulations')
            if i == 0 and j == 0:
                axs01.set_ylabel('Share of cleared offers [%]')
                axs02.set_ylabel('Share of cleared bids [%]')
                axs03.set_ylabel('Weighted average prices [€/kWh]')
            if j == 0:
                axs01.set_title(type_quality)

    axs2 = fig.add_subplot(gs[:-1, :-4], sharex=axs01)
    axs3 = fig.add_subplot(gs[2, :-4], sharex=axs01, sharey=axs03)
    for j in range(len(config['lem']['types_clearing_ex_ante'])):
        type_clearing = config['lem']['types_clearing_ex_ante'][j]
        axs2.plot(dict_results_general['df_qty_traded'][f'mc_mean_{type_clearing}'] / 1000,
                  label=type_clearing, color=dict_plot[type_clearing]['color'])
        axs3.plot(dict_results_general['df_price_wavg'][f'mc_mean_{type_clearing}'] / 1e9 * 1000,
                  label=type_clearing, color=dict_plot[type_clearing]['color'])

    axs2.grid()
    axs2.set_ylabel('Average cleared energy [kWh]')
    axs3.grid()
    axs3.set_ylabel('Weighted average price [€/kWh]')
    axs3.set_xlabel('Number of simulations')

    fig.subplots_adjust(wspace=0.6, top=0.95, bottom=0.1, left=0.08, right=.95)
    handles, labels = axs3.get_legend_handles_labels()
    fig.legend(handles, labels, bbox_to_anchor=(0.5, .0), loc='lower center', frameon=False, ncol=5)

    plt.savefig(f'{path_figure}/mc_mean_energy_price_shares.png')
    plt.show()


if __name__ == '__main__':
    # # Run simulations ####################################
    # # load configuration file
    # with open(f"monte_carlo_config.yaml") as config_file:
    #     config_mc = yaml.load(config_file, Loader=yaml.FullLoader)
    # # Create simulation directory
    # t_current = pd.Timestamp.now().strftime('%Y-%m-%d_%H-%M-%S')
    # path_sim, path_input, path_output, path_results, path_figures = create_simulation_folder(
    #     simulation_path=f'simulations/{t_current}')
    # # Save configuration to simulation folder
    # shutil.copyfile(src=f"monte_carlo_config.yaml", dst=f"{path_sim}/mc_config.yaml")
    # print('Task: Calculate single clearings.')
    # # Prepare parallelization
    # num_cores = mp.cpu_count()
    # # Run lem iterations for mc simulation
    # with mp.Pool(processes=num_cores,
    #              initializer=_init_db_obj_workers,
    #              initargs=(single_lem_simulation,
    #                        config_mc["db_connections"]["database_connection_user"],
    #                        config_mc,
    #                        path_input,
    #                        path_output
    #                        )) as pool:
    #     pool.map(single_lem_simulation, list(range(0, config_mc['monte_carlo']['n_iterations'])))
    #
    # pool.close()

    # Load pre-existing results and configurations ##########
    path_sim = 'simulations/2021-03-21_21-07-31/'
    # load configuration file
    with open(f"{path_sim}/mc_config.yaml") as config_file:
        config_mc = yaml.load(config_file, Loader=yaml.FullLoader)
    path_input = path_sim + 'input'
    path_output = path_sim + 'output'
    path_results = path_sim + 'results'
    path_figures = path_sim + 'figures'

    # # Analyze simulation results ############################
    # print('Task: Analyze and aggregate clearings.')
    # # Analyze lem simulations according to monte carlo
    # dict_general_monte_carlo, dict_quality_shares_monte_carlo = \
    #     analyze_lem_simulations(config_mc=config_mc, path_output=path_output)
    # # Save mc results to
    # with open(f'{path_results}/dict_general_monte_carlo.p', 'wb') as fp:
    #     pickle.dump(dict_general_monte_carlo, fp, protocol=pickle.HIGHEST_PROTOCOL)
    # with open(f'{path_results}/dict_quality_shares_monte_carlo.p', 'wb') as fp:
    #     pickle.dump(dict_quality_shares_monte_carlo, fp, protocol=pickle.HIGHEST_PROTOCOL)

    # Plot monte carlo results ################################
    print('Task: Plot results.')
    with open(f'{path_results}/dict_general_monte_carlo.p', 'rb') as fp:
        dict_general_monte_carlo = pickle.load(fp)
    with open(f'{path_results}/dict_quality_shares_monte_carlo.p', 'rb') as fp:
        dict_quality_shares_monte_carlo = pickle.load(fp)

    # Plot monte carlo results
    dict_plot_clearing_types = {}
    cm = plt.get_cmap('Set2')
    dark2 = plt.get_cmap('Dark2')
    for j in range(len(config_mc['lem']['types_clearing_ex_ante'])):
        clearing_type = config_mc['lem']['types_clearing_ex_ante'][j]
        dict_plot_clearing_types[clearing_type] = {'color': cm.colors[j]}
    dict_plot_clearing_types['na'] = {'color': dark2.colors[-1]}
    dict_plot_clearing_types['local'] = {'color': dark2.colors[-3]}
    dict_plot_clearing_types['green'] = {'color': dark2.colors[-4]}
    dict_plot_clearing_types['green_local'] = {'color': dark2.colors[0]}
    plot_mc_results_with_welfare(config=config_mc,
                                 dict_results_general=dict_general_monte_carlo,
                                 path_figure=path_figures,
                                 dict_plot=dict_plot_clearing_types)
    plot_mc_results_bar_algorithms(config=config_mc,
                                   dict_results_general=dict_general_monte_carlo,
                                   dict_results_quality_shares=dict_quality_shares_monte_carlo,
                                   path_figure=path_figures,
                                   dict_plot=dict_plot_clearing_types)
    plot_mc_results_bar_qualities(config=config_mc,
                                  dict_results_general=dict_general_monte_carlo,
                                  dict_results_quality_shares=dict_quality_shares_monte_carlo,
                                  path_figure=path_figures,
                                  dict_plot=dict_plot_clearing_types)
    plot_stacked_bar_results(config=config_mc,
                             dict_results_quality_shares=dict_quality_shares_monte_carlo,
                             path_figure=path_figures,
                             dict_plot=dict_plot_clearing_types)
    plot_bars_all_dicts(config=config_mc,
                        dict_results_quality_shares=dict_quality_shares_monte_carlo,
                        path_figure=path_figures,
                        dict_plot=dict_plot_clearing_types)
    plot_mc_results(config=config_mc,
                    dict_results_general=dict_general_monte_carlo,
                    dict_results_quality_shares=dict_quality_shares_monte_carlo,
                    path_figure=path_figures, dict_plot=dict_plot_clearing_types)
