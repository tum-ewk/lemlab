import multiprocessing as mp
import os
import pickle
import shutil
import time
import traceback
import tikzplotlib
from pathlib import Path
import random

import numpy as np
import pandas as pd
import yaml
from matplotlib import pyplot as plt
from tqdm import tqdm

from lem_analysis.random_lem_fcts import create_random_positions, create_user_ids
from lemlab.db_connection.db_connection import DatabaseConnection
from lemlab.lem.clearing_ex_ante import clearing_pda, clearing_pp, clearing_cc, calc_market_position_shares, \
    _convert_qualities_to_int


def run_clearings(db_obj,
                  config_lem,
                  type_clearing,
                  offers,
                  bids,
                  cc_max_while_exec=None,
                  n_test_case=0,
                  verbose=False
                  ):
    if verbose:
        print(f'\nTest case #{n_test_case}')

    positions_cleared = pd.DataFrame()

    t_clearing_start = time.time()
    if cc_max_while_exec is None:
        cc_max_while_exec = 100

    # Combinations WITHOUT consideration of quality premium
    if 'pda' == type_clearing:
        positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
            clearing_pda(db_obj,
                         config_lem,
                         offers,
                         bids)

    if 'h2l' == type_clearing:
        positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
            clearing_pp(db_obj,
                        config_lem,
                        offers,
                        bids,
                        type_prioritization='h2l')

    if 'l2h' == type_clearing:
        positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
            clearing_pp(db_obj,
                        config_lem,
                        offers,
                        bids,
                        type_prioritization='l2h')

    if 'sep' == type_clearing:
        positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
            clearing_pp(db_obj,
                        config_lem,
                        offers,
                        bids,
                        type_prioritization='sep')

    if 'cc' == type_clearing:
        positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
            clearing_cc(db_obj,
                        config_lem,
                        offers,
                        bids,
                        max_while_executions=cc_max_while_exec,
                        verbose=verbose)

    if 'cc_h2l' == type_clearing:
        positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
            clearing_cc(db_obj,
                        config_lem,
                        offers,
                        bids,
                        max_while_executions=cc_max_while_exec,
                        verbose=verbose)

        if not bids_uncleared.empty and not offers_uncleared.empty:
            # Clear in a preference prioritization for remaining positions
            results_pp, offers_uncleared_pp, bids_uncleared_pp, offers_cleared_pp, bids_cleared_pp = \
                clearing_pp(db_obj=db_obj,
                            config_lem=config_lem,
                            offers=offers_uncleared,
                            bids=bids_uncleared,
                            type_clearing=type_clearing,
                            type_prioritization='h2l')
            positions_cleared = positions_cleared.append(results_pp).reset_index(drop=True)

    # Combinations WITH consideration of quality premium ###
    # Standard da AFTER advanced clearing
    if 'cc_h2l_pda' == type_clearing:
        positions_cleared, offers_uncleared, bids_uncleared, offers_cleared, bids_cleared = \
            clearing_cc(db_obj,
                        config_lem,
                        offers,
                        bids,
                        max_while_executions=cc_max_while_exec,
                        add_premium=True,
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
                            type_prioritization='h2l')
            positions_cleared = positions_cleared.append(results_pp).reset_index(drop=True)

            positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                clearing_pda(db_obj,
                             config_lem,
                             offers_uncleared_pp,
                             bids_uncleared_pp,
                             add_premium=False, )

            positions_cleared = positions_cleared.append(positions_cleared_da, ignore_index=True)

    if not positions_cleared.empty and config_lem['share_quality_logging_extended']:
        positions_cleared = calc_market_position_shares(db_obj, config_lem, offers, bids, positions_cleared)

    t_clearing_end = time.time()
    t_clearing = t_clearing_end - t_clearing_start

    return positions_cleared, t_clearing


if __name__ == '__main__':
    # load configuration file
    with open(f"time_complexity_config.yaml") as config_file:
        config_tc = yaml.load(config_file, Loader=yaml.FullLoader)

    db_obj = DatabaseConnection(db_dict=config_tc["db_connections"]["database_connection_user"],
                                lem_config=config_tc['lem'])

    timing_dict = dict()
    timing_dict["config"] = pd.DataFrame(config_tc["time_complexity"], index=[0])

    for type_clearing in config_tc['lem']['types_clearing_ex_ante'].values():
        timing_dict[type_clearing] = pd.DataFrame(index=range(config_tc["time_complexity"]["n_samples"]),
                                                  columns=range(config_tc["time_complexity"]["min_positions"],
                                                                config_tc["time_complexity"]["max_positions"],
                                                                config_tc["time_complexity"]["step_size"]))

    for n_positions in range(config_tc["time_complexity"]["min_positions"],
                             config_tc["time_complexity"]["max_positions"],
                             config_tc["time_complexity"]["step_size"]):

        print(f"Number of positions: {n_positions}")

        # Create list of random user ids
        ids_users_random = create_user_ids(num=n_positions * 10)

        for sample in range(config_tc["time_complexity"]["n_samples"]):
            print(f"Sample number: {sample}")

            # Compute random market positions
            positions = create_random_positions(db_obj=db_obj,
                                                config=config_tc,
                                                ids_user=ids_users_random,
                                                n_positions=n_positions,
                                                verbose=False)

            # Extract bids and offers
            bids = positions[positions['type_position'] == 'bid']
            offers = positions[positions['type_position'] == 'offer']
            bids = _convert_qualities_to_int(db_obj, bids, config_tc['lem']['types_quality'])
            offers = _convert_qualities_to_int(db_obj, offers, config_tc['lem']['types_quality'])
            for type_clearing in config_tc['lem']['types_clearing_ex_ante'].values():
                print(f"Clearing type: {type_clearing}")

                # run clearings and save to files
                positions_cleared, t_clearing = run_clearings(db_obj=db_obj,
                                                              config_lem=config_tc['lem'],
                                                              type_clearing=type_clearing,
                                                              offers=offers,
                                                              bids=bids,
                                                              cc_max_while_exec=config_tc["lem"]["cc_max_while_exec"],
                                                              n_test_case=sample,
                                                              verbose=False)

                timing_dict[type_clearing].loc[sample, n_positions] = t_clearing

    t_current_str = pd.Timestamp.now().strftime("%Y-%m-%d-%H-%M-%S")
    file_name_timing_results = f"{t_current_str}_timing_results"
    writer = pd.ExcelWriter(f"simulations\\{file_name_timing_results}.xlsx")
    for key, df in timing_dict.items():
        df.to_excel(writer, sheet_name=key)
    writer.save()

    excel_file = pd.ExcelFile(f"simulations\\{file_name_timing_results}.xlsx")
    timing_results_dict = {}
    for sheet in excel_file.sheet_names:
        timing_results_dict[sheet] = pd.read_excel(excel_file, sheet, index_col=0)

    for key, value in timing_results_dict.items():
        if key != "config":
            timing_results_dict[key].mean().plot(label=key)
    plt.legend()
    plt.show()
