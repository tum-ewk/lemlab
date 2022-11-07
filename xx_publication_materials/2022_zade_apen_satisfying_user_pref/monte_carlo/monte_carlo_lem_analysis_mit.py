import os
import pickle
import shutil
import time
import traceback
import random
import multiprocessing as mp
import numpy as np
import pandas as pd

from ruamel.yaml import YAML
from matplotlib import pyplot as plt
from pathlib import Path

from random_lem_fcts import create_random_positions, create_user_ids
from lemlab.db_connection.db_connection import DatabaseConnection
from lemlab.lem.clearing_ex_ante import clearing_pda, clearing_pp, clearing_cc, calc_market_position_shares, \
    convert_qualities_to_int


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

    t_clearings = pd.DataFrame()
    positions_cleared = pd.DataFrame()

    t_clearing_start = round(time.time())
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
            positions_cleared = pd.concat([positions_cleared, results_pp]).reset_index(drop=True)

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
            positions_cleared = pd.concat([positions_cleared, results_pp]).reset_index(drop=True)

            positions_cleared_da, offers_uncleared_da, bids_uncleared_da, offers_cleared_da, bids_cleared_da = \
                clearing_pda(db_obj,
                             config_lem,
                             offers_uncleared_pp,
                             bids_uncleared_pp,
                             add_premium=False, )

            positions_cleared = pd.concat([positions_cleared, positions_cleared_da], ignore_index=True)

    if not positions_cleared.empty and config_lem['share_quality_logging_extended']:
        positions_cleared = calc_market_position_shares(db_obj, config_lem, offers, bids, positions_cleared)

    t_clearing_end = round(time.time())
    t_clearings.at[n_test_case, type_clearing] = t_clearing_end - t_clearing_start

    return positions_cleared, t_clearings


def create_simulation_folder(config, simulation_path='test/'):
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
    for type_clearing in config['lem']['types_clearing_ex_ante'].values():
        path_out_clearing = path_out + '/' + type_clearing
        Path(path_out_clearing).mkdir(parents=True, exist_ok=False)
        for i in range(config['monte_carlo']['n_trials']):
            path_out_trial = path_out_clearing + '/' + str(i)
            Path(path_out_trial).mkdir(parents=True, exist_ok=False)

    for i in range(config['monte_carlo']['n_trials']):
        path_in_trial = path_in + '/' + str(i)
        Path(path_in_trial).mkdir(parents=True, exist_ok=False)

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
    function.cc_max_while_exec = config['lem']['cc_max_while_exec']


def single_lem_simulation(test_case_number):
    # Create list of random user ids
    ids_users_random = create_user_ids(num=single_lem_simulation.config['prosumer']['general_number_of'])
    for trial in range(single_lem_simulation.config['monte_carlo']['n_trials']):
        # Compute random market positions
        positions = create_random_positions(db_obj=single_lem_simulation.db_obj,
                                            config=single_lem_simulation.config,
                                            ids_user=ids_users_random,
                                            n_positions=single_lem_simulation.config['monte_carlo'][
                                                'n_positions_per_iteration'],
                                            verbose=False)

        # Store positions as input to file
        positions.reset_index().to_feather(path=f'{single_lem_simulation.path_input}/'
                                                f'{trial}/'
                                                f'{test_case_number}.ft')

        # Extract bids and offers
        bids = positions[positions['type_position'] == 'bid']
        offers = positions[positions['type_position'] == 'offer']
        bids = convert_qualities_to_int(single_lem_simulation.db_obj,
                                         bids, single_lem_simulation.config['lem']['types_quality'])
        offers = convert_qualities_to_int(single_lem_simulation.db_obj,
                                           offers, single_lem_simulation.config['lem']['types_quality'])
        for clearing in single_lem_simulation.config['lem']['types_clearing_ex_ante'].values():
            # run clearings and save to files
            positions_cleared, t_clearing = run_clearings(db_obj=single_lem_simulation.db_obj,
                                                          config_lem=single_lem_simulation.config['lem'],
                                                          type_clearing=clearing,
                                                          offers=offers,
                                                          bids=bids,
                                                          cc_max_while_exec=single_lem_simulation.cc_max_while_exec,
                                                          n_test_case=test_case_number,
                                                          verbose=False)
            positions_cleared.reset_index().to_feather(path=f'{single_lem_simulation.path_output}/'
                                                            f'{clearing}/'
                                                            f'{trial}/'
                                                            f'{test_case_number}.ft')


def extract_relevant_data(config_mc, positions_cleared, positions_placed):
    try:
        data_dict = {}
        # Extract buy and ask bids
        bids = positions_placed[positions_placed['type_position'] == 'bid']
        offers = positions_placed[positions_placed['type_position'] == 'offer']
        # Sum absolute energy quantites placed on lem
        data_dict['qty_energy_bids_cum'] = bids['qty_energy'].sum()
        data_dict['qty_energy_offers_cum'] = offers['qty_energy'].sum()
        # Calculate energy shares of buy and ask bids
        data_dict['share_bids'] = round(bids['qty_energy'].sum() / positions_placed['qty_energy'].sum() * 100, 3)
        data_dict['share_offers'] = round(offers['qty_energy'].sum() / positions_placed['qty_energy'].sum() * 100, 3)
        # Go through all energy qualities
        for j in reversed(range(len(config_mc['lem']['types_quality']))):
            type_quality = config_mc['lem']['types_quality'][j]
            # Extract all buy and ask bids with specific quality
            bids_quality = bids[bids['quality_energy'] == type_quality]
            offers_quality = offers[offers['quality_energy'] == type_quality]
            # Calculate share of placed buy and ask bids with specific energy quality/preference
            data_dict['shares_bids_' + type_quality] = round(bids_quality['qty_energy'].sum() / bids[
                'qty_energy'].sum() * 100, 3)
            data_dict['shares_offers_' + type_quality] = round(offers_quality['qty_energy'].sum() / offers[
                'qty_energy'].sum() * 100, 3)
            # Calculate weighted average prices of buy and ask bids with specific energy quality/preference
            data_dict['prices_wavg_bids_' + type_quality] = round(np.average(bids_quality['price_energy'],
                                                                             weights=bids_quality['qty_energy']), 3)
            data_dict['prices_wavg_offers_' + type_quality] = round(np.average(offers_quality['price_energy'],
                                                                               weights=offers_quality['qty_energy']), 3)
            data_dict['qty_energy_bids_' + type_quality] = round(np.average(bids_quality['qty_energy']), 3)
            data_dict['qty_energy_offers_' + type_quality] = round(np.average(offers_quality['qty_energy']), 3)
            data_dict['premium_wavg_bids_' + type_quality] = round(np.average(
                bids_quality['premium_preference_quality'], weights=bids_quality['qty_energy']), 3)
        if positions_cleared.empty:
            data_dict['qty_traded'] = [0]
            data_dict['price_wavg'] = np.NaN
            data_dict['welfare'] = 0
            for j in reversed(range(len(config_mc['lem']['types_quality']))):
                type_quality = config_mc['lem']['types_quality'][j]
                data_dict['shares_offers_cleared_' + type_quality] = np.NaN
                data_dict['shares_bids_cleared_' + type_quality] = np.NaN
                data_dict['shares_offers_cleared_all_' + type_quality] = np.NaN
                data_dict['shares_bids_cleared_all_' + type_quality] = np.NaN
                data_dict['shares_bids_satis_' + type_quality] = np.NaN
                data_dict['prices_quality_' + type_quality] = np.NaN
        else:
            data_dict['qty_traded'] = [positions_cleared['qty_energy_traded'].sum()]
            data_dict['price_wavg'] = np.average(positions_cleared['price_energy_market_uniform'],
                                                 weights=positions_cleared['qty_energy_traded'])
            cons_surplus = ((positions_cleared['price_energy_bid'] - positions_cleared['price_energy_market_uniform']) *
                            positions_cleared['qty_energy_traded']).sum()
            prod_surplus = (
                    (positions_cleared['price_energy_market_uniform'] - positions_cleared['price_energy_offer']) *
                    positions_cleared['qty_energy_traded']).sum()
            data_dict['welfare'] = cons_surplus + prod_surplus

            share_quality_offers_cleared_excess = 0  # initialize excess offer share with zero
            for j in reversed(range(len(config_mc['lem']['types_quality']))):
                type_quality = config_mc['lem']['types_quality'][j]
                # Share quality offers in cleared positions
                share_quality_offers_cleared = np.average(
                    positions_cleared.loc[:, f'share_quality_offers_cleared_{type_quality}'],
                    weights=positions_cleared.loc[:, 'qty_energy_traded'])
                data_dict['shares_offers_cleared_' + type_quality] = share_quality_offers_cleared

                if config_mc['lem']['share_quality_logging_extended']:
                    # Share preference bids in cleared positions
                    share_preference_bids_cleared = np.average(
                        positions_cleared.loc[:, f'share_preference_bids_cleared_{type_quality}'],
                        weights=positions_cleared.loc[:, 'qty_energy_traded'])
                    data_dict['shares_bids_cleared_' + type_quality] = share_preference_bids_cleared
                    # Share quality offers cleared of all quality offers
                    share_quality_offers_cleared_all = np.average(
                        positions_cleared.loc[:, f'share_quality_offers_cleared_{type_quality}'] /
                        positions_cleared.loc[:, f'share_quality_offers_{type_quality}'],
                        weights=positions_cleared.loc[:, 'qty_energy_traded'])
                    data_dict['shares_offers_cleared_all_' + type_quality] = share_quality_offers_cleared_all
                    # Share preference bids cleared of all preference bids
                    share_preference_bids_cleared_all = np.average(
                        positions_cleared.loc[:, f'share_preference_bids_cleared_{type_quality}'] /
                        positions_cleared.loc[:, f'share_preference_bids_{type_quality}'],
                        weights=positions_cleared.loc[:, 'qty_energy_traded'])
                    data_dict['shares_bids_cleared_all_' + type_quality] = share_preference_bids_cleared_all
                    # Share preference bids satisfied by offers in cleared positions
                    if share_preference_bids_cleared > 0:
                        data_dict['shares_bids_satis_' + type_quality] = \
                            (share_quality_offers_cleared + share_quality_offers_cleared_excess)\
                            / share_preference_bids_cleared
                        if share_quality_offers_cleared \
                                + share_quality_offers_cleared_excess - share_preference_bids_cleared > 0:
                            share_quality_offers_cleared_excess = \
                                share_quality_offers_cleared_excess \
                                + share_quality_offers_cleared - share_preference_bids_cleared
                        else:
                            share_quality_offers_cleared_excess = 0

                if positions_cleared.loc[:, f'share_quality_offers_cleared_{type_quality}'].sum() == 0:
                    price_quality = 0
                else:
                    price_quality = np.average(positions_cleared['price_energy_market_uniform'],
                                               weights=positions_cleared.loc[:,
                                                       f'share_quality_offers_cleared_{type_quality}']
                                                       * positions_cleared.loc[:, 'qty_energy_traded'])
                data_dict['prices_quality_' + type_quality] = price_quality

        df_data = pd.DataFrame(data_dict)
    except Exception as e:
        print(e)
        traceback.print_exc()

    return df_data


def plot_mc_results_offers_cleared_95(config,
                                      results_dict,
                                      path_figure,
                                      dict_plot=None):
    # fig = plt.figure(constrained_layout=False, figsize=(15, 7))
    # gs = fig.add_gridspec(1, len(config['lem']['types_clearing_ex_ante']) + 2)
    # # plt.rcParams['font.size'] = 12
    # # plt.rcParams['legend.fontsize'] = 12
    # for i in range(len(config['lem']['types_clearing_ex_ante'])):
    #     if i == 0:
    #         axs02 = fig.add_subplot(gs[0, 2 + i])
    #         axs02.set_ylabel('Share of offer qualities cleared [%]')
    #
    #     else:
    #         axs02 = fig.add_subplot(gs[0, 2 + i], sharex=axs02, sharey=axs02)
    #     type_clearing = config['lem']['types_clearing_ex_ante'][i]
    #     axs02.set_title(type_clearing)
    #     axs02.set_xticks([])
    #     axs02.set_xticklabels([])
    #     for j in range(len(config['lem']['types_quality'])):
    #         type_quality = config['lem']['types_quality'][j]
    #         axs02.bar(x=j,
    #                   height=results_dict[type_clearing]['df_mean']['shares_offers_cleared_' + type_quality],
    #                   yerr=results_dict[type_clearing]['df_std']['shares_offers_cleared_' + type_quality] * 2,
    #                   color=dict_plot[type_quality]['color'], label=type_quality)
    #     axs02.grid(axis='y')
    #
    # axs00 = fig.add_subplot(gs[0, :-len(config['lem']['types_clearing_ex_ante'])])
    # axs00.set_xticks(list(range(len(config['lem']['types_clearing_ex_ante']))))
    # axs00.set_xticklabels(config['lem']['types_clearing_ex_ante'].values())
    # axs00.set_ylabel('Average cleared energy [kWh]')
    # bottom_value_00 = [0] * len(config['lem']['types_clearing_ex_ante'])
    # for q in config['lem']['types_quality']:
    #     type_quality = config['lem']['types_quality'][q]
    #     for j in range(len(config['lem']['types_clearing_ex_ante'])):
    #         type_clearing = config['lem']['types_clearing_ex_ante'][j]
    #         if q == len(config['lem']['types_quality']) - 1:
    #             axs00.bar(x=j,
    #                       height=results_dict[type_clearing]['df_mean']['qty_traded'] *
    #                              results_dict[type_clearing]['df_mean'][
    #                                  'shares_offers_cleared_' + type_quality] / 100 / 1000,
    #                       yerr=results_dict[type_clearing]['df_std']['qty_traded'] / 1000 * 2,
    #                       bottom=bottom_value_00[j], label=type_clearing, color=dict_plot[type_quality]['color'])
    #         else:
    #             axs00.bar(x=j,
    #                       height=results_dict[type_clearing]['df_mean']['qty_traded'] *
    #                              results_dict[type_clearing]['df_mean'][
    #                                  'shares_offers_cleared_' + type_quality] / 100 / 1000,
    #                       bottom=bottom_value_00[j], label=type_clearing, color=dict_plot[type_quality]['color'])
    #             bottom_value_00[j] += results_dict[type_clearing]['df_mean']['qty_traded'] * \
    #                                   results_dict[type_clearing]['df_mean']['shares_offers_cleared_' +
    #                                                                          type_quality] / 100 / 1000
    #
    # axs00.grid(axis='y')
    #
    # fig.subplots_adjust(wspace=0.5, top=0.95, bottom=0.15, left=0.05, right=.98)
    # handles, labels = axs02.get_legend_handles_labels()
    # fig.legend(handles, labels, bbox_to_anchor=(0.5, .03), loc='lower center', frameon=False, ncol=5)
    #
    # plt.xticks(rotation=90)
    # tikzplotlib.save(f'{path_figure}/mc_results_shares_offers_cleared_95.tex')
    # plt.savefig(f'{path_figure}/mc_results_shares_offers_cleared_95.png')
    # plt.show()

    fig, axs = plt.subplots(constrained_layout=False, figsize=(10, 5))
    for i in range(len(config['lem']['types_clearing_ex_ante'])):
        plt.xlabel('Share of offers cleared [%]')
        plt.yticks(ticks=list(config['lem']['types_clearing_ex_ante'].keys()),
                   labels=config['lem']['types_clearing_ex_ante'].values())
        axs.spines["right"].set_visible(False)
        axs.spines["top"].set_visible(False)
        type_clearing = config['lem']['types_clearing_ex_ante'][i]
        for j in range(len(config['lem']['types_quality'])):
            type_quality = config['lem']['types_quality'][j]
            if i == 0:
                plt.errorbar(x=results_dict[type_clearing]['df_mean']['shares_offers_cleared_' + type_quality],
                             y=i,
                             xerr=results_dict[type_clearing]['df_std']['shares_offers_cleared_' + type_quality] * 2,
                             marker='x',
                             markersize=10,
                             markeredgewidth=2,
                             ecolor='gray',
                             elinewidth=2,
                             color=dict_plot[type_quality]['color'], label=type_quality)
            else:
                plt.errorbar(x=results_dict[type_clearing]['df_mean']['shares_offers_cleared_' + type_quality],
                             y=i,
                             xerr=results_dict[type_clearing]['df_std']['shares_offers_cleared_' + type_quality] * 2,
                             marker='x',
                             markersize=10,
                             markeredgewidth=2,
                             ecolor='gray',
                             elinewidth=2,
                             color=dict_plot[type_quality]['color'])
        plt.grid(axis='y')

    fig.subplots_adjust(wspace=0.2, top=0.95, bottom=0.22, left=0.1, right=.98)
    handles, labels = axs.get_legend_handles_labels()
    fig.legend(handles, labels, bbox_to_anchor=(0.5, .03), loc='lower center', frameon=False, ncol=5)

    # tikzplotlib.save(f'{path_figure}/mc_results_shares_offers_cleared_95.tex')
    plt.savefig(f'{path_figure}/mc_results_shares_offers_cleared_95.png')
    plt.show()


def plot_mc_results_bids_cleared_95(config,
                                    results_dict,
                                    path_figure,
                                    dict_plot=None):
    # fig = plt.figure(constrained_layout=False, figsize=(15, 7))
    # gs = fig.add_gridspec(1, len(config['lem']['types_clearing_ex_ante']) + 2)
    # # plt.rcParams['font.size'] = 12
    # # plt.rcParams['legend.fontsize'] = 12
    # for i in range(len(config['lem']['types_clearing_ex_ante'])):
    #     if i == 0:
    #         axs02 = fig.add_subplot(gs[0, 2 + i])
    #         axs02.set_ylabel('Share of bid preferences cleared [%]')
    #     else:
    #         axs02 = fig.add_subplot(gs[0, 2 + i], sharex=axs02, sharey=axs02)
    #     axs02.set_xticks([])
    #     axs02.set_xticklabels([])
    #     type_clearing = config['lem']['types_clearing_ex_ante'][i]
    #     axs02.set_title(type_clearing)
    #     for j in range(len(config['lem']['types_quality'])):
    #         type_quality = config['lem']['types_quality'][j]
    #         axs02.bar(x=j,
    #                   height=results_dict[type_clearing]['df_mean']['shares_bids_cleared_' + type_quality],
    #                   yerr=results_dict[type_clearing]['df_std']['shares_bids_cleared_' + type_quality] * 2,
    #                   color=dict_plot[type_quality]['color'], label=type_quality)
    #     axs02.grid(axis='y')
    #
    # axs00 = fig.add_subplot(gs[0, :-len(config['lem']['types_clearing_ex_ante'])])
    # axs00.set_xticks(list(range(len(config['lem']['types_clearing_ex_ante']))))
    # axs00.set_xticklabels(config['lem']['types_clearing_ex_ante'].values())
    # axs00.set_ylabel('Average cleared energy [kWh]')
    # bottom_value_00 = [0] * len(config['lem']['types_clearing_ex_ante'])
    # for q in config['lem']['types_quality']:
    #     type_quality = config['lem']['types_quality'][q]
    #     for j in range(len(config['lem']['types_clearing_ex_ante'])):
    #         type_clearing = config['lem']['types_clearing_ex_ante'][j]
    #         if q == len(config['lem']['types_quality']) - 1:
    #             axs00.bar(x=j,
    #                       height=results_dict[type_clearing]['df_mean']['qty_traded'] *
    #                              results_dict[type_clearing]['df_mean'][
    #                                  'shares_bids_cleared_' + type_quality] / 100 / 1000,
    #                       yerr=results_dict[type_clearing]['df_std']['qty_traded'] / 1000 * 2,
    #                       bottom=bottom_value_00[j], label=type_clearing, color=dict_plot[type_quality]['color'])
    #         else:
    #             axs00.bar(x=j,
    #                       height=results_dict[type_clearing]['df_mean']['qty_traded'] *
    #                              results_dict[type_clearing]['df_mean'][
    #                                  'shares_bids_cleared_' + type_quality] / 100 / 1000,
    #                       bottom=bottom_value_00[j], label=type_clearing, color=dict_plot[type_quality]['color'])
    #         bottom_value_00[j] += results_dict[type_clearing]['df_mean']['qty_traded'] * \
    #                               results_dict[type_clearing]['df_mean']['shares_bids_cleared_' +
    #                                                                      type_quality] / 100 / 1000
    #
    # axs00.grid(axis='y')
    #
    # fig.subplots_adjust(wspace=0.5, top=0.95, bottom=0.15, left=0.05, right=.98)
    # handles, labels = axs02.get_legend_handles_labels()
    # fig.legend(handles, labels, bbox_to_anchor=(0.5, .03), loc='lower center', frameon=False, ncol=5)
    #
    # plt.xticks(rotation=90)
    # tikzplotlib.save(f'{path_figure}/mc_results_shares_bids_cleared_95.tex')
    # plt.savefig(f'{path_figure}/mc_results_shares_bids_cleared_95.png')
    # plt.show()

    fig, axs = plt.subplots(constrained_layout=False, figsize=(10, 5))
    for i in range(len(config['lem']['types_clearing_ex_ante'])):
        plt.xlabel('Share of bids cleared [%]')
        plt.yticks(ticks=list(config['lem']['types_clearing_ex_ante'].keys()),
                   labels=config['lem']['types_clearing_ex_ante'].values())
        axs.spines["right"].set_visible(False)
        axs.spines["top"].set_visible(False)
        type_clearing = config['lem']['types_clearing_ex_ante'][i]
        for j in range(len(config['lem']['types_quality'])):
            type_quality = config['lem']['types_quality'][j]
            if i == 0:
                plt.errorbar(x=results_dict[type_clearing]['df_mean']['shares_bids_cleared_' + type_quality],
                             y=i,
                             xerr=results_dict[type_clearing]['df_std']['shares_bids_cleared_' + type_quality] * 2,
                             marker='x',
                             markersize=10,
                             markeredgewidth=2,
                             ecolor='gray',
                             elinewidth=2,
                             color=dict_plot[type_quality]['color'], label=type_quality)
            else:
                plt.errorbar(x=results_dict[type_clearing]['df_mean']['shares_bids_cleared_' + type_quality],
                             y=i,
                             xerr=results_dict[type_clearing]['df_std']['shares_bids_cleared_' + type_quality] * 2,
                             marker='x',
                             markersize=10,
                             markeredgewidth=2,
                             ecolor='gray',
                             elinewidth=2,
                             color=dict_plot[type_quality]['color'])
        plt.grid(axis='y')

    fig.subplots_adjust(wspace=0.2, top=0.95, bottom=0.22, left=0.1, right=.98)
    handles, labels = axs.get_legend_handles_labels()
    fig.legend(handles, labels, bbox_to_anchor=(0.5, .03), loc='lower center', frameon=False, ncol=5)

#    tikzplotlib.save(f'{path_figure}/mc_results_shares_bids_cleared_95.tex')
    plt.savefig(f'{path_figure}/mc_results_shares_bids_cleared_95.png')
    plt.show()


def plot_mc_results_placed_positions_95(config,
                                        results_dict,
                                        path_figure,
                                        dict_plot=None):
    fig = plt.figure(constrained_layout=False, figsize=(10, 10))
    gs = fig.add_gridspec(3, 2)
    axs00 = fig.add_subplot(gs[0, 0])
    axs01 = fig.add_subplot(gs[0, 1], sharey=axs00, sharex=axs00)
    axs00.set_ylabel('Quantity of energy [kWh]')
    axs01.set_title('Offers')
    axs00.set_title('Bids')
    axs10 = fig.add_subplot(gs[1, 0], sharex=axs00)
    axs11 = fig.add_subplot(gs[1, 1], sharey=axs10, sharex=axs00)
    axs10.set_ylabel('Prices of energy [€/kWh]')
    axs20 = fig.add_subplot(gs[2, 0], sharex=axs00)
    axs20.set_ylabel('Price premiums [%]')
    axs00.set_xticks([])

    for j in range(len(config['lem']['types_quality'])):
        type_quality = config['lem']['types_quality'][j]
        axs01.bar(x=j,
                  height=results_dict['pda']['df_mean']['qty_energy_offers_' + type_quality] / 1000,
                  yerr=results_dict['pda']['df_std']['qty_energy_offers_' + type_quality] / 1000 * 2,
                  color=dict_plot[type_quality]['color'], label=type_quality)
        axs00.bar(x=j,
                  height=results_dict['pda']['df_mean']['qty_energy_bids_' + type_quality] / 1000,
                  yerr=results_dict['pda']['df_std']['qty_energy_bids_' + type_quality] / 1000 * 2,
                  color=dict_plot[type_quality]['color'], label=type_quality)
        axs11.bar(x=j,
                  height=results_dict['pda']['df_mean']['prices_wavg_offers_' + type_quality] / 1e9 * 1000,
                  yerr=results_dict['pda']['df_std']['prices_wavg_offers_' + type_quality] / 1e9 * 1000 * 2,
                  color=dict_plot[type_quality]['color'], label=type_quality)
        axs10.bar(x=j,
                  height=results_dict['pda']['df_mean']['prices_wavg_bids_' + type_quality] / 1e9 * 1000,
                  yerr=results_dict['pda']['df_std']['prices_wavg_bids_' + type_quality] / 1e9 * 1000 * 2,
                  color=dict_plot[type_quality]['color'], label=type_quality)
        axs20.bar(x=j,
                  height=results_dict['pda']['df_mean']['premium_wavg_bids_' + type_quality],
                  yerr=results_dict['pda']['df_std']['premium_wavg_bids_' + type_quality] * 2,
                  color=dict_plot[type_quality]['color'], label=type_quality)
    axs01.grid(axis='y')
    axs00.grid(axis='y')
    axs11.grid(axis='y')
    axs10.grid(axis='y')
    axs20.grid(axis='y')

    fig.subplots_adjust(wspace=0.2, top=0.95, bottom=0.1, left=0.08, right=.98)
    handles, labels = axs01.get_legend_handles_labels()
    fig.legend(handles, labels, bbox_to_anchor=(0.5, .03), loc='lower center', frameon=False, ncol=5)
    # tikzplotlib.save(f'{path_figure}/mc_results_placed_positions_95.tex')
    plt.savefig(f'{path_figure}/mc_results_placed_positions_95.png')
    plt.show()


def plot_mc_results_prices_wavg_95(config,
                                   results_dict,
                                   path_figure,
                                   dict_plot=None):
    # fig = plt.figure(constrained_layout=False, figsize=(15, 7))
    # gs = fig.add_gridspec(1, len(config['lem']['types_clearing_ex_ante']) + 2)
    # for i in range(len(config['lem']['types_clearing_ex_ante'])):
    #     if i == 0:
    #         axs02 = fig.add_subplot(gs[0, 2 + i])
    #         axs02.set_ylabel('Weighted average prices per quality [€/kWh]')
    #     else:
    #         axs02 = fig.add_subplot(gs[0, 2 + i], sharex=axs02, sharey=axs02)
    #     axs02.set_xticks([])
    #     axs02.set_xticklabels([])
    #     type_clearing = config['lem']['types_clearing_ex_ante'][i]
    #     axs02.set_title(type_clearing)
    #     for j in range(len(config['lem']['types_quality'])):
    #         type_quality = config['lem']['types_quality'][j]
    #         axs02.bar(x=j,
    #                   height=results_dict[type_clearing]['df_mean']['prices_quality_' + type_quality] / 1e9 * 1000,
    #                   yerr=results_dict[type_clearing]['df_std']['prices_quality_' + type_quality] / 1e9 * 1000 * 2,
    #                   color=dict_plot[type_quality]['color'], label=type_quality)
    #     axs02.grid(axis='y')
    #
    # axs00 = fig.add_subplot(gs[0, :-len(config['lem']['types_clearing_ex_ante'])], sharey=axs02)
    # axs00.set_xticks(list(range(len(config['lem']['types_clearing_ex_ante']))))
    # axs00.set_xticklabels(config['lem']['types_clearing_ex_ante'].values())
    # axs00.set_ylabel('Weighted average price [€/kWh]')
    # for j in range(len(config['lem']['types_clearing_ex_ante'])):
    #     type_clearing = config['lem']['types_clearing_ex_ante'][j]
    #     axs00.bar(x=j,
    #               height=results_dict[type_clearing]['df_mean']['price_wavg'] / 1e9 * 1000,
    #               yerr=results_dict[type_clearing]['df_std']['price_wavg'] / 1e9 * 1000 * 2,
    #               color=dict_plot[type_clearing]['color'])
    #
    # axs00.grid(axis='y')
    #
    # fig.subplots_adjust(wspace=0.7, top=0.95, bottom=0.15, left=0.05, right=.98)
    # handles, labels = axs02.get_legend_handles_labels()
    # fig.legend(handles, labels, bbox_to_anchor=(0.5, .03), loc='lower center', frameon=False, ncol=5)
    #
    # plt.xticks(rotation=90)
    # tikzplotlib.save(f'{path_figure}/mc_results_prices_wavg_95.tex')
    # plt.savefig(f'{path_figure}/mc_results_prices_wavg_95.png')
    # plt.show()

    # fig = plt.figure(constrained_layout=False, figsize=(10, 5))
    # gs = fig.add_gridspec(1, len(config['lem']['types_clearing_ex_ante']))
    # for i in range(len(config['lem']['types_clearing_ex_ante'])):
    #     if i == 0:
    #         axs02 = fig.add_subplot(gs[0, i])
    #         axs02.set_yticks(list(config['lem']['types_quality'].keys()))
    #         axs02.set_yticklabels(config['lem']['types_quality'].values())
    #     else:
    #         axs02 = fig.add_subplot(gs[0, i], sharex=axs02)
    #         axs02.set_yticks(list(config['lem']['types_quality'].keys()))
    #         axs02.set_yticklabels([])
    #     if i == 3:
    #         axs02.set_xlabel('Weighted average prices per quality [€/kWh]')
    #     type_clearing = config['lem']['types_clearing_ex_ante'][i]
    #     axs02.set_title(type_clearing)
    #     axs02.spines["right"].set_visible(False)
    #     axs02.spines["top"].set_visible(False)
    #     for j in range(len(config['lem']['types_quality'])):
    #         type_quality = config['lem']['types_quality'][j]
    #         axs02.errorbar(x=results_dict[type_clearing]['df_mean']['prices_quality_' + type_quality] / 1e9 * 1000,
    #                        y=j,
    #                        xerr=results_dict[type_clearing]['df_std'][
    #                                 'prices_quality_' + type_quality] / 1e9 * 1000 * 2,
    #                        marker='x',
    #                        markersize=10,
    #                        markeredgewidth=2,
    #                        ecolor='gray',
    #                        capsize=4,
    #                        capthick=0.5,
    #                        color=dict_plot[type_quality]['color'], label=type_quality)
    #     axs02.grid(axis='y')
    #
    # fig.subplots_adjust(wspace=0.2, top=0.95, bottom=0.15, left=0.1, right=.98)
    # # handles, labels = axs02.get_legend_handles_labels()
    # # fig.legend(handles, labels, bbox_to_anchor=(0.5, .03), loc='lower center', frameon=False, ncol=5)
    #
    # tikzplotlib.save(f'{path_figure}/mc_results_prices_wavg_95.tex')
    # plt.savefig(f'{path_figure}/mc_results_prices_wavg_95.png')
    # plt.show()
    #
    # fig = plt.figure(constrained_layout=False, figsize=(10, 4))
    # gs = fig.add_gridspec(1, 1)
    # for i in range(len(config['lem']['types_clearing_ex_ante'])):
    #     axs02 = fig.add_subplot(gs[0, 0])
    #     axs02.set_yticks(list(config['lem']['types_quality'].keys()))
    #     axs02.set_yticklabels(config['lem']['types_quality'].values())
    #     axs02.set_xlabel('Weighted average prices per quality [€/kWh]')
    #     type_clearing = config['lem']['types_clearing_ex_ante'][i]
    #     axs02.spines["right"].set_visible(False)
    #     axs02.spines["top"].set_visible(False)
    #     for j in range(len(config['lem']['types_quality'])):
    #         type_quality = config['lem']['types_quality'][j]
    #         if j == 0:
    #             axs02.errorbar(x=results_dict[type_clearing]['df_mean'][
    #             'prices_quality_' + type_quality] / 1e9 * 1000,
    #                            y=j,
    #                            xerr=results_dict[type_clearing]['df_std'][
    #                                     'prices_quality_' + type_quality] / 1e9 * 1000 * 2,
    #                            marker='x',
    #                            markersize=10,
    #                            markeredgewidth=2,
    #                            ecolor='gray',
    #                            capsize=4,
    #                            capthick=0.5,
    #                            color=dict_plot[type_clearing]['color'], label=type_clearing)
    #         else:
    #             axs02.errorbar(x=results_dict[type_clearing]['df_mean'][
    #             'prices_quality_' + type_quality] / 1e9 * 1000,
    #                            y=j,
    #                            xerr=results_dict[type_clearing]['df_std'][
    #                                     'prices_quality_' + type_quality] / 1e9 * 1000 * 2,
    #                            marker='x',
    #                            markersize=10,
    #                            markeredgewidth=2,
    #                            ecolor='gray',
    #                            capsize=4,
    #                            capthick=0.5,
    #                            color=dict_plot[type_clearing]['color'])
    #     axs02.grid(axis='y')
    #
    # fig.subplots_adjust(wspace=0.2, top=0.95, bottom=0.25, left=0.1, right=.98)
    # handles, labels = axs02.get_legend_handles_labels()
    # fig.legend(handles, labels, bbox_to_anchor=(0.5, .03), loc='lower center', frameon=False, ncol=7)
    #
    # tikzplotlib.save(f'{path_figure}/mc_results_prices_wavg_95.tex')
    # plt.savefig(f'{path_figure}/mc_results_prices_wavg_95.png')
    # plt.show()

    fig, axs = plt.subplots(constrained_layout=False, figsize=(10, 5))
    for i in range(len(config['lem']['types_clearing_ex_ante'])):
        plt.yticks(ticks=list(config['lem']['types_clearing_ex_ante'].keys()),
                   labels=config['lem']['types_clearing_ex_ante'].values())
        plt.xlabel('Weighted average price [€/kWh]')
        type_clearing = config['lem']['types_clearing_ex_ante'][i]
        axs.spines["right"].set_visible(False)
        axs.spines["top"].set_visible(False)
        for j in range(len(config['lem']['types_quality'])):
            type_quality = config['lem']['types_quality'][j]
            if i == 0:
                plt.errorbar(x=results_dict[type_clearing]['df_mean']['prices_quality_' + type_quality] / 1e9 * 1000,
                             y=i,
                             xerr=results_dict[type_clearing]['df_std'][
                                      'prices_quality_' + type_quality] / 1e9 * 1000 * 2,
                             marker='x',
                             markersize=10,
                             markeredgewidth=2,
                             ecolor='gray',
                             elinewidth=2,
                             color=dict_plot[type_quality]['color'], label=type_quality)
            else:
                plt.errorbar(x=results_dict[type_clearing]['df_mean']['prices_quality_' + type_quality] / 1e9 * 1000,
                             y=i,
                             xerr=results_dict[type_clearing]['df_std'][
                                      'prices_quality_' + type_quality] / 1e9 * 1000 * 2,
                             marker='x',
                             markersize=10,
                             markeredgewidth=2,
                             ecolor='gray',
                             elinewidth=2,
                             color=dict_plot[type_quality]['color'])
        plt.grid(axis='y')

    fig.subplots_adjust(wspace=0.2, top=0.95, bottom=0.22, left=0.1, right=.98)
    handles, labels = axs.get_legend_handles_labels()
    fig.legend(handles, labels, bbox_to_anchor=(0.5, .03), loc='lower center', frameon=False, ncol=7)

    # tikzplotlib.save(f'{path_figure}/mc_results_prices_wavg_95.tex')
    plt.savefig(f'{path_figure}/mc_results_prices_wavg_95.png')
    plt.show()


def plot_mc_results_prices_welfare_qty_traded_95(config,
                                                 results_dict,
                                                 path_figure,
                                                 dict_plot=None):
    fig = plt.figure(constrained_layout=False, figsize=(10, 5))
    gs = fig.add_gridspec(1, 3)
    axes_dict = {}
    for i in range(3):
        if i == 0:
            axes_dict[i] = fig.add_subplot(gs[0, 0])
            axes_dict[i].set_yticks(list(config['lem']['types_clearing_ex_ante'].keys()))
            axes_dict[i].set_yticklabels(config['lem']['types_clearing_ex_ante'].values())
        else:
            axes_dict[i] = fig.add_subplot(gs[0, i])
            axes_dict[i].set_yticks(list(config['lem']['types_clearing_ex_ante'].keys()))
            axes_dict[i].set_yticklabels({})
        axes_dict[i].spines["right"].set_visible(False)
        axes_dict[i].spines["top"].set_visible(False)
        axes_dict[i].grid(axis='y')
    axes_dict[0].set_xlabel('Cleared energy [kWh]')
    axes_dict[1].set_xlabel('Weighted average price [€/kWh]')
    axes_dict[2].set_xlabel('Welfare [€]')
    for i in range(len(config['lem']['types_clearing_ex_ante'])):
        type_clearing = config['lem']['types_clearing_ex_ante'][i]
        axes_dict[0].errorbar(x=results_dict[type_clearing]['df_mean']['qty_traded'] / 1000,
                              y=i,
                              xerr=results_dict[type_clearing]['df_std']['qty_traded'] / 1000 * 1.96,
                              marker='x',
                              markersize=10,
                              markeredgewidth=2,
                              ecolor='gray',
                              elinewidth=2,
                              color=dict_plot[type_clearing]['color'], label=type_clearing)
        axes_dict[1].errorbar(x=results_dict[type_clearing]['df_mean']['price_wavg'] / 1e9 * 1000,
                              y=i,
                              xerr=results_dict[type_clearing]['df_std']['price_wavg'] / 1e9 * 1000 * 1.96,
                              marker='x',
                              markersize=10,
                              markeredgewidth=2,
                              ecolor='gray',
                              elinewidth=2,
                              color=dict_plot[type_clearing]['color'], label=type_clearing)
        axes_dict[2].errorbar(x=results_dict[type_clearing]['df_mean']['welfare'] / 1e9,
                              y=i,
                              xerr=results_dict[type_clearing]['df_std']['welfare'] / 1e9 * 1.96,
                              marker='x',
                              markersize=10,
                              markeredgewidth=2,
                              ecolor='gray',
                              elinewidth=2,
                              color=dict_plot[type_clearing]['color'], label=type_clearing)

    fig.subplots_adjust(wspace=0.2, top=0.95, bottom=0.15, left=0.1, right=.98)
    # tikzplotlib.save(f'{path_figure}/mc_results_qty_traded_price_welfare_95.tex')
    plt.savefig(f'{path_figure}/mc_results_qty_traded_price_welfare_95.png')
    plt.show()


def plot_all_clearing_results(config, results_dict, dict_plot=None):
    n_results = len(results_dict[list(results_dict.keys())[0]]['df_mean'].columns)
    for i in range(n_results):
        for j in config['lem']['types_clearing_ex_ante'].keys():
            type_clearing = config['lem']['types_clearing_ex_ante'][j]
            column_name = results_dict[type_clearing]['df_mean'].columns[i]
            plt.bar(j,
                    results_dict[type_clearing]['df_mean'][column_name],
                    yerr=results_dict[type_clearing]['df_std'][column_name] * 1.96,
                    label=type_clearing, color=dict_plot[type_clearing]['color'])
        plt.title(column_name)
        plt.legend()
        plt.show()


if __name__ == '__main__':
    # Run simulations ####################################
    # load configuration file
    with open(f"monte_carlo_config.yaml") as config_file:
        config_mc = YAML().load(config_file)
    # Create simulation directory
    t_current = pd.Timestamp.now().strftime('%Y-%m-%d_%H-%M-%S')
    path_sim, path_input, path_output, path_results, path_figures = create_simulation_folder(
        simulation_path=f'simulations/{t_current}', config=config_mc)

    # Save configuration to simulation folder
    shutil.copyfile(src=f"monte_carlo_config.yaml", dst=f"{path_sim}/mc_config.yaml")

    # Calculate lem simulations ########################################
    # Calculate n_trials x n_iterations * types_clearing lem simulations
    random.seed(0)
    print(f'Task: Calculate {config_mc["monte_carlo"]["n_trials"]} x '
          f'{config_mc["monte_carlo"]["n_iterations"]} random lem simulations')
    # Prepare parallelization
    num_cores = mp.cpu_count()
    # Run lem iterations for mc simulation
    with mp.Pool(processes=num_cores,
                 initializer=_init_db_obj_workers,
                 initargs=(single_lem_simulation,
                           config_mc["db_connections"]["database_connection_user"],
                           config_mc,
                           path_input,
                           path_output
                           )) as pool:
        pool.map(single_lem_simulation, list(range(0, config_mc['monte_carlo']['n_iterations'])))
    pool.close()

    # # Load pre-existing results and configurations ##########
    # path_sim = 'simulations/2022-04-29_10-39-30/'
    # # load configuration file
    # with open(f"{path_sim}/mc_config.yaml") as config_file:
    #     config_mc = yaml.load(config_file, Loader=yaml.FullLoader)
    # path_input = path_sim + 'input'
    # path_output = path_sim + 'output'
    # path_results = path_sim + 'results'
    # path_figures = path_sim + 'figures'

    # Evaluate placed and cleared market positions #############################
    print(f"Task: Evaluate placed and cleared market positions")
    result_dict = {}
    for clearing in config_mc['lem']['types_clearing_ex_ante'].values():
        df_trials = pd.DataFrame()
        for trial in range(config_mc['monte_carlo']['n_trials']):
            df_iterations = pd.DataFrame()
            result_file_names = os.listdir(f'{path_output}/{clearing}/{trial}')
            for result_name in result_file_names:
                positions_matched = pd.read_feather(path=f'{path_output}/{clearing}/{trial}/{result_name}')
                positions_placed = pd.read_feather(path=f'{path_input}/{trial}/{result_name}')
                # Extracts all relevant data fields from cleared positions
                df_iterations = pd.concat([df_iterations,
                                           extract_relevant_data(config_mc, positions_matched, positions_placed)],
                                          ignore_index=True)
            # Calculates mean of all iterations
            df_trials = pd.concat([df_trials, df_iterations.mean().to_frame().T])
        # calculates mean and std of all trials
        df_mean = df_trials.mean().to_frame().T
        df_std = df_trials.std().to_frame().T
        result_dict[clearing] = {'df_mean': df_mean, 'df_std': df_std}

    with open(f'{path_results}/result_dict.p', 'wb') as fp:
        pickle.dump(result_dict, fp, protocol=pickle.HIGHEST_PROTOCOL)

    # Plot MC results ##########################################
    print(f"Task: Plot monte carlo simulation results")
    with open(f'{path_results}/result_dict.p', 'rb') as fp:
        result_dict = pickle.load(fp)

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
    dict_plot_clearing_types['local_green'] = {'color': dark2.colors[0]}

    # plot_all_clearing_results(config_mc, results_dict, dict_plot=dict_plot_clearing_types)

    plot_mc_results_offers_cleared_95(config=config_mc,
                                      results_dict=result_dict,
                                      path_figure=path_figures,
                                      dict_plot=dict_plot_clearing_types)
    plot_mc_results_bids_cleared_95(config=config_mc,
                                    results_dict=result_dict,
                                    path_figure=path_figures,
                                    dict_plot=dict_plot_clearing_types)
    plot_mc_results_prices_wavg_95(config=config_mc,
                                   results_dict=result_dict,
                                   path_figure=path_figures,
                                   dict_plot=dict_plot_clearing_types)
    plot_mc_results_prices_welfare_qty_traded_95(config=config_mc,
                                                 results_dict=result_dict,
                                                 path_figure=path_figures,
                                                 dict_plot=dict_plot_clearing_types)
    plot_mc_results_placed_positions_95(config=config_mc,
                                        results_dict=result_dict,
                                        path_figure=path_figures,
                                        dict_plot=dict_plot_clearing_types)
