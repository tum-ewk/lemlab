import numpy as np
from matplotlib import pyplot as plt
import yaml
import pandas as pd
import os
import pickle
from monte_carlo_lem_analysis_mit import extract_relevant_data

if __name__ == '__main__':
    # Run simulations ####################################
    # load configuration file
    with open(f"monte_carlo_config.yaml") as config_file:
        config_mc = yaml.load(config_file, Loader=yaml.FullLoader)
    # Create simulation directory
    t_current = pd.Timestamp.now().strftime('%Y-%m-%d_%H-%M-%S')

    # Load pre-existing results and configurations ##########
    path_sim = 'simulations/2021-05-23_16-35-23/'
    # load configuration file
    with open(f"{path_sim}/mc_config.yaml") as config_file:
        config_mc = yaml.load(config_file, Loader=yaml.FullLoader)
    path_input = path_sim + 'input'
    path_output = path_sim + 'output'
    path_results = path_sim + 'results'
    path_figures = path_sim + 'figures'

    # Evaluate placed and cleared market positions #############################
    print(f"Task: Evaluate placed and cleared market positions")
    result_dict = {}
    for clearing in config_mc['lem']['types_clearing_ex_ante'].values():
        df_iterations = pd.DataFrame()
        result_file_names = os.listdir(f'{path_output}/{clearing}/0')
        for result_name in result_file_names:
            positions_matched = pd.read_feather(path=f'{path_output}/{clearing}/0/{result_name}')
            positions_placed = pd.read_feather(path=f'{path_input}/0/{result_name}')
            # Extracts all relevant data fields from cleared positions
            df_iterations = df_iterations.append(extract_relevant_data(config_mc, positions_matched, positions_placed),
                                                 ignore_index=True)

        # Accuracy is set to approximately 1 % of the expected mean
        # Confidence interval is set to 95 or 99 % (z-value 1.96 and 2.58)
        n_qty_traded_99 = (df_iterations["qty_traded"].std() * 2.58 / int(
            df_iterations["qty_traded"].mean()/100))**2
        n_qty_traded_95 = (df_iterations["qty_traded"].std() * 1.96 / int(
            df_iterations["qty_traded"].mean() / 100)) ** 2

        n_price_wavg_99 = (df_iterations["price_wavg"].std() * 2.58 / int(
            df_iterations["price_wavg"].mean()/100))**2
        n_price_wavg_95 = (df_iterations["price_wavg"].std() * 1.96 / int(
            df_iterations["price_wavg"].mean() / 100)) ** 2

        n_welfare_99 = (df_iterations["welfare"].std() * 2.58 / int(
            df_iterations["welfare"].mean()/100))**2
        n_welfare_95 = (df_iterations["welfare"].std() * 1.96 / int(
            df_iterations["welfare"].mean() / 100)) ** 2

        n_bids_cld_local_green_99 = (df_iterations["shares_bids_cleared_local_green"].std() * 2.58 / (df_iterations[
            "shares_bids_cleared_local_green"].mean() / 100)) ** 2
        n_bids_cld_local_green_95 = (df_iterations["shares_bids_cleared_local_green"].std() * 1.96 / (df_iterations[
            "shares_bids_cleared_local_green"].mean() / 100)) ** 2

        n_bids_cld_local_99 = (df_iterations["shares_bids_cleared_local"].std() * 2.58 / (df_iterations[
            "shares_bids_cleared_local"].mean() / 100)) ** 2
        n_bids_cld_local_95 = (df_iterations["shares_bids_cleared_local"].std() * 1.96 / (df_iterations[
            "shares_bids_cleared_local"].mean() / 100)) ** 2

        n_bids_cld_na_99 = (df_iterations["shares_bids_cleared_na"].std() * 2.58 / (df_iterations[
            "shares_bids_cleared_na"].mean() / 100)) ** 2
        n_bids_cld_na_95 = (df_iterations["shares_bids_cleared_na"].std() * 1.96 / (df_iterations[
            "shares_bids_cleared_na"].mean() / 100)) ** 2

        n_offers_cld_local_green_99 = (df_iterations["shares_offers_cleared_local_green"].std() * 2.58 / (df_iterations[
            "shares_offers_cleared_local_green"].mean() / 100)) ** 2
        n_offers_cld_local_green_95 = (df_iterations["shares_offers_cleared_local_green"].std() * 1.96 / (df_iterations[
            "shares_offers_cleared_local_green"].mean() / 100)) ** 2

        n_offers_cld_local_99 = (df_iterations["shares_offers_cleared_local"].std() * 2.58 / (df_iterations[
            "shares_offers_cleared_local"].mean() / 100)) ** 2
        n_offers_cld_local_95 = (df_iterations["shares_offers_cleared_local"].std() * 1.96 / (df_iterations[
            "shares_offers_cleared_local"].mean() / 100)) ** 2

        n_offers_cld_na_99 = (df_iterations["shares_offers_cleared_na"].std() * 2.58 / (df_iterations[
            "shares_offers_cleared_na"].mean() / 100)) ** 2
        n_offers_cld_na_95 = (df_iterations["shares_offers_cleared_na"].std() * 1.96 / (df_iterations[
            "shares_offers_cleared_na"].mean() / 100)) ** 2

        n_prices_wavg_quality_local_green_99 = (df_iterations["prices_quality_local_green"].std() * 2.58 / (df_iterations[
            "prices_quality_local_green"].mean() / 100)) ** 2
        n_prices_wavg_quality_local_green_95 = (df_iterations["prices_quality_local_green"].std() * 1.96 / (df_iterations[
            "prices_quality_local_green"].mean() / 100)) ** 2

        n_prices_wavg_quality_local_99 = (df_iterations["prices_quality_local"].std() * 2.58 / (df_iterations[
            "prices_quality_local"].mean() / 100)) ** 2
        n_prices_wavg_quality_local_95 = (df_iterations["prices_quality_local"].std() * 1.96 / (df_iterations[
            "prices_quality_local"].mean() / 100)) ** 2

        n_prices_wavg_quality_na_99 = (df_iterations["prices_quality_na"].std() * 2.58 / (df_iterations[
            "prices_quality_na"].mean() / 100)) ** 2
        n_prices_wavg_quality_na_95 = (df_iterations["prices_quality_na"].std() * 1.96 / (df_iterations[
            "prices_quality_na"].mean() / 100)) ** 2

        result_dict[clearing] = {"n_welfare_95": n_welfare_95, "n_welfare_99": n_welfare_99,
                                 "n_qty_traded_95": n_qty_traded_95, "n_qty_traded_99": n_qty_traded_99,
                                 "n_price_wavg_95": n_price_wavg_95, "n_price_wavg_99": n_price_wavg_99,
                                 "n_bids_cld_local_green_95": n_bids_cld_local_green_95,
                                 "n_bids_cld_local_green_99": n_bids_cld_local_green_99,
                                 "n_bids_cld_local_95": n_bids_cld_local_95,
                                 "n_bids_cld_local_99": n_bids_cld_local_99,
                                 "n_bids_cld_na_95": n_bids_cld_na_95,
                                 "n_bids_cld_na_99": n_bids_cld_na_99,
                                 "n_offers_cld_local_green_95": n_offers_cld_local_green_95,
                                 "n_offers_cld_local_green_99": n_offers_cld_local_green_99,
                                 "n_offers_cld_local_95": n_offers_cld_local_95,
                                 "n_offers_cld_local_99": n_offers_cld_local_99,
                                 "n_offers_cld_na_95": n_offers_cld_na_95,
                                 "n_offers_cld_na_99": n_offers_cld_na_99,
                                 "n_prices_wavg_quality_local_green_95": n_prices_wavg_quality_local_green_95,
                                 "n_prices_wavg_quality_local_green_99": n_prices_wavg_quality_local_green_99,
                                 "n_prices_wavg_quality_local_95": n_prices_wavg_quality_local_95,
                                 "n_prices_wavg_quality_local_99": n_prices_wavg_quality_local_99,
                                 "n_prices_wavg_quality_na_95": n_prices_wavg_quality_na_95,
                                 "n_prices_wavg_quality_na_99": n_prices_wavg_quality_na_99}

    df_results = pd.DataFrame(result_dict)

    n_max = df_results.max().max()
    n_min = df_results.min().min()

    print(f"Maximal number of simulations for a CI of 99 %: {n_max}")
    print(f"Minimum number of simulations for a CI of 99 %: {n_min}")
