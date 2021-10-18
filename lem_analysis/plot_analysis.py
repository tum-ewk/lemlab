from lemlab.platform.lem import clearing_standard, clearing_pref_prioritization, clearing_pref_satisfaction
from lemlab.db_connection import db_param
import pandas as pd
import numpy as np


def run_clearings(offers,
                  bids,
                  results_dict,
                  pricing_uniform=True,
                  pricing_discriminative=True,
                  plotting=True,
                  n_test_case=0,
                  verbose=False
                  ):
    if verbose:
        print(f'\nTest case #{n_test_case}')
    # Example test configuration
    results_dict[n_test_case] = {}
    results_dict[n_test_case]['Std'], _, _, _, _ = clearing_standard(offers,
                                                                     bids,
                                                                     pricing_uniform=pricing_uniform,
                                                                     pricing_discriminative=pricing_discriminative,
                                                                     plotting=plotting,
                                                                     plotting_title=f'Test #{n_test_case}',
                                                                     ylim=[0, max(offers[db_param.PRICE_ENERGY].max(),
                                                                                  bids[
                                                                                      db_param.PRICE_ENERGY].max()) * 1.1])
    kpis = pd.DataFrame(analyze_results(results_dict[n_test_case]['Std']), index=['DA'])
    results_dict[n_test_case]['Prio'], _, _, _, _ = clearing_pref_prioritization(offers,
                                                                                 bids,
                                                                                 pricing_uniform=pricing_uniform,
                                                                                 pricing_discriminative=pricing_discriminative,
                                                                                 plotting=plotting,
                                                                                 plotting_title=f'Test #{n_test_case}')
    kpis = kpis.append(pd.DataFrame(analyze_results(results_dict[n_test_case]['Prio']), index=['Prio']))
    results_dict[n_test_case]['Satis'] = clearing_pref_satisfaction(offers,
                                                                    bids,
                                                                    pricing_uniform=pricing_uniform,
                                                                    pricing_discriminative=pricing_discriminative,
                                                                    plotting=plotting,
                                                                    plotting_title=f'Test #{n_test_case}')
    kpis = kpis.append(pd.DataFrame(analyze_results(results_dict[n_test_case]['Satis']), index=['Satis']))

    return results_dict, kpis


def analyze_results(results_df):
    kpis_results = dict()
    kpis_results['qty_energy_cleared'] = results_df[db_param.QTY_ENERGY_TRADED].sum()
    kpis_results['wavg_price_energy'] = np.average(results_df[db_param.PRICE_ENERGY_CLEARED_UNIFORM],
                                                   weights=results_df[db_param.QTY_ENERGY_TRADED])
    kpis_results['welfare'] = ((results_df[db_param.PRICE_ENERGY_BID] -
                               results_df[db_param.PRICE_ENERGY_CLEARED_UNIFORM]) *
                               results_df[db_param.QTY_ENERGY_TRADED]).sum() + \
                              ((results_df[db_param.PRICE_ENERGY_CLEARED_UNIFORM] -
                               results_df[db_param.PRICE_ENERGY_OFFER]) *
                               results_df[db_param.QTY_ENERGY_TRADED]).sum()
    kpis_results['quality_0'] = results_df[results_df[db_param.QUALITY_ENERGY_OFFER] == 0][db_param.QTY_ENERGY_TRADED].sum()
    kpis_results['quality_1'] = results_df[results_df[db_param.QUALITY_ENERGY_OFFER] == 1][db_param.QTY_ENERGY_TRADED].sum()
    kpis_results['quality_2'] = results_df[results_df[db_param.QUALITY_ENERGY_OFFER] == 2][db_param.QTY_ENERGY_TRADED].sum()

    return kpis_results


# Standard input data
prices_energy_offers = [1, 2, 2, 2, 2, 3, 4, 6]
qty_energy_offers = [2, 2, 4, 2, 2, 2, 3, 5]
qualities_energy_offers = [0, 0, 2, 0, 1, 0, 2, 0]
ids_user_offers = ['a', 'b', 'c', 'd', 'e', 'q', 'w', 'y']
ts_delivery_offers = [0] * len(prices_energy_offers)
prices_energy_bids = [6, 5, 4, 3, 2, 1]
qty_energy_bids = [3, 4, 3, 5, 2, 3]
qualities_energy_bids = [0, 2, 1, 0, 0, 0]
ids_user_bids = ['r', 't', 'z', 'u', 'i', 'x']
ts_delivery_bids = [0] * len(prices_energy_bids)
# Offers and demands
offers_std = pd.DataFrame(data={db_param.PRICE_ENERGY: prices_energy_offers,
                                db_param.QTY_ENERGY: qty_energy_offers,
                                db_param.ID_USER: ids_user_offers,
                                db_param.QUALITY_ENERGY: qualities_energy_offers,
                                db_param.TS_DELIVERY: ts_delivery_offers})
bids_std = pd.DataFrame(data={db_param.PRICE_ENERGY: prices_energy_bids,
                              db_param.QTY_ENERGY: qty_energy_bids,
                              db_param.ID_USER: ids_user_bids,
                              db_param.QUALITY_ENERGY: qualities_energy_bids,
                              db_param.TS_DELIVERY: ts_delivery_bids})
# Results dictionary
results = {}
# Test case
# test_cases = range(1, 19)
test_cases = [1]
kpis = pd.DataFrame()
for test_case in test_cases:
    offers_test = offers_std
    bids_test = bids_std
    if test_case == 1:
        results, kpis = run_clearings(offers_test, bids_test, results,
                                      pricing_uniform=True, pricing_discriminative=True,
                                      plotting=True, n_test_case=test_case, verbose=True)

kpis = kpis.transpose()

print(kpis)
