from lem import clearing_standard, clearing_pref_prioritization, clearing_pref_satisfaction
from Database_Connector import db_param
from tqdm import tqdm
import pandas as pd
import random


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
        print(f'Test case #{n_test_case}')
    # Example test configuration
    results_dict[n_test_case] = {}
    results_dict[n_test_case]['Std'], _, _, _, _ = clearing_standard(offers,
                                                                     bids,
                                                                     pricing_uniform=pricing_uniform,
                                                                     pricing_discriminative=pricing_discriminative,
                                                                     plotting=plotting,
                                                                     plotting_title=f'Test #{n_test_case}')
    results_dict[n_test_case]['Prio'], _, _, _, _ = clearing_pref_prioritization(offers,
                                                                                 bids,
                                                                                 pricing_uniform=pricing_uniform,
                                                                                 pricing_discriminative=pricing_discriminative,
                                                                                 plotting=plotting,
                                                                                 plotting_title=f'Test #{n_test_case}')
    results_dict[n_test_case]['Satis'] = clearing_pref_satisfaction(offers,
                                                                    bids,
                                                                    pricing_uniform=pricing_uniform,
                                                                    pricing_discriminative=pricing_discriminative,
                                                                    plotting=plotting,
                                                                    plotting_title=f'Test #{n_test_case}')

    return results_dict


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
offers_std = pd.DataFrame(data={db_param.price_energy: prices_energy_offers,
                                db_param.qty_energy: qty_energy_offers,
                                db_param.id_user: ids_user_offers,
                                db_param.quality_energy: qualities_energy_offers,
                                db_param.ts_delivery: ts_delivery_offers})
bids_std = pd.DataFrame(data={db_param.price_energy: prices_energy_bids,
                              db_param.qty_energy: qty_energy_bids,
                              db_param.id_user: ids_user_bids,
                              db_param.quality_energy: qualities_energy_bids,
                              db_param.ts_delivery: ts_delivery_bids})
# Results dictionary
results = {}
# Test case
test_cases = range(1, 19)
# test_cases = [18]

for test_case in test_cases:
    offers_test = offers_std
    bids_test = bids_std
    if test_case == 1:
        results = run_clearings(offers_test, bids_test, results,
                                pricing_uniform=True, pricing_discriminative=True,
                                plotting=True, n_test_case=test_case, verbose=True)
    if test_case == 2:
        bids_test = bids_std.drop(index=bids_std.tail(3).index)
        results = run_clearings(offers_test, bids_test, results,
                                pricing_uniform=True, pricing_discriminative=True,
                                plotting=True, n_test_case=test_case, verbose=True)

    if test_case == 3:
        bids_test = pd.DataFrame()
        results = run_clearings(offers_test, bids_test, results,
                                pricing_uniform=True, pricing_discriminative=True,
                                plotting=True, n_test_case=test_case, verbose=True)

    if test_case == 4:
        offers_test = pd.DataFrame()
        results = run_clearings(offers_test, bids_test, results,
                                pricing_uniform=True, pricing_discriminative=True,
                                plotting=True, n_test_case=test_case, verbose=True)

    if test_case == 5:
        offers_test = pd.DataFrame()
        bids_test = pd.DataFrame()
        results = run_clearings(offers_test, bids_test, results,
                                pricing_uniform=True, pricing_discriminative=True,
                                plotting=True, n_test_case=test_case, verbose=True)

    if test_case == 6:
        offers_test = offers_std.assign(**{db_param.qty_energy: [qty * 10 for qty in qty_energy_offers]})
        results = run_clearings(offers_test, bids_test, results,
                                pricing_uniform=True, pricing_discriminative=True,
                                plotting=True, n_test_case=test_case, verbose=True)

    if test_case == 7:
        offers_test = offers_std.assign(**{db_param.price_energy: [price * 10 for price in prices_energy_offers]})
        results = run_clearings(offers_test, bids_test, results,
                                pricing_uniform=True, pricing_discriminative=True,
                                plotting=True, n_test_case=test_case, verbose=True)

    if test_case == 8:
        bids_test = bids_std.assign(**{db_param.price_energy: [price * 10 for price in prices_energy_bids]})
        results = run_clearings(offers_test, bids_test, results,
                                pricing_uniform=True, pricing_discriminative=True,
                                plotting=True, n_test_case=test_case, verbose=True)

    if test_case == 9:
        bids_test = bids_std.assign(**{db_param.price_energy: [price * (-1) for price in prices_energy_bids]})
        results = run_clearings(offers_test, bids_test, results,
                                pricing_uniform=True, pricing_discriminative=True,
                                plotting=True, n_test_case=test_case, verbose=True)

    if test_case == 10:
        offers_test = offers_std.assign(**{db_param.price_energy: [price * (-1) for price in prices_energy_offers]})
        bids_test = bids_std.assign(**{db_param.price_energy: [price * (-1) for price in prices_energy_bids]})
        results = run_clearings(offers_test, bids_test, results,
                                pricing_uniform=True, pricing_discriminative=True,
                                plotting=True, n_test_case=test_case, verbose=True)

    if test_case == 11:
        offers_test = offers_std.drop(index=offers_test.tail(3).index)
        results = run_clearings(offers_test, bids_test, results,
                                pricing_uniform=True, pricing_discriminative=True,
                                plotting=True, n_test_case=test_case, verbose=True)

    if test_case == 12:
        offers_test = offers_std.assign(**{db_param.price_energy: [3] * len(qty_energy_offers)})
        bids_test = bids_std.assign(**{db_param.price_energy: [3] * len(qty_energy_bids)})
        bids_test = bids_test.drop(index=bids_test.tail(3).index)
        results = run_clearings(offers_test, bids_test, results,
                                pricing_uniform=True, pricing_discriminative=True,
                                plotting=True, n_test_case=test_case, verbose=True)

    if test_case == 13:
        offers_test = offers_std.assign(**{db_param.price_energy: [3] * len(qty_energy_offers)})
        bids_test = bids_std.assign(**{db_param.price_energy: [30] * len(qty_energy_bids)})
        bids_test = bids_test.drop(index=bids_test.tail(3).index)
        results = run_clearings(offers_test, bids_test, results,
                                pricing_uniform=True, pricing_discriminative=True,
                                plotting=True, n_test_case=test_case, verbose=True)

    if test_case == 14:
        offers_test = offers_std.assign(**{db_param.price_energy: [30] * len(qty_energy_offers)})
        bids_test = bids_std.assign(**{db_param.price_energy: [3] * len(qty_energy_bids)})
        bids_test = bids_test.drop(index=bids_test.tail(3).index)
        results = run_clearings(offers_test, bids_test, results,
                                pricing_uniform=True, pricing_discriminative=True,
                                plotting=True, n_test_case=test_case, verbose=True)

    if test_case == 15:
        bids_test = bids_std.assign(**{db_param.qty_energy: [0] * len(prices_energy_bids)})
        bids_test = bids_test.drop(index=bids_test.tail(3).index)
        results = run_clearings(offers_test, bids_test, results,
                                pricing_uniform=True, pricing_discriminative=True,
                                plotting=True, n_test_case=test_case, verbose=True)

    if test_case == 16:
        offers_test = offers_std.assign(**{db_param.quality_energy: [0] * len(prices_energy_offers)})
        results = run_clearings(offers_test, bids_test, results,
                                pricing_uniform=True, pricing_discriminative=True,
                                plotting=True, n_test_case=test_case, verbose=True)

    if test_case == 17:
        bids_test = bids_std.assign(**{db_param.quality_energy: [2] * len(prices_energy_bids)})
        results = run_clearings(offers_test, bids_test, results,
                                pricing_uniform=True, pricing_discriminative=True,
                                plotting=True, n_test_case=test_case, verbose=True)

    if test_case == 18:
        # Random test value generator
        for i in tqdm(range(10)):
            number = 100
            prices_energy_offers = random.choices(range(1, 10), k=number)
            qty_energy_offers = random.choices(range(1, 10), k=number)
            prices_energy_bids = random.choices(range(3, 15), k=number)
            qty_energy_bids = random.choices(range(1, 10), k=number)
            ts_delivery = [0] * number

            offers_test = pd.DataFrame(data={db_param.price_energy: prices_energy_offers,
                                             db_param.qty_energy: qty_energy_offers,
                                             db_param.id_user: random.choices(ids_user_offers, k=number),
                                             db_param.quality_energy: random.choices(qualities_energy_offers, k=number),
                                             db_param.ts_delivery: ts_delivery})

            bids_test = pd.DataFrame(data={db_param.price_energy: prices_energy_bids,
                                           db_param.qty_energy: qty_energy_bids,
                                           db_param.id_user: random.choices(ids_user_bids, k=number),
                                           db_param.quality_energy: random.choices(qualities_energy_bids, k=number),
                                           db_param.ts_delivery: ts_delivery})

            results = run_clearings(offers_test, bids_test, results,
                                    pricing_uniform=True, pricing_discriminative=True,
                                    plotting=True, n_test_case=test_case+i, verbose=True)