from Database_Connector import db_connection, db_param
from matplotlib import pyplot as plt
import numpy as np
import pandas as pd


def group_and_apply_fcts(results):
    grouped_results = results.groupby(db_param.ts_delivery).agg({db_param.qty_energy_traded: ['sum',
                                                                                              'max',
                                                                                              'min'],
                                                                 db_param.price_energy_cleared_uniform: ['mean',
                                                                                                         'max',
                                                                                                         'min',
                                                                                                         'std'],
                                                                 db_param.t_cleared: ['mean', 'max', 'min']})
    grouped_results[db_param.price_energy_cleared_uniform, 'wmean'] = results.groupby([db_param.ts_delivery]). \
        apply(lambda x: np.average(x[db_param.price_energy_cleared_uniform], weights=x[db_param.qty_energy_traded]))

    return grouped_results


# Create a db connection object
db_conn = db_connection.DatabaseConnection(db_dict={'user': 'postgres',
                                                    'pw': 'EWK!BC?Authority',
                                                    'host': '129.187.90.187',
                                                    'port': '5432',
                                                    'db': 'MZ_Test'
                                                    }
                                           )
# Get bids and offers
bids, offers = db_conn.get_bids_offers_market()
# Get market results
matched_results_std, _ = db_conn.get_results_market(table_name=db_param.name_table_results_market_clearing_standard)
matched_results_pref_prio, _ = db_conn.get_results_market(
    table_name=db_param.name_table_results_market_clearing_pref_prioritization)
matched_results_pref_satisf, _ = db_conn.get_results_market(
    table_name=db_param.name_table_results_market_clearing_pref_satisfaction)

# Extract unique time of deliveries
ts_delivery_std_unique = matched_results_std[db_param.ts_delivery].unique()
ts_delivery_pref_prio_unique = matched_results_pref_prio[db_param.ts_delivery].unique()
ts_delivery_pref_satisf_unique = matched_results_pref_satisf[db_param.ts_delivery].unique()

# unique time of deliveries in all results
ts_delivery_unique = list(set.intersection(*map(set, [ts_delivery_std_unique,
                                                      ts_delivery_pref_prio_unique,
                                                      ts_delivery_pref_satisf_unique])))

# remove any matched positions that do not appear in all three tables
matched_results_std = matched_results_std[matched_results_std[db_param.ts_delivery].isin(ts_delivery_unique)]
matched_results_pref_prio = matched_results_pref_prio[
    matched_results_pref_prio[db_param.ts_delivery].isin(ts_delivery_unique)]
matched_results_pref_satisf = matched_results_pref_satisf[
    matched_results_pref_satisf[db_param.ts_delivery].isin(ts_delivery_unique)]

# Group by ts_delivery and apply various basic analysis functions
matched_results_std_grouped = group_and_apply_fcts(matched_results_std)
matched_results_pref_prio_grouped = group_and_apply_fcts(matched_results_pref_prio)
matched_results_pref_satisf_grouped = group_and_apply_fcts(matched_results_pref_satisf)

# change unix timestamps to pandas timestamps
matched_results_std_grouped.index = pd.to_datetime(matched_results_std_grouped.index, unit='s'). \
    astype('datetime64[ns, Europe/Paris]').tz_convert('UTC')
matched_results_pref_prio_grouped.index = pd.to_datetime(matched_results_pref_prio_grouped.index, unit='s'). \
    astype('datetime64[ns, Europe/Paris]').tz_convert('UTC')
matched_results_pref_satisf_grouped.index = pd.to_datetime(matched_results_pref_satisf_grouped.index, unit='s'). \
    astype('datetime64[ns, Europe/Paris]').tz_convert('UTC')

cm = plt.get_cmap('Dark2')
plot_dict = {'std': {'color': cm.colors[0], 'linestyle': '-', 'marker': 'x'},
             'pref_prio': {'color': cm.colors[2], 'linestyle': '--', 'marker': 'o'},
             'pref_satisf': {'color': cm.colors[7], 'linestyle': ':', 'marker': '+'}}

result_dict = {'std': matched_results_std_grouped,
               'pref_prio': matched_results_pref_prio_grouped,
               'pref_satisf': matched_results_pref_satisf_grouped}

# Plot weighted mean
for result in result_dict:
    plt.plot(result_dict[result].index, result_dict[result][db_param.price_energy_cleared_uniform]['wmean'],
             label=result, color=plot_dict[result]['color'], linestyle=plot_dict[result]['linestyle'])
plt.legend()
plt.show()


# Plot min/max/mean price
for result in result_dict:
    plt.fill_between(result_dict[result].index,
                     result_dict[result][db_param.price_energy_cleared_uniform]['min'],
                     result_dict[result][db_param.price_energy_cleared_uniform]['max'],
                     alpha=0.3, color=plot_dict[result]['color'])
    plt.plot(result_dict[result].index, result_dict[result][db_param.price_energy_cleared_uniform]['wmean'],
             label=result, color=plot_dict[result]['color'], linestyle=plot_dict[result]['linestyle'])
plt.legend()
plt.show()

# Plot quantities of three clearings
for result in result_dict:
    plt.scatter(result_dict[result].index[1:], result_dict[result][db_param.qty_energy_traded]['sum'][1:],
                label=result, color=plot_dict[result]['color'], marker=plot_dict[result]['marker'])
plt.legend()
plt.show()