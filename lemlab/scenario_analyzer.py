__author__ = "TUM-Doepfert"
__credits__ = ["sdlumpp"]
__license__ = ""
__maintainer__ = "TUM-Doepfert"
__email__ = "markus.doepfert@tum.de"

import os
import math
import time
import pandas as pd
import numpy as np
import itertools as it
import matplotlib.pyplot as plt
import lemlab.db_connection.db_param as db_p
import lemlab.db_connection.db_connection as db_conn
from datetime import datetime
from ruamel.yaml import YAML


class ScenarioAnalyzer:
    """
    A class used to create various plots to analyze scenarios
    ...

    Attributes
    ----------
    path_results : str
        path to the scenario results
    save_figures : bool
        boolean to state if plots are to be saved as png
    path_analyzer : str
        path for the analyzer results to be stored
    yaml : YAML
        allows reading and writing of yaml files without changing their original format
    config : dict
        config file that contains all required settings to set up a new or edit an existing scenario
    db_conn : DatabaseConnection
        establishes a connection to the lemlab database
    max_time : int
        maximum timestep that is to be considered for the analysis
    pv_bat_ev_hp_wind_fix : dataframe
        contains the information about which household has which type of plants
    conv_to_kWh : float
        conversion factor to kWh
    conv_to_kW : float
        conversion factor to kW
    conv_to_EUR : float
        conversion factor to Euros

    Public Methods
    -------
    __init__(path_results: str, save_figures: bool) -> None
        initializer, requires the path to the scenario's results and information about if the plots are to be stored
    run_analysis() -> None
        creates all pre-configured plots
    plot_virtual_feeder_flow() -> None
        plots the virtual flow within the lem over time
    plot_mcp(type_market: str) -> None
        plots the average and individual market clearing prices over time
    plot_balance() -> None
        plots the balance of each participant
    plot_price_quality() -> None
        plots the price of electricity as well as its quality over time
    plot_household(type_household: tuple) -> None
        plots the load profile and the power sales and purchases over time for the chosen type of example household
    plot_average_mcp_per_type() -> None
        plots the specific energy costs for each type of participant
    """

    def __init__(self, path_results, save_figures: bool = False,
                 show_figures: bool = True):
        """initializer

        Args:
            path_results: string that contains the path where the results are in relation to main directory
            save_figures: boolean that specifies if the figures are to be saved as png or not

        Returns:
            None

        """

        self.path_results = path_results
        self.save_figures = save_figures
        self.show_figures = show_figures
        self.path_analyzer = f"{self.path_results}/analyzer"
        self.__create_folder()
        self.yaml = YAML()
        self.config = self.__load_config()
        self.db_conn = db_conn.DatabaseConnection(db_dict=self.config['db_connections']['database_connection_admin'],
                                                  lem_config=self.config['lem'])
        self.max_time = self.__max_timestamp()
        self.pv_bat_ev_hp_wind_fix = self.__check_pv_bat_ev_hp_wind_fix()
        self.conv_to_kWh = 1/1000           # to convert from Wh to kWh
        self.conv_to_kW = 1/250             # to convert from Wh per 15 min to kW
        self.conv_to_EUR = 1/1000000000     # to convert from sigma to €

    def run_analysis(self) -> None:
        """calls all pre-configured plotting functions

        Args:

        Returns:
            None

        """

        self.plot_virtual_feeder_flow()             # plots the virtual power flow of the LEM
        #self.plot_mcp()                             # plots the market clearing prices and their weighted average
        #self.plot_balance()                         # plots the balance of each household at the end
        #self.plot_price_type()                      # plots price vs. type of energy over time
        #self.plot_household()                       # plots the power profile of one household as example
        #self.plot_average_mcp_per_type()            # plots the weighted costs per energy for each household type

    def plot_virtual_feeder_flow(self) -> None:
        """plots the flow within the market over time

        Args:

        Returns:
            None

        """

        print("*** CREATING PLOT OF VIRTUAL POWERFLOW ***")

        # Get IDs of all main meters (1=utility with multiple submeters, 2=utility meter)
        df_meter_info = pd.read_csv(f"{self.path_results}/db_snapshot/{db_p.NAME_TABLE_INFO_METER}.csv", index_col=0)
        list_main_meters = list(df_meter_info[df_meter_info[db_p.TYPE_METER].isin(
            ["grid meter", "virtual grid meter"])][db_p.ID_METER])

        # Get power flows of all meters in list_main_meters
        df_meter_readings_delta = pd.read_csv(f"{self.path_results}/db_snapshot/"
                                              f"{db_p.NAME_TABLE_READINGS_METER_DELTA}.csv", index_col=0)

        df_results = df_meter_readings_delta[df_meter_readings_delta[db_p.ID_METER].isin(list_main_meters)]
        df_results = df_results.groupby(db_p.TS_DELIVERY).sum() * self.conv_to_kW
        df_results.columns = ["negative_flow_kW", "positive_flow_kW"]
        df_results["negative_flow_kW"] = - df_results["negative_flow_kW"]
        df_results["net_flow_kW"] = df_results["positive_flow_kW"] + df_results["negative_flow_kW"]
        cols = df_results.columns.tolist()
        cols = cols[1:] + [cols[0]]
        df_results = df_results[cols]
        df_results = df_results.sort_index()
        df_results = df_results.drop(columns=["positive_flow_kW", "negative_flow_kW"])
        # Plots
        scplotter = ScenarioPlotter()
        colors = ["black"]
        labels = ["Netto"]
        xvalues = df_results.index.values
        yvalues = df_results.transpose().values.tolist()
        for idx, yvalue in enumerate(yvalues):
            scplotter.ax.plot(xvalues, yvalue, color=colors[idx], label=labels[idx], linewidth=1)
            #scplotter.ax.axhline(0, color='black', linewidth=1.1)

        plt.axhspan(-100, 0, facecolor='red', alpha=0.2)
        plt.axhspan(0, 100, facecolor='green', alpha=0.2)
        # Figure setup
        xlims = [min(xvalues), max(xvalues)]
        scplotter.figure_setup(title="Virtual microgrid power flow summary",
                               ylabel="Leistung (kW)",
                               legend_labels=("Netto",),
                               xlims=xlims,
                               xticks_style="date")
        if self.save_figures:
            self.__save_figure(name="virtual_feeder_flow")
        if self.show_figures:
            plt.show()

    def determine_autarky(self) -> None:
        """This function calculates the degree of energy independency of the simulated LEM"""

        # Get IDs of all main meters (1=utility with multiple submeters, 2=utility meter)
        df_meter_info = pd.read_csv(f"{self.path_results}/db_snapshot/{db_p.NAME_TABLE_INFO_METER}.csv", index_col=0)
        list_submeters = list(df_meter_info[df_meter_info[db_p.TYPE_METER].isin(
            ["plant submeter", "virtual plant submeter", ""])][db_p.ID_METER])

        # Get power flows of all meters in list_main_meters
        df_meter_readings_delta = pd.read_csv(f"{self.path_results}/db_snapshot/"
                                              f"{db_p.NAME_TABLE_READINGS_METER_DELTA}.csv", index_col=0)

        df_results1 = df_meter_readings_delta[df_meter_readings_delta[db_p.ID_METER].isin(list_submeters)]
        df_results1 = df_results1.groupby(db_p.TS_DELIVERY).sum()
        df_results1.columns = ["negative_flow_kW", "positive_flow_kW"]
        df_results1["negative_flow_kW"] = - df_results1["negative_flow_kW"]
        df_results1["net_flow_kW"] = df_results1["positive_flow_kW"] + df_results1["negative_flow_kW"]

        sum_consumption = df_results1["negative_flow_kW"].sum()

        # Get IDs of all main meters (1=utility with multiple submeters, 2=utility meter)

        list_main_meters = list(df_meter_info[df_meter_info[db_p.TYPE_METER].isin(
            ["grid meter", "virtual grid meter"])][db_p.ID_METER])
        # Get power flows of all meters in list_main_meters
        df_meter_readings_delta = pd.read_csv(f"{self.path_results}/db_snapshot/"
                                              f"{db_p.NAME_TABLE_READINGS_METER_DELTA}.csv", index_col=0)
        df_results = df_meter_readings_delta[df_meter_readings_delta[db_p.ID_METER].isin(list_main_meters)]
        df_results = df_results.groupby(db_p.TS_DELIVERY).sum()
        df_results.columns = ["negative_flow_kW", "positive_flow_kW"]
        df_results["negative_flow_kW"] = - df_results["negative_flow_kW"]
        df_results["net_flow_kW"] = df_results["positive_flow_kW"] + df_results["negative_flow_kW"]
        df_results["grid_cons"] = df_results[df_results["net_flow_kW"] <= 0][["net_flow_kW"]]
        df_results["grid_prod"] = df_results[df_results["net_flow_kW"] >= 0][["net_flow_kW"]]
        df_results.fillna(0, inplace=True)
        sum_consumption_grid = df_results["grid_cons"].sum()

        autarky = sum_consumption_grid/sum_consumption
        autarky = 100 - round(autarky * 100, 1)

        print("Autarky")
        print(autarky)
        print("Internal consumption")
        print(sum_consumption)
        print("External consumption")
        print(sum_consumption_grid)
        print("External production")
        print(df_results["grid_prod"].sum())

    def plot_mcp(self, type_market: str = None) -> None:
        """checks the market type to be plotted and calls the respective subfunction to plot the weighted average and
        the individual market (if applicable) clearing prices for each time step

        Args:
            type_market: string that specifies, which market is to be plotted

        Returns:
            None

        """

        print("*** CREATING PLOT OF ELECTRICITY PRICES ***")

        # Get the market that is to be plotted if none is provided and the corresponding name of the price column
        if type_market is None:
            if len(self.config["lem"]["types_clearing_ex_ante"]):
                type_market = f"ex_ante_{self.config['lem']['types_clearing_ex_ante'][0]}"
            elif len(self.config["lem"]["types_clearing_ex_post"]):
                type_market = f"ex_post_{self.config['lem']['types_clearing_ex_post'][0]}"
        column_price = [column for column in self.__get_table_columns(f"results_market_{type_market}")
                        if db_p.PRICE_ENERGY_MARKET_ in column][0]

        if "ex_ante" in type_market:
            self.__mcp_ex_ante(type_market, column_price)
        elif "ex_post" in type_market:
            self.__mcp_ex_post(type_market, column_price)
        else:
            raise NameError

    def plot_balance(self) -> None:
        """plots the balance of each market participant

        Args:

        Returns:
            None

        """

        print("*** CREATING PLOT OF PARTICIPANT BALANCES ***")

        # Create dataframe to gather all the information and do the necessary calculations
        df_results = pd.DataFrame(columns=[db_p.ID_USER, db_p.ID_METER, "PV_Bat_EV_HP_Wind_Fix", "revenue_sold_€",
                                           "cost_bought_€", "balance_€"]).set_index(db_p.ID_USER)

        # Look up each participant's main meter id
        df_meters = pd.read_csv(f"{self.path_results}/db_snapshot/{db_p.NAME_TABLE_INFO_METER}.csv", index_col=0, dtype={"id_user": str})
        df_temp = df_meters[df_meters[db_p.TYPE_METER].isin(["grid meter", "virtual grid meter"])]\
            [[db_p.ID_USER, db_p.ID_METER]]
        df_temp.set_index(db_p.ID_USER, inplace=True)
        df_results[db_p.ID_USER] = df_temp.index
        df_results.set_index(db_p.ID_USER, inplace=True)
        df_results[db_p.ID_METER] = df_temp

        # Check which market participants have PV, batteries, EVs and heat pumps
        df_results["PV_Bat_EV_HP_Wind_Fix"] = self.pv_bat_ev_hp_wind_fix
        # Sort all the transactions according to the user for the time period max_time
        df_transactions = pd.read_csv(f"{self.path_results}/db_snapshot/{db_p.NAME_TABLE_LOGS_TRANSACTIONS}.csv",
                                      index_col=0)
        df_transactions = df_transactions[df_transactions[db_p.TS_DELIVERY] <= self.max_time]
        df_transactions[db_p.DELTA_BALANCE] = df_transactions[db_p.DELTA_BALANCE] * self.conv_to_EUR
        df_temp_pos = df_transactions[df_transactions[db_p.QTY_ENERGY] >= 0]
        df_temp_pos = df_temp_pos.groupby(db_p.ID_USER).sum()
        df_temp_neg = df_transactions[df_transactions[db_p.QTY_ENERGY] < 0]
        df_temp_neg = df_temp_neg.groupby(db_p.ID_USER).sum()
        df_results["revenue_sold_€"] = df_temp_pos[db_p.DELTA_BALANCE]
        df_results["cost_bought_€"] = - df_temp_neg[db_p.DELTA_BALANCE]
        df_results = df_results.fillna(0)
        df_results["balance_€"] = df_results["revenue_sold_€"] - df_results["cost_bought_€"]

        # Get rid of retailer and aggregator
        try:
            df_results = df_results.drop([self.config["retailer"]["id_user"]])
        except:
            pass
        try:
            df_results = df_results.drop([self.config["aggregator"]["id_user"]])
        except:
            pass

        # Check for large-scale producers
        self.get_producers(df_results)

        # Transpose list and replace the booleans with checkmarks and blanks
        cell_texts = self.create_checkmarks(df_results["PV_Bat_EV_HP_Wind_Fix"])


        # Plots
        # Check number of necessary plots
        max_par = 20                                                # maximum number of participants in one plot
        num_plots = int(np.ceil((len(df_results) - 1) / max_par))   # for every max_par participants a separate plot
        for x in range(num_plots):
            # Bar plot
            scplotter = ScenarioPlotter()
            xvalues = [int(x) for x in df_results.index.values.tolist()][x * max_par:(x + 1) * max_par]
            yvalues = df_results["balance_€"].tolist()[x * max_par:(x + 1) * max_par]
            scplotter.ax.bar(xvalues, yvalues, color="0.6")
            # Create table
            columns = [f"#{x}" for x in xvalues]
            rows = ["PV", "Battery", "EV", "Heat pump", "Wind", "Fixed gen"]
            cell_text = [cell[x * max_par:(x + 1) * max_par] for cell in cell_texts]
            the_table = scplotter.ax.table(cellText=cell_text, rowLabels=rows, colLabels=columns,
                                           loc="bottom", cellLoc="center")
            self.change_table_height(the_table, 1.2)
            # Display balance of retailer
            text_str = f"retailer: {round(df_results['balance_€'].tolist()[0], 2)} €"
            bbox_style = {
                "edgecolor": "k",
                "facecolor": "w",
                "alpha": 0.4,
            }
            scplotter.ax.text(0.97, 0.05, text_str, transform=scplotter.ax.transAxes, fontsize=12,
                              verticalalignment="bottom", horizontalalignment="right", bbox=bbox_style)
            # Figure setup
            scplotter.ax.set(ylim=(min(0, math.floor(min(df_results["balance_€"].tolist()[1:]) * 1.1)),
                                   max(0, math.ceil(max(df_results["balance_€"].tolist()[1:]) * 1.1))))
            scplotter.figure_setup(title="Balances of market participants", ylabel="Balance (€)")
            if self.save_figures:
                self.__save_figure(name=f"balance_{x}")
            if self.show_figures:
                plt.show()

    def plot_price_type(self, type_market: str = None) -> None:
        """checks the market type to be plotted and calls the respective subfunction to plot the average clearing price
        and the quality of the delivered energy for each time step

        Args:
            type_market: string that specifies, which market is to be plotted

        Returns:
            None

        """

        print("*** CREATING PLOT OF PRICE VS TYPE OF ENERGY ***")

        # Get the market that is to be plotted if none is provided and the corresponding name of the price column
        if type_market is None:
            if len(self.config["lem"]["types_clearing_ex_ante"]):
                type_market = f"ex_ante_{self.config['lem']['types_clearing_ex_ante'][0]}"
            elif len(self.config["lem"]["types_clearing_ex_post"]):
                type_market = f"ex_post_{self.config['lem']['types_clearing_ex_post'][0]}"
        column_price = [column for column in self.__get_table_columns(f"results_market_{type_market}")
                        if db_p.PRICE_ENERGY_MARKET_ in column][0]

        if "ex_ante" in type_market:
            self.__price_type_ex_ante(type_market, column_price)
        elif "ex_post" in type_market:
            self.__price_type_ex_post(type_market, column_price)
        else:
            raise NameError

    def plot_household(self, type_household: tuple = (1, 1, 1, 0, 0, 0), id_user: int = None) -> None:
        """gathers information about the chosen example household and calls the subfunctions to plot the power profile
        and the power purchases and sales over time

        Args:
            type_household: tuple that specifies which type of household is to be plotted. 1 means that the type of
                            plant is part of the household. The following order is applied (pv, bat, ev, hp, fixedgen)
            id_user: string that contains the ID of a specific user that is to be plotted. If an ID is provided the
                     parameter type_household is ignored

        Returns:
            None

        """

        print("*** CREATING PLOTS OF EXAMPLE HOUSEHOLD ***")

        # Load meter information
        df_meters = pd.read_csv(f"{self.path_results}/db_snapshot/{db_p.NAME_TABLE_INFO_METER}.csv", index_col=0, dtype={"id_user": str})

        # Create dataframe to store the information about the participants
        df_users = pd.DataFrame(columns=[db_p.ID_USER, "main_id", "PV_Bat_EV_HP_Wind_Fix"])
        df_users[db_p.ID_USER] = df_meters[db_p.ID_USER].unique()
        df_users["main_id"] = df_users[db_p.ID_USER]
        df_users.set_index(db_p.ID_USER, inplace=True)

        # Get rid of retailer and aggregator
        try:
            df_users = df_users.drop([self.config["retailer"]["id_user"]])
        except:
            pass
        try:
            df_users = df_users.drop([self.config["aggregator"]["id_user"]])
        except:
            pass
        # Find the ID of each main meter
        df_temp = df_meters[df_meters[db_p.TYPE_METER].isin(["grid meter", "virtual grid meter"])]
        map_user_to_meter = dict([(i, a) for i, a in zip(df_temp["id_user"], df_temp["id_meter"])])

        # Check which market participants have PV, batteries, EVs and heat pumps
        df_users["PV_Bat_EV_HP_Wind_Fix"] = self.pv_bat_ev_hp_wind_fix
        df_users = df_users.replace({"main_id": map_user_to_meter})
        # Check if a specific user was provided to be analyzed, if not plot a random household with the provided config
        if id_user:
            # id_user can be specified either as integer or as string
            if type(id_user) is int:
                idx = [idx for idx in df_users.index.values if int(idx) == id_user][0]
            elif type(id_user) is str:
                idx = [idx for idx in df_users.index.values if idx == id_user][0]
            else:
                raise TypeError("id_user can only be an integer or a string.")
            id_meter = df_users["main_id"][idx]
            type_household = df_users["PV_Bat_EV_HP_Wind_Fix"][idx]
        else:
            # Get example household based on type_household
            users = df_users.loc[df_users["PV_Bat_EV_HP_Wind_Fix"] == type_household, "main_id"]

            # If a user has the specifications as in type_household, choose last user
            if len(users) > 0:
                id_meter = users.values[0]
            # Else find the user with the most devices to depict
            else:
                idx = [sum(x) for x in df_users["PV_Bat_EV_HP_Wind_Fix"]]
                idx = idx.index(max(idx))
                print(f"Chosen household type does not exist. The following household type was chosen: "
                      f"{df_users['PV_Bat_EV_HP_Wind_Fix'][idx]} (PV, Battery, EV, Heat pump, Wind, Fixed gen)")
                type_household = df_users["PV_Bat_EV_HP_Wind_Fix"][idx]  # update type_household to find the correct meter IDs
                id_meter = df_users["main_id"][idx]

        # Get the meter ids of the submeters and setup plot labels and colors
        # Household meter
        devices = ["hh"]
        labels = ["Main meter", "Household"]
        colors = ["0.2", "#a8d277"]
        # Get ID of household meter. If no household meter exists, look for virtual meter that represents the
        #   residual load
        try:
            ids = [df_meters.loc[(df_meters[db_p.ID_METER_SUPER] == id_meter) &
                                 (df_meters[db_p.INFO_ADDITIONAL] == devices[0]),
                                 db_p.ID_METER].values[0]]
        except IndexError:
            try:
                devices = ["residual load"]
                labels = ["Main meter", "Residual load"]
                ids = [df_meters.loc[(df_meters[db_p.ID_METER_SUPER] == id_meter) &
                                     (df_meters[db_p.INFO_ADDITIONAL] == devices[0]),
                                     db_p.ID_METER].values[0]]
            except IndexError:
                devices = ["virtual submeter"]
                labels = ["Main meter", "Plant"]
                # print(df_meters[db_p.INFO_ADDITIONAL].isin(devices))
                ids = [df_meters.loc[(df_meters[db_p.ID_METER_SUPER] == id_meter) &
                                     (df_meters[db_p.INFO_ADDITIONAL].str.contains(devices[0])),
                                     db_p.ID_METER].values[0]]

        # Optional meters
        devices_opt = ("pv", "bat", "ev", "hp", "wind", "fixedgen")  # optional devices that the participant might have
        labels_opt = ["PV", "Battery", "EV", "Heat pump", "Wind", "Fixed gen"]  # according labels for the devices
        colors_opt = ["#ffd045", "#ec6a0e", "#2791be", "#c33528", "#28a9c3", "0.7"]  # according colors for the devices
        for idx, elem in enumerate(type_household):
            if type_household[idx]:  # check if type of device should be included in plot
                # Get ID of optional device. If it does not exist, ignore it since it is part of the residual load
                try:
                    ids.append(df_meters.loc[(df_meters[db_p.ID_METER_SUPER] == id_meter) &
                                             (df_meters[db_p.INFO_ADDITIONAL] == devices_opt[idx]),
                                             db_p.ID_METER].values[0])
                except IndexError:
                    print(f"The user has a {labels_opt[idx]} plant, however, it has no separate meter and is included "
                          f"in the main meter/residual load.")
                else:
                    devices.append(devices_opt[idx])
                    labels.append(labels_opt[idx])
                    colors.append(colors_opt[idx])

        # Plot household power profile and financial balance
        id_users = df_users.index.values
        id_user = df_users[df_users["main_id"] == id_meter].index.values[0]
        self.__plot_household_power(id_user, id_meter, ids, devices, labels, colors)
        self.__plot_household_finance(id_users, id_user, complete_table=False)

    def plot_average_mcp_per_type(self, all_types: bool = False) -> None:
        """This function plots the specific energy purchasing costs and average total energy purchased
           for each type of participant.

        Args:
            all_types: boolean that specifies if plot should show also the non-existing types as column or not

        Returns:
            None

        """

        print("*** CREATING PLOT OF WEIGHTED BALANCES PER USER TYPE ***")

        # Create dataframe to gather all the information and do the necessary calculations
        df_info = pd.DataFrame(columns=[db_p.ID_USER, db_p.ID_METER, "PV_Bat_EV_HP_Wind_Fix", "energy_sold_kWh",
                                        "revenue_sold_€", "energy_bought_kWh", "cost_bought_€", "energy_balance_kWh",
                                        "balance_€", "consumption_kWh", "avg_price_€/kWh"]).set_index(db_p.ID_USER)

        # Look up each participants main meter id
        df_meters = pd.read_csv(f"{self.path_results}/db_snapshot/{db_p.NAME_TABLE_INFO_METER}.csv", index_col=0,
                                dtype={"id_user": str})
        df_temp = df_meters[df_meters[db_p.TYPE_METER].isin(["grid meter", "virtual grid meter"])] \
            [[db_p.ID_USER, db_p.ID_METER]]
        df_temp.set_index(db_p.ID_USER, inplace=True)
        df_info[db_p.ID_USER] = df_temp.index
        df_info.set_index(db_p.ID_USER, inplace=True)
        df_info[db_p.ID_METER] = df_temp

        # Check which market participants have PV, batteries, EVs, heat pumps, wind and fixed gen
        df_info["PV_Bat_EV_HP_Wind_Fix"] = self.pv_bat_ev_hp_wind_fix

        # Sort all the transactions according to the user for the time period max_time
        df_transactions = pd.read_csv(f"{self.path_results}/db_snapshot/{db_p.NAME_TABLE_LOGS_TRANSACTIONS}.csv",
                                      index_col=0)
        df_transactions = df_transactions[df_transactions[db_p.TS_DELIVERY] <= self.max_time]
        df_temp_pos = df_transactions[df_transactions[db_p.QTY_ENERGY] >= 0]
        df_temp_pos = df_temp_pos.groupby(db_p.ID_USER).sum()
        df_info["energy_sold_kWh"] = df_temp_pos[db_p.QTY_ENERGY] * self.conv_to_kWh
        df_info["revenue_sold_€"] = df_temp_pos[db_p.DELTA_BALANCE] * self.conv_to_EUR
        df_temp_neg = df_transactions[df_transactions[db_p.QTY_ENERGY] < 0]
        df_temp_neg_energy = df_temp_neg[df_temp_neg[db_p.TYPE_TRANSACTION].isin(["market"])]. \
            groupby(db_p.ID_USER).sum()
        df_info["energy_bought_kWh"] = - df_temp_neg_energy[db_p.QTY_ENERGY] * self.conv_to_kWh
        # df_temp_neg_cost = df_temp_neg.groupby(db_p.ID_USER).sum()
        df_info["cost_bought_€"] = - df_temp_neg_energy[db_p.DELTA_BALANCE] * self.conv_to_EUR

        df_info = df_info.fillna(0)
        df_info["energy_balance_kWh"] = df_info["energy_sold_kWh"] - df_info["energy_bought_kWh"]
        df_info["balance_€"] = df_info["revenue_sold_€"] - df_info["cost_bought_€"]
        df_info["n_participants"] = 1
        # Get the power consumption of every household by checking the submeter's delta readings
        df_consumption = pd.read_csv(f"{self.path_results}/db_snapshot/{db_p.NAME_TABLE_READINGS_METER_DELTA}.csv",
                                     index_col=0)
        df_meters = df_meters.set_index("id_meter")
        df_consumption = df_consumption[df_consumption[db_p.TS_DELIVERY] <= self.max_time].sort_values(db_p.TS_DELIVERY)
        df_consumption = df_consumption.groupby("id_meter").sum()

        df_consumption["main_meter"], df_consumption["id_user"] = df_meters["id_meter_super"], df_meters["id_user"]
        # Sort out all main meter, and PV, wind and fixed gen meters as only the consumption of the household load,
        # the heatpump, the ev the battery is important (current method simply adds up energy_in)
        df_consumption = df_consumption[df_consumption["main_meter"] != "0000000000"]  # delete main meter
        df_consumption = df_consumption[df_consumption["energy_in"] > 0]  # delete all generators
        df_consumption["energy_consumed"] = df_consumption["energy_in"]  # "- df_consumption["energy_out"]",
        #     if battery discharge to be subtracted
        df_consumption = df_consumption.groupby("id_user").sum()
        df_info["consumption_kWh"] = df_consumption["energy_consumed"] / 1000
        # avg_price: negative balance means positive price  (you have to pay for every kWh);
        # positive balance means negative price (you receive money for every kWh)
        df_info["avg_price_€/kWh"] = - df_info["balance_€"].abs() / df_info["consumption_kWh"]
        df_info["avg_price_€/kWh"] = - df_info["cost_bought_€"].abs() / df_info["energy_bought_kWh"]
        df_info.loc[abs(df_info["avg_price_€/kWh"]) == float("inf"), "avg_price_€/kWh"] = 0

        # Group all participants by PV_Bat_EV_HP_Wind_Fix and create list of single values for boxplots
        df_temp = df_info.groupby("PV_Bat_EV_HP_Wind_Fix").sum()
        df_temp["avg_price_€/kWh"] = - df_temp["balance_€"] / df_temp["consumption_kWh"]
        df_temp["avg_price_€/kWh"] = - df_temp["cost_bought_€"].abs() / df_temp["energy_bought_kWh"]

        df_temp["n_avg_price_ct/kWh"] = 0
        df_temp["n_avg_price_ct/kWh"] = df_temp["n_avg_price_ct/kWh"].astype("object")

        df_temp["n_avg_energy_bought_kWh"] = df_temp["energy_bought_kWh"]/df_temp["n_participants"]
        for user_type in df_temp.index.values:
            df_temp.at[user_type, "n_avg_price_ct/kWh"] = list(
                df_info[df_info["PV_Bat_EV_HP_Wind_Fix"] == user_type]["avg_price_€/kWh"] * -100)

        # Create dataframe to store the information for the graph depending on display type
        if all_types and len(df_temp) < 16:
            df_results = pd.DataFrame(columns=["PV_Bat_EV_HP_Wind_Fix", "avg_price_€/kWh", "n_participants",
                                               "n_avg_price_ct/kWh"])
            df_results["PV_Bat_EV_HP_Wind_Fix"] = list(it.product([0, 1], repeat=len(df_temp.index.values[0])))
            df_results.set_index("PV_Bat_EV_HP_Wind_Fix", inplace=True)
            df_results["avg_price_€/kWh"] = df_temp["avg_price_€/kWh"]
            df_results["n_participants"] = df_temp["n_participants"]
            df_results["n_avg_price_ct/kWh"] = df_temp["n_avg_price_ct/kWh"]
            df_results = df_results.fillna(0)
        else:
            df_results = df_temp
        df_results.loc[abs(df_results["avg_price_€/kWh"]) == float("inf"), "avg_price_€/kWh"] = 0

        # Transpose list of indices and replace the booleans with checkmarks and blanks for the table
        base_cell_texts = self.create_checkmarks(df_results.index.values)
        base_rows = ["PV", "Battery", "EV", "Heat pump", "Wind", "Fixed gen"]

        cell_texts = []
        rows = []

        for i, row in enumerate(base_cell_texts):
            num_char = 0
            for entry in row:
                num_char += len(entry)
            if num_char:
                rows.append(base_rows[i])
                cell_texts.append(base_cell_texts[i])

        # Plots
        # Box plot (commented out lines are for manual sorting)
        # order = [1, 0, 3, 5, 2, 4]
        scplotter = ScenarioPlotter()
        raw_data = list(df_results["n_avg_price_ct/kWh"])
        # drop nan
        data = []
        for entry in raw_data:
            data.append([datapoint for datapoint in entry if not (math.isnan(datapoint))])

        scplotter.ax2 = scplotter.ax.twinx()
        plt1 = scplotter.ax.bar(x=list(range(1, len(data)+1)), height=list(df_results["n_avg_energy_bought_kWh"]),
                                label="Average energy import")
        # scplotter.ax.legend(loc="lower right")
        plt2 = scplotter.ax2.boxplot(data, showfliers=False)

        scplotter.ax2.legend([plt1, plt2["whiskers"][1]],
                            ["Mean quantity of energy purchased", "Mean price of energy purchased"],
                            loc='lower left')
                            # bbox_to_anchor=(0, 0))

        # Create table
        columns = [f"n={round(x)}" for x in df_results["n_participants"]]
        # columns = [columns[i] for i in order]

        # cell_texts = list(map(list, zip(*cell_texts)))
        # cell_texts = [cell_texts[i] for i in order]
        # cell_texts = list(map(list, zip(*cell_texts)))
        the_table = scplotter.ax.table(cellText=cell_texts, rowLabels=rows, colLabels=columns,
                                       loc="bottom", cellLoc="center", colLoc="center")
        self.change_table_height(the_table, 1.2)
        # Figure setup
        scplotter.grid_b_minor = True
        scplotter.figure_setup(ylabel_right="Price (c/kWh)",
                               ylabel="Energy (kWh)")
        scplotter.ax2.set_ylim([0, 10])
        scplotter.ax.set_ylim([0, 40])
        # scplotter.ax2.legend(["", "box"])
        # scplotter.ax.legend(["bar"])


        if self.save_figures:
            self.__save_figure(name=f"balance_per_type")
        if self.show_figures:
            plt.show()

    # internal functions

    def __price_type_ex_ante(self, type_market, column_price) -> None:
        """plots the average clearing price and the quality of the delivered energy for each time step for
        ex-ante markets

        Args:
            type_market: string that specifies, which market is to be plotted
            column_price: string that contains the number of the column that contains the prices

        Returns:
            None

        """

        # Get market data and truncate at maximum simulated time step
        df_market_results = pd.read_csv(f"{self.path_results}/db_snapshot/results_market_{type_market}.csv",
                                        index_col=0)
        df_market_results = df_market_results[df_market_results[db_p.TS_DELIVERY] <= self.max_time]

        # Dataframe setup with timestamps as indices
        df_results = pd.DataFrame(columns=["timestamp", "energy_kWh", "cost_€", "price_€/kWh",
                                           "energy_loc_kWh", "loc_share", "energy_greloc_kWh",
                                           "greloc_share"])
        df_results["timestamp"] = sorted(df_market_results[db_p.TS_DELIVERY].unique())
        df_results.set_index("timestamp", inplace=True)

        # Get data from market results
        df_results["energy_kWh"] = df_market_results.groupby(db_p.TS_DELIVERY).sum()[db_p.QTY_ENERGY_TRADED] * \
                                   self.conv_to_kWh
        df_market_results["costs"] = df_market_results[db_p.QTY_ENERGY_TRADED] * \
                                     df_market_results[column_price]
        df_results["cost_€"] = df_market_results.groupby(db_p.TS_DELIVERY).sum()["costs"] * self.conv_to_EUR
        df_results["price_€/kWh"] = df_results["cost_€"] / df_results["energy_kWh"]

        # Get the shares of the different energy types
        # NOTE: The values of df_results are the total values meaning that the green share includes both green
        # non-local and local while local included local green and fossil. In df_market_results they are separate
        df_market_results["energy_loc"] = df_market_results[db_p.QTY_ENERGY_TRADED] * \
                                          df_market_results["share_quality_offers_cleared_local"] / 100
        df_market_results["energy_greloc"] = df_market_results[db_p.QTY_ENERGY_TRADED] * \
                                             df_market_results["share_quality_offers_cleared_green_local"] / 100

        df_results["energy_loc_kWh"] = (df_market_results.groupby(db_p.TS_DELIVERY).sum()["energy_loc"] +
                                        df_market_results.groupby(db_p.TS_DELIVERY).sum()["energy_greloc"]) * \
                                       self.conv_to_kWh
        df_results["energy_greloc_kWh"] = df_market_results.groupby(db_p.TS_DELIVERY).sum()["energy_greloc"] * \
                                       self.conv_to_kWh

        df_results["loc_share"] = df_results["energy_loc_kWh"] / df_results["energy_kWh"]
        df_results["greloc_share"] = df_results["energy_greloc_kWh"] / df_results["energy_kWh"]
        df_results = df_results.fillna(0)
        df_results = df_results.sort_index()

        # Plot lines and bars
        # Plots
        scplotter = ScenarioPlotter()
        xvalues = df_results.index.values.tolist()
        # Local share (left y-axis)
        yvalues = [x * 100 for x in df_results["loc_share"].tolist()]
        scplotter.ax.fill_between(xvalues, yvalues, color="#e79208", alpha=0.5, label="Local")
        # Local share (left y-axis)
        yvalues = [x * 100 for x in df_results["greloc_share"].tolist()]
        scplotter.ax.fill_between(xvalues, yvalues, color="#369f28", alpha=0.5, label="Green & local")
        scplotter.ax.set(ylim=(0, 100))
        # Price plot (right y-axis)
        yvalues = [x for x in df_results["price_€/kWh"].tolist()]
        scplotter.ax2 = scplotter.ax.twinx()
        scplotter.ax2.plot(xvalues, yvalues, color="0.2", linewidth=3, alpha=1, label="Price")
        scplotter.ax2.set(ylim=(min(0, round(min(yvalues) * 1.1, 1)), max(0, round(max(yvalues) * 1.1, 1))))
        # Figure setup
        xlims = [min(xvalues), max(xvalues)]
        lines_1, labels_1 = scplotter.ax.get_legend_handles_labels()
        lines_2, labels_2 = scplotter.ax2.get_legend_handles_labels()
        lines = lines_1 + lines_2
        labels = labels_1 + labels_2
        scplotter.ax.legend(lines, labels, bbox_to_anchor=(0.5, -0.2), ncol=min(4, len(labels)))
        scplotter.figure_setup(title="MCP vs. energy quality", xlabel="",
                               ylabel="Energy quality share (%)", ylabel_right="Average MCP (€/kWh)",
                               xlims=xlims, xticks_style="date")
        if self.save_figures:
            self.__save_figure(name=f"price_type_{type_market}")
        if self.show_figures:
            plt.show()

    def __price_type_ex_post(self, type_market, column_price) -> None:
        """plots the average clearing price and the quality of the delivered energy for each time step for
        ex-post markets

        Args:
            type_market: string that specifies, which market is to be plotted
            column_price: string that contains the number of the column that contains the prices

        Returns:
            None

        """

        # Get market data and truncate at maximum simulated time step
        df_market_results = pd.read_csv(f"{self.path_results}/db_snapshot/results_market_{type_market}.csv",
                                        index_col=0)
        df_market_results = df_market_results[df_market_results[db_p.TS_DELIVERY] <= self.max_time]
        df_market_results.set_index(db_p.TS_DELIVERY, inplace=True)
        df_market_results = df_market_results.sort_index()

        # Add shares of the same quality (green or local) to receive total values
        df_market_results["loc_share"] = df_market_results["share_quality_local"] + \
                                         df_market_results["share_quality_green_local"]

        # Plot lines and bars
        # Plots
        scplotter = ScenarioPlotter(

        )
        xvalues = df_market_results.index.values.tolist()
        # Local share (left y-axis)
        yvalues = df_market_results["loc_share"].tolist()
        scplotter.ax.fill_between(xvalues, yvalues, color="#e79208", alpha=0.5, label="Local")
        # Local share (left y-axis)
        yvalues = df_market_results["share_quality_green_local"].tolist()
        scplotter.ax.fill_between(xvalues, yvalues, color="#369f28", alpha=0.5, label="Green & local")
        scplotter.ax.set(ylim=(0, 100))
        # Price plot (right y-axis)
        yvalues = [x * self.conv_to_EUR / self.conv_to_kWh * 100 for x in df_market_results[column_price]]
        scplotter.ax2 = scplotter.ax.twinx()
        scplotter.ax2.plot(xvalues, yvalues, color="0.2", linewidth=3, alpha=1, label="Price")
        scplotter.ax2.set(ylim=(min(0, round(min(yvalues) * 1.1, 1)), max(0, round(max(yvalues) * 1.1, 1))))
        # Figure setup
        xlims = [min(xvalues), max(xvalues)]
        lines_1, labels_1 = scplotter.ax.get_legend_handles_labels()
        lines_2, labels_2 = scplotter.ax2.get_legend_handles_labels()
        lines = lines_1 + lines_2
        labels = labels_1 + labels_2
        scplotter.ax.legend(lines, labels, bbox_to_anchor=(0.5, -0.2), ncol=min(4, len(labels)))
        scplotter.figure_setup(title="Price vs. sustainability & locality", xlabel="",
                               ylabel="Share (%)", ylabel_right="Electricity price (ct/kWh)",
                               xlims=xlims, xticks_style="date")
        if self.save_figures:
            self.__save_figure(name=f"price_type_{type_market}")
        if self.show_figures:
            plt.show()

    def __plot_household_power(self, id_user, id_meter, ids, devices, labels, colors) -> None:
        """plots the power profile of the example household over time

        Args:
            id_user: string that contains the ID of the user
            id_meter: string that contains the ID of the main meter
            ids: list that contains the IDs of the additional meters
            devices: list that contains the types of the additional meters
            labels: list that contains the labels of all meters
            colors: list that contains the colors for the plots of the meters

        Returns:
            None

        """

        # Get meter data of example household for each submeter ID
        df_meter_readings = pd.read_csv(f"{self.path_results}/db_snapshot/{db_p.NAME_TABLE_READINGS_METER_DELTA}.csv",
                                        index_col=0)
        df_user = df_meter_readings.loc[df_meter_readings[db_p.ID_METER] == id_meter]
        df_user = df_user.set_index(db_p.TS_DELIVERY)

        df_user["main_reading"] = (df_user["energy_out"] - df_user["energy_in"]) * self.conv_to_kW
        for idx in range(len(devices)):
            try:
                df_temp = df_meter_readings.loc[df_meter_readings[db_p.ID_METER] == ids[idx]]
                df_temp = df_temp.set_index(db_p.TS_DELIVERY)
                df_user[f"{devices[idx]}_reading"] = (df_temp["energy_out"] - df_temp["energy_in"]) * self.conv_to_kW
            except NameError as ne:
                print(ne)
        df_user = df_user.sort_index()

        # Plots
        scplotter = ScenarioPlotter()
        xvalues = df_user.index.values
        df_pos, df_neg = df_user.iloc[:, 4:].clip(lower=0), df_user.iloc[:, 4:].clip(upper=0)
        # Line plot of main meter
        yvalues = df_user.iloc[:, 3].transpose().values.tolist()
        scplotter.ax.plot(xvalues, yvalues, linewidth=3, color=colors[0])
        # Stackplot of submeters (positive values)
        yvalues = df_pos.transpose().values.tolist()
        scplotter.ax.stackplot(xvalues, yvalues, baseline="zero", colors=colors[1:])
        # Stackplot of submeters (negative values)
        yvalues = df_neg.transpose().values.tolist()
        scplotter.ax.stackplot(xvalues, yvalues, baseline="zero", colors=colors[1:])
        # Figure setup
        xlims = [min(xvalues), max(xvalues)]
        scplotter.figure_setup(title=f"Power flow for prosumer #{int(id_user)}", ylabel="Power (kW)",
                               legend_labels=labels, xlims=xlims, xticks_style="date")
        if self.save_figures:
            self.__save_figure(name=f"household_power_({int(id_user)})")
        if self.show_figures:
            plt.show()

    def __plot_household_finance(self, id_users, id_user, complete_table=False) -> None:
        """plots the power purchases and sales of the chosen example household

        Args:
            id_users: list that contains the IDs of all users
            id_user: string that contains the ID of the user with the chosen example household
            complete_table: boolean that specifies if the purchases and sales should be calculated for all users or not

        Returns:
            None

        """

        # Sort all the transactions according to the user for the time period max_time
        df_transactions = pd.read_csv(f"{self.path_results}/db_snapshot/{db_p.NAME_TABLE_LOGS_TRANSACTIONS}.csv",
                                      index_col=0)
        df_transactions = df_transactions[df_transactions[db_p.TS_DELIVERY] <= self.max_time]
        df_temp_revenue = df_transactions[df_transactions[db_p.DELTA_BALANCE] >= 0]
        df_temp_bal_pos = df_transactions[(df_transactions[db_p.DELTA_BALANCE] >= 0) &
                                          (df_transactions[db_p.TYPE_TRANSACTION] == "balancing")]
        df_temp_bal_neg = df_transactions[(df_transactions[db_p.DELTA_BALANCE] < 0) &
                                          (df_transactions[db_p.TYPE_TRANSACTION] == "balancing")]
        df_temp_levies = df_transactions[(df_transactions[db_p.DELTA_BALANCE] < 0) &
                                           (df_transactions[db_p.TYPE_TRANSACTION] == "levies")]
        df_temp_market = df_transactions[(df_transactions[db_p.DELTA_BALANCE] < 0) &
                                           (df_transactions[db_p.TYPE_TRANSACTION] == "market")]

        # Create dataframe to gather all the information and do the necessary calculations
        if complete_table:
            col = id_users
        else:
            col = [id_user]
        col = pd.MultiIndex.from_product([col, ["revenue_€", "balancing_pos_€", "balancing_neg_€", "levies_€",
                                                "market_€"]])
        df_balance = pd.DataFrame(index=sorted(df_transactions[db_p.TS_DELIVERY].unique()),
                                  columns=col)
        df_balance.index.name = db_p.TS_DELIVERY

        for column in df_balance.columns.get_level_values(level=0).unique():
            df_balance.loc[:, (column, "revenue_€")] = df_temp_revenue[df_temp_revenue[db_p.ID_USER] == column].\
                groupby(db_p.TS_DELIVERY).sum()[db_p.DELTA_BALANCE] * self.conv_to_EUR
            df_balance.loc[:, (column, "balancing_pos_€")] = df_temp_bal_pos[df_temp_bal_pos[db_p.ID_USER] == column].\
                groupby(db_p.TS_DELIVERY).sum()[db_p.DELTA_BALANCE] * self.conv_to_EUR
            df_balance.loc[:, (column, "balancing_neg_€")] = df_temp_bal_neg[df_temp_bal_neg[db_p.ID_USER] == column].\
                groupby(db_p.TS_DELIVERY).sum()[db_p.DELTA_BALANCE] * self.conv_to_EUR
            df_balance.loc[:, (column, "levies_€")] = df_temp_levies[df_temp_levies[db_p.ID_USER] == column].\
                groupby(db_p.TS_DELIVERY).sum()[db_p.DELTA_BALANCE] * self.conv_to_EUR
            df_balance.loc[:, (column, "market_€")] = df_temp_market[df_temp_market[db_p.ID_USER] == column].\
                groupby(db_p.TS_DELIVERY).sum()[db_p.DELTA_BALANCE] * self.conv_to_EUR
            df_balance = df_balance.fillna(0)
            df_balance.loc[:, (column, "balance_€")] = df_balance[column]["revenue_€"] \
                                                       + df_balance[column]["balancing_pos_€"] \
                                                       + df_balance[column]["balancing_neg_€"] \
                                                       + df_balance[column]["levies_€"] \
                                                       + df_balance[column]["market_€"]

        df_balance = df_balance.sort_index()

        # Plots
        scplotter = ScenarioPlotter()
        xvalues = df_balance.index.values
        bar_width = (xvalues[1] - xvalues[0]) * 0.8
        # Y-values of monetary inflow
        yvalues_pos = [yvalue for yvalue in df_balance[id_user]["revenue_€"].tolist()]
        yvalues1_pos = [yvalue for yvalue in df_balance[id_user]["balancing_pos_€"].tolist()]
        # Y-values of monetary outflow
        yvalues_neg = [yvalue for yvalue in df_balance[id_user]["market_€"].tolist()]
        yvalues1_neg = [yvalue for yvalue in df_balance[id_user]["levies_€"].tolist()]
        ybottom = [yvalues_neg[x]+yvalues1_neg[x] for x in range(len(yvalues_neg))] # auxiliary values
        yvalues2_neg = [yvalue for yvalue in df_balance[id_user]["balancing_neg_€"].tolist()]
        # Stacked bar chart (Note: Order is different than expected due to legend labeling behavior of matplotlib)
        scplotter.ax.bar(xvalues, yvalues_neg, bar_width,  color="#a02222", alpha=0.9, label="Cost")
        scplotter.ax.bar(xvalues, yvalues_pos, bar_width, color="green", alpha=0.9, label="Revenue")
        scplotter.ax.bar(xvalues, yvalues1_neg, bar_width,  bottom=yvalues_neg, color="#a02222", alpha=0.5,
                         label="Levies")
        scplotter.ax.bar(xvalues, yvalues1_pos, bar_width, bottom=yvalues_pos, color="green", alpha=0.3,
                         label="Pos. Balancing")
        scplotter.ax.bar(xvalues, yvalues2_neg, bar_width,  bottom=ybottom, color="#a02222", alpha=0.3,
                         label="Neg. Balancing")
        # Line plot of balance
        yvalues_bal = [yvalue for yvalue in (df_balance[id_user]["balance_€"]).to_list()]
        scplotter.ax.plot(xvalues, yvalues_bal, color="0.1", linewidth=2)
        # Figure setup
        xlims = [min(xvalues), max(xvalues)]
        labels = ("Balance", "Cost", "Revenue", "Levies", "Pos. Balancing", "Neg. Balancing")
        scplotter.figure_setup(title=f"Finances of household #{int(id_user)}", ylabel="Cash flow (€)", legend_labels=labels,
                               xlims=xlims, xticks_style="date")
        if self.save_figures:
            self.__save_figure(name=f"household_finance_({int(id_user)})")
        if self.show_figures:
            plt.show()

    def __mcp_ex_ante(self, type_market, column_price) -> None:
        """plots the weighted average and the individual market clearing prices for each time step for ex-ante markets

        Args:
            type_market: string that specifies, which market is to be plotted
            column_price: string that contains the number of the column that contains the prices

        Returns:
            None

        """

        # Read and prepare the desired market dataframe
        df_market_results = pd.read_csv(f"{self.path_results}/db_snapshot/results_market_{type_market}.csv",
                                        index_col=0)
        df_market_results = df_market_results[df_market_results[db_p.TS_DELIVERY] <= self.max_time]
        df_market_results["costs_€"] = df_market_results[db_p.QTY_ENERGY_TRADED] * df_market_results[column_price] * \
                                       self.conv_to_EUR

        # Create dataframe for the results
        df_results = pd.DataFrame(columns=["timestamp", "total_cost_€", "total_energy_kWh", "avg_price_€/kWh"])

        # Gather values form df_market_results and calculate weighted average
        df_results["timestamp"] = sorted(df_market_results[db_p.TS_DELIVERY].unique())
        df_results.set_index("timestamp", inplace=True)
        df_temp = df_market_results.groupby(db_p.TS_DELIVERY).sum()
        df_results["total_cost_€"] = df_temp["costs_€"]
        df_results["total_energy_kWh"] = df_temp[db_p.QTY_ENERGY_TRADED] * self.conv_to_kWh
        df_results["avg_price_€/kWh"] = df_results["total_cost_€"] / df_results["total_energy_kWh"]

        # Plots

        # Plot: Weighted average
        scplotter = ScenarioPlotter()
        xvalues = df_results.index.values
        yvalues = (df_results["avg_price_€/kWh"]).tolist()
        scplotter.ax.plot(xvalues, yvalues, color="#a02222", alpha=0.8) #, sizes=np.ones(len(xvalues)) * 20)

        # Plot: Market prices
        xvalues = df_market_results.ts_delivery
        yvalues = (df_market_results[column_price] * self.conv_to_EUR / self.conv_to_kWh).\
            tolist()
        scplotter.ax.scatter(xvalues, yvalues, color="#369f28", alpha=0.3, sizes=np.ones(len(xvalues)) * 5)
        # Figure setup
        xlims = [min(xvalues), max(xvalues)]
        scplotter.figure_setup(title="Market clearing prices (ex-ante)", ylabel="Market clearing price (€/kWh)",
                               xlabel="", legend_labels=("Weighted average", "Individual clearing"),
                               xlims=xlims, xticks_style="date")
        if self.save_figures:
            self.__save_figure(name=f"mcp_{type_market}")
        if self.show_figures:
            plt.show()

    def __mcp_ex_post(self, type_market, column_price) -> None:
        """plots the market clearing price for each time step for ex-post markets

        Args:
            type_market: string that specifies, which market is to be plotted
            column_price: string that contains the number of the column that contains the prices

        Returns:
            None

        """

        # Read the desired market dataframe
        df_market_results = pd.read_csv(f"{self.path_results}/db_snapshot/results_market_{type_market}.csv",
                                        index_col=0)
        df_market_results = df_market_results[df_market_results[db_p.TS_DELIVERY] <= self.max_time]
        df_market_results = df_market_results.sort_index()

        # Plot: Market prices
        scplotter = ScenarioPlotter()
        xvalues = df_market_results.ts_delivery
        yvalues = (df_market_results[column_price] * self.conv_to_EUR / self.conv_to_kWh * 100).to_list()
        scplotter.ax.scatter(xvalues, yvalues, color="#a02222", alpha=0.8, sizes=np.ones(len(xvalues)) * 20)
        # Figure setup
        xlims = [min(xvalues), max(xvalues)]
        scplotter.figure_setup(title="LEM clearing prices", ylabel="Electricity price (ct/kWh)",
                               xlims=xlims, xticks_style="date")
        if self.save_figures:
            self.__save_figure(name=f"mcp_{type_market}")
        if self.show_figures:
            plt.show()

    def __create_folder(self) -> None:
        """creates a folder in the specified path in path_analyzer

        Args:

        Returns:
            None

        """

        if not os.path.isdir(self.path_analyzer):
            os.mkdir(self.path_analyzer)
        time.sleep(0.1)

    def __max_timestamp(self) -> int:
        """checks for the last cleared timestamp

        Args:

        Returns:
            integer that represents the last timestamp that was cleared

        """

        db_settlement_status = pd.read_csv(f"{self.path_results}/db_snapshot/{db_p.NAME_TABLE_STATUS_SETTLEMENT}.csv",
                                           index_col=0)
        db_settlement_status = db_settlement_status.sort_values(by=db_p.TS_DELIVERY)

        return max(db_settlement_status[db_settlement_status[db_p.STATUS_SETTLEMENT_COMPLETE] == 1][db_p.TS_DELIVERY])

    def __check_pv_bat_ev_hp_wind_fix(self) -> tuple:
        """checks the plant configuration for all prosumers and returns results as boolean tuples

        Args:

        Returns:
            dataframe that contains a boolean tuple which represents the plant configuration for each prosumer

        """

        # Load meter information
        df_meters = pd.read_csv(f"{self.path_results}/db_snapshot/{db_p.NAME_TABLE_INFO_METER}.csv", index_col=0, dtype={"id_user": str})

        # Prepare temporary dataframe to create tuple column that contains if participants have devices
        df_temp = pd.DataFrame(columns=[db_p.ID_USER, "PV_Bat_EV_HP_Wind_Fix"])
        df_temp[db_p.ID_USER] = df_meters[db_p.ID_USER].unique()
        df_temp.set_index(db_p.ID_USER, inplace=True)
        df_temp["PV_Bat_EV_HP_Wind_Fix"] = df_temp["PV_Bat_EV_HP_Wind_Fix"].apply(lambda x: (0, 0, 0, 0, 0, 0))

        # Check which market participants have PV, batteries, EVs and heat pumps
        devices = ("pv", "bat", "ev", "hp", "wind", "fixedgen")

        for device in devices:
            df_temp[device] = df_meters[(df_meters[db_p.INFO_ADDITIONAL] == device)]. \
                              set_index(db_p.ID_USER)[db_p.INFO_ADDITIONAL]

        # Post-process data to create tuples of (x, x, x, x, x, x) x ∈ [0, 1] and return information
        df_temp = df_temp.fillna(0)
        df_temp = df_temp.stack()
        df_temp[df_temp != 0] = 1
        df_temp = df_temp.unstack()
        df_temp["PV_Bat_EV_HP_Wind_Fix"] = tuple(zip(df_temp["pv"], df_temp["bat"], df_temp["ev"],
                                                     df_temp["hp"], df_temp["wind"], df_temp["fixedgen"]))
        return df_temp["PV_Bat_EV_HP_Wind_Fix"]

    def get_producers(self, df_results) -> tuple:
        """
        TODO: Create function that gives out the user ids of the producers to exclude them in plots
        """
        # print(df_results.to_string())
        pass

    def __load_config(self) -> dict:
        """loads the config file

        Args:

        Returns:
            dict that contains the config file of the scenario

        """

        with open(f"{self.path_results}/config.yaml") as config_file:
            config = self.yaml.load(config_file)

        return config

    def __get_table_columns(self, table_name) -> list:
        """returns a list containing the names of all columns of the provided table

        Args:
            table_name: string with the name of the table

        Returns:
            list_columns: list containing the names of all table columns

        """

        table = [table for table in self.db_conn.list_tables if table.name == table_name][0]
        _list_columns = []
        for column in table.list_columns:
            _list_columns.append(column.name)

        return _list_columns

    def __save_figure(self, name) -> None:
        """saves the current plot under the provided name as png

        Args:
            name: string that contains the name of the plot

        Returns:
            None

        """

        # Save file to analyzer directory. If file with the name already exists, replace it with the new file
        if os.path.isfile(f"{self.path_analyzer}/{name}.png"):
            os.remove(f"{self.path_analyzer}/{name}.png")
        plt.savefig(f"{self.path_analyzer}/{name}.png")

    @staticmethod
    def change_table_height(table, multiplier=1.2) -> None:
        """adjusts the cell height of the provided table by the mulitplier value

        Args:
            table: table that is to be changed
            multiplier: multiplication factor to adjust the table height

        Returns:
            None

        """

        [cell.set_height(cell.get_height() * multiplier) for cell in table.properties()["children"]]

    @staticmethod
    def create_checkmarks(source) -> object:
        """saves the current plot under the provided name as png

        Args:
            source: string that contains the name of the plot

        Returns:
            cell_texts: object that contains the table of checkmarks

        """

        # Transpose list and replace the booleans with checkmarks and blanks
        cell_texts = np.array(list(map(list, zip(*source.tolist())))).tolist()
        for idxs, elems in enumerate(cell_texts):
            for idx, elem in enumerate(elems):
                cell_texts[idxs][idx] = "✓" * elem

        return cell_texts


class ScenarioPlotter:
    """
    A class used to create a uniform look for the plots
    ...

    Attributes
    ----------
    fig : figure
        figure that contains the plots
    ax : axis
        primary axis
    ax2 : axis
        secondary axis (optional)

    Public Methods
    -------
    __init__() -> None
        initializer, requires no input
    figure_setup(title: str = "", xlabel: str = "", ylabel: str = "", ylabel_right: str = None,
                 legend_labels: tuple = (), xlims: list = None, xticks_style: str = None) -> None
        sets up the default figure configurations to give the plots a uniform look
    """

    def __init__(self):
        """initializer

        Args:

        Returns:

        """

        # Style settings
        path_class_defn = os.path.dirname(__file__)
        path_plotstyle = os.path.join(path_class_defn, 'lemlab_plots.mplstyle')
        plt.style.use(path_plotstyle)

        # Plots
        self.fig, self.ax = plt.subplots()
        self.ax2 = None

    def figure_setup(self, title: str = "", xlabel: str = "", ylabel: str = "", ylabel_right: str = None,
                     legend_labels: tuple = (), xlims: list = None, xticks_style: str = None) -> None:
        """sets up the default figure configurations to give the plots a uniform look

        Args:
            title: string that contains the title of the figure
            xlabel: string that contains the x-label of the figure
            ylabel: string that contains the left y-label of the figure
            ylabel_right: string that contains the right y-label of the figure if figures has two y-axes
            legend_labels: string that contains the legend labels of the figure
            xlims: list that contains the minimum and maximum value of the x-axis
            xticks_style: string that contains the style of the x-ticks. Currently three styles are available:
                            1. None:    No x-ticks are displayed
                            2. numeric: X-axis is displayed with numeric values
                            3. date:    X-axis is displayed with dates

        Returns:

        """

        # Title settings
        plt.title(title)

        # Axes settings
        self.ax.set_ylabel(ylabel=ylabel)
        self.ax.set_xlabel(xlabel=xlabel)
        # checks if x-axis needs to be adjusted in case it differs from the standard output
        self.__set_xticks(xlims, xticks_style)
        self.ax.tick_params(axis='both', which='major')

        # Legend
        if 0 < len(legend_labels) <= 5:  # change labels of legend, if desired
            self.ax.legend(legend_labels, bbox_to_anchor=(0.5, -0.2-(0.1*int(len(legend_labels)/4))), ncol=4)
        elif len(legend_labels) > 5:
            # legend_labels = legend_labels
            self.ax.legend(legend_labels, bbox_to_anchor=(0.5, -0.2-(0.1*int(len(legend_labels)/4))),
                           ncol=min(4, math.ceil(round(len(legend_labels)/2))))

        # Scale x-axis tightly
        self.ax.autoscale(enable=True, axis='x', tight=True)
        #self.ax.set_ylim(bottom=-70, top=90)
        # Adjust second y-axis if it exists
        if ylabel_right:
            self.ax2.set_ylabel(ylabel=ylabel_right)
            self.ax2.set_xlabel(xlabel=xlabel)
            self.ax2.tick_params(axis='both', which='major')
            self.ax2.autoscale(enable=True, axis='x', tight=True)
            self.ax.grid(b=False)

        plt.tight_layout()

    @staticmethod
    def __set_xticks(xlims: list, xticks_style: str = None) -> None:
        """sets the x-ticks labels according to the chosen style

        Args:
            xlims: list that contains the minimum and maximum value of the x-axis
            xticks_style: string that contains the style of the x-ticks. Currently three styles are available:
                            1. None:    No x-ticks are displayed
                            2. numeric: X-axis is displayed with numeric values
                            3. date:    X-axis is displayed with dates

        Returns:

        """

        # maximum number of x-ticks
        n_max = 10

        if not xticks_style:
            # turn x-ticks off
            plt.xticks([])
        elif xticks_style.lower() == "numeric":
            # create numeric values for entire range
            x_values = list(range(int(xlims[0]), int(xlims[1] + 1)))
            # reduce list of x-values by half until maximum number of x-ticks is reached
            while len(x_values) > n_max:
                x_values = x_values[::2]
            plt.xticks(x_values, x_values)
        elif xticks_style.lower() == "date":
            time_step = 1 * 15 * 60  # time step of 15 minutes
            x_values = xlims[0] - xlims[0] % time_step  # ensure that first value is a multiple of time_step
            x_values = list(range(x_values, xlims[1] + time_step, time_step))

            # Get step size depending on duration of simulation
            x_step = [x for x in [1, 2, 4, 8, 16, 24, 96, 2*96, 4*96, 5*96]
                      if len(x_values) / x <= n_max][0]

            # Get x-values. If they exceed the 5-day threshold (5*96), dates will be displayed only for the first and
            # 15th of each month
            if x_step:
                x_values = x_values[::x_step]
            else:
                x_values = [x for x in x_values if datetime.fromtimestamp(x).strftime("%H") == "00" and
                            (datetime.fromtimestamp(x).strftime("%d") == "01" or
                             datetime.fromtimestamp(x).strftime("%d") == "15")]

            # Change date style based on duration
            if x_step < 4:
                xformat = "%H:%M"
                xformat2 = "%d.%m"
            elif x_step < 96:
                xformat = "%Hh"
                xformat2 = "%d.%m"
            else:
                xformat = "%d"
                xformat2 = "%b"

            # Create labels and improve readability
            x_labels = [datetime.fromtimestamp(x).strftime(xformat) for x in x_values]
            x_labels_adj = x_labels.copy()
            for idx, x in enumerate(x_values[2:]):
                if idx == 0:
                    x_labels_adj[idx] = datetime.fromtimestamp(x_values[idx]).strftime(xformat2)
                if x_labels[idx+2][:2] < x_labels[idx+1][:2]:
                    x_labels_adj[idx+2] = datetime.fromtimestamp(x).strftime(xformat2)
            plt.xticks(x_values, x_labels_adj)
