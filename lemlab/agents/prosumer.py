__author__ = "sdlumpp"
__credits__ = []
__license__ = ""
__maintainer__ = "sdlumpp"
__email__ = "sebastian.lumpp@tum.de"

import json
import datetime
import feather as ft
import pandas as pd
import numpy as np
from pyomo import environ as pyo
from typing import Union
from random import random
from lemlab.utilities.forecasting import ForecastManager
from bisect import bisect_left
import os
import matplotlib.pyplot as plt
import seaborn as sns


class Prosumer:
    """Prosumer defines objects and methods used to simulate a single family home in a local energy market

       This class is intended for use in conjunction with the Retailer and Simulation classes. In addition, the
       functionality of the Platform/clearing_ex_ante.py module is required in order for market clearing to be enacted.

       A single family home can simulate the following physical plants, depending on the selected configuration:
                        1)  Fixed household loads with or without electric vehicles
                        2)  PV plants with or without battery storage
                        3)  Market agents for trading in double-sided electricity markets

       Instances of the Prosumer class are created and destroyed once each Simulation step.

        Public methods:

        __init__ :              Create an instance of the Prosumer class from a configuration folder created using the
                                Scenario_Executor class
        pre_clearing_activity:  Perform all activities required before market clearing. This includes:
                                    real time controller execution
                                    logging of metering values
                                    forecasting
                                    predictive control
                                    market trading

        post_clearing_activity: Performs all activities required after market clearing, mainly:
                                    retrieving market results
                                    determining controller setpoints
    """

    def __init__(self, path, t_override=None, df_weather_history=None, df_weather_fcast=None, count=None):
        """Create a Prosumer instance from a configuration folder created using the Simulation class.

        :param path: path to prosumer configuration directory
        :param t_override: pandas Timestamp, if supplied, this parameter forces the MP to use the supplied
                            timestamp, otherwise the current time is used.
        """
        # set current timestamp from system clock or keyword arg
        self.base_quantiles_first = None
        self.t_now = t_override if t_override else pd.Timestamp.now().timestamp()
        # derive previous and next timestamps
        self.ts_delivery_prev = round(pd.Timestamp(self.t_now, unit="s").floor("15min").timestamp() - 15 * 60)
        self.ts_delivery_current = self.ts_delivery_prev + 15 * 60
        self.path = path
        self.path_out = os.path.join(os.getcwd(), r'csv')
        if not os.path.exists(self.path_out):
            os.makedirs(self.path_out)
        self.path_plot = os.path.join(os.getcwd(), r'plot')
        if not os.path.exists(self.path_plot):
            os.makedirs(self.path_plot)

        with open(f"{self.path}/config_account.json", "r") as read_file:
            self.config_dict = json.load(read_file)

        with open(f"{self.path}/config_plants.json", "r") as read_file:
            self.plant_dict = json.load(read_file)

        self.meas_val = {"timestamp": self.ts_delivery_prev}

        self.df_weather_history = df_weather_history
        self.df_weather_fcast = df_weather_fcast
        # only if forecasts are required
        self.fcast_manager = ForecastManager(self)

        # initialize instance dataframes to be used in later methods
        # df containing all time series forecasts for MPC and market agents
        self.fcast_table = None
        # df containing all MPC inputs and results for the MP
        self.mpc_table = None
        # df containing matched market results for the MP
        self.matched_bids = None
        # df containing net matched market volumes by timestep (multiple matched offers for each timestamp summated)
        self.matched_bids_by_timestep = None
        self.count = count
        self.base_quantiles = None
        self.save_output_result = False

    def pre_clearing_activity(self, db_obj, clear_positions=False):
        self.update_user_preferences(db_obj)
        self.controller_real_time()
        self.log_meter_readings(db_obj=db_obj)

        # if the user considers the future in any way, the controller strategy will contain the component "mpc"
        if "mpc" in self.config_dict["controller_strategy"]:
            # set market type currently running:
            market_type = "ex_ante" if db_obj.lem_config["types_clearing_ex_ante"] else "ex_post"

            # get most recent market results, update price history
            self.get_market_results(market_type=market_type,
                                    db_obj=db_obj)
            # update forecasts for all plants, retrain if necessary
            self.fcast_manager.update_forecasts()

            # if self.config_dict["id_user"] == "0000000021":
            #     # self.controller_model_predictive_test()
            #     self.mpc_qtl()
            # else:
            #     # execute model predictive control
            #     self.controller_model_predictive()

            # execute model predictive control
            self.controller_model_predictive()

            # # finally then, execute market agent if ex-ante market
            # self.market_agent(db_obj=db_obj, clear_positions=clear_positions)

            if db_obj.lem_config["types_clearing_ex_ante"]:
                self.market_agent(db_obj=db_obj, clear_positions=clear_positions)

                # if self.config_dict["id_user"] == "0000000021":
                #     # self.market_agent_test(db_obj=db_obj, clear_positions=clear_positions)
                #     # self.plot_graph_before()
                #     self.market_agent_qtl(db_obj=db_obj, clear_positions=clear_positions)
                # else:
                #     self.market_agent(db_obj=db_obj, clear_positions=clear_positions)

    def post_clearing_activity(self, db_obj):
        market_type = "ex_ante" if db_obj.lem_config["types_clearing_ex_ante"] else "ex_post"
        # if the user
        if "mpc" in self.config_dict["controller_strategy"]:
            self.get_market_results(market_type=market_type,
                                    db_obj=db_obj)
        # if the user
        if "mpc" in self.config_dict["controller_strategy"]:
            self.set_target_grid_power(market_type)

        # if self.config_dict["id_user"] == "0000000001":
        #     self.plot_graph_after()

    # internal functions

    def add_c_pv_rtc(self, rtc_model):
        # pv variables
        rtc_model.p_pv = pyo.Var(self._get_list_plants(plant_type="pv"),
                                 domain=pyo.NonNegativeReals)

        # pv maximum power constraint
        def pv_rule(_model, _plant):
            p_max = ft.read_dataframe(f"{self.path}/raw_data_{_plant}.ft", columns=["timestamp", "power"]
                                      ).set_index("timestamp")
            p_max = p_max.loc[self.ts_delivery_prev, "power"]
            p_max *= self.plant_dict[_plant]["power"]
            if self.plant_dict[_plant].get("controllable"):
                return _model.p_pv[_plant] <= p_max
            return _model.p_pv[_plant] == p_max

        if self._get_list_plants(plant_type="pv"):
            rtc_model.con_pv = pyo.Constraint(self._get_list_plants(plant_type="pv"),
                                              rule=pv_rule)

    def add_c_wind_rtc(self, rtc_model):
        # pv variables
        rtc_model.p_wind = pyo.Var(self._get_list_plants(plant_type="wind"),
                                   domain=pyo.NonNegativeReals)

        current_wind_speed = float(self.df_weather_history.loc[self.ts_delivery_prev, "wind_speed"])

        # wind maximum power constraint
        def wind_rule(_model, _plant):

            with open(f"{self.path}/spec_{_plant}.json") as read_file:
                spec_file = json.load(read_file)

            lookup_wind_speed = spec_file["wind_speed_m/s"]
            lookup_power = spec_file["power_pu"]

            p_max = self._lookup(current_wind_speed, lookup_wind_speed, lookup_power) * self.plant_dict[_plant]["power"]

            if self.plant_dict[_plant].get("controllable"):
                return _model.p_wind[_plant] <= p_max

            return _model.p_wind[_plant] == p_max

        if self._get_list_plants(plant_type="wind"):
            rtc_model.con_wind = pyo.Constraint(self._get_list_plants(plant_type="wind"),
                                                rule=wind_rule)

    def add_c_fixedgen_rtc(self, rtc_model):
        # fixedgen decision variables
        rtc_model.p_fixedgen = pyo.Var(self._get_list_plants(plant_type="fixedgen"),
                                       domain=pyo.NonNegativeReals)

        def fixedgen_rule(_model, _plant):
            p_max = ft.read_dataframe(f"{self.path}/raw_data_{_plant}.ft").set_index("timestamp")
            p_max = p_max.loc[self.ts_delivery_prev, "power"]
            p_max *= self.plant_dict[_plant]["power"]
            if self.plant_dict[_plant].get("controllable"):
                return _model.p_fixedgen[_plant] <= p_max
            return _model.p_fixedgen[_plant] == p_max

        if self._get_list_plants(plant_type="fixedgen"):
            rtc_model.con_fixedgen = pyo.Constraint(self._get_list_plants(plant_type="fixedgen"),
                                                    rule=fixedgen_rule)

    def add_c_hp_rtc(self, rtc_model):
        # hp variables
        rtc_model.p_hp = pyo.Var(self._get_list_plants(plant_type="hp"),
                                 domain=pyo.NonPositiveReals)

        rtc_model.q_hp = pyo.Var(self._get_list_plants(plant_type="hp"),
                                 domain=pyo.NonNegativeReals)

        # rtc_model.p_hp_milp = pyo.Var(self._get_list_plants(plant_type="hp"), domain=pyo.Binary)
        res_hp_dict = {}

        for _plant in self._get_list_plants(plant_type="hp"):
            temp_amb = float(self.df_weather_history.loc[self.ts_delivery_prev, "temp"]) - 273.15
            hp_param = pd.read_json(f"{self.path}/spec_{_plant}.json")
            heatpump = HeatPump(hp_param)
            t_in_secondary = self.plant_dict[_plant]["temperature"] - 5
            hp_sim_res = heatpump.simulate(t_in_primary=temp_amb, t_in_secondary=t_in_secondary,
                                           t_amb=temp_amb, mode=1)
            res_hp_dict[_plant] = hp_sim_res

        # hp maximum power constraint
        def hp_p_rule(_model, _plant):
            # return _model.p_hp[_plant] == res_hp_dict[_plant]['P_el'] * (-1) * rtc_model.p_hp_milp[_plant]
            return _model.p_hp[_plant] >= res_hp_dict[_plant]['P_el'] * (-1)

        if self._get_list_plants(plant_type="hp"):
            rtc_model.con_hp_p = pyo.Constraint(self._get_list_plants(plant_type="hp"),
                                                rule=hp_p_rule)

        def hp_q_rule(_model, _plant):
            return _model.q_hp[_plant] == -1 * _model.p_hp[_plant] * res_hp_dict[_plant]['COP']

        if self._get_list_plants(plant_type="hp"):
            rtc_model.con_hp_q = pyo.Constraint(self._get_list_plants(plant_type="hp"),
                                                rule=hp_q_rule)

        # constraints for thermal energy storage
        # tes decision variables
        rtc_model.q_tes_in = pyo.Var(self._get_list_plants(plant_type="hp"), domain=pyo.NonNegativeReals)
        rtc_model.q_tes_out = pyo.Var(self._get_list_plants(plant_type="hp"), domain=pyo.NonNegativeReals)
        rtc_model.q_tes_milp = pyo.Var(self._get_list_plants(plant_type="hp"), domain=pyo.Binary)

        # else set battery power to zero
        dict_soc_old = {}
        dict_capacity_wh = {}
        dict_power_th = {}
        rtc_model.n_tes = {}

        for hp in self._get_list_plants(plant_type="hp"):
            with open(f"{self.path}/soc_{hp}.json", "r") as read_file:
                dict_soc_old[hp] = json.load(read_file)
            dict_capacity_wh[hp] = self.plant_dict[hp]['capacity']
            dict_power_th[hp] = self.plant_dict[hp]['power_th']
            rtc_model.n_tes[hp] = self.plant_dict[hp]["efficiency"]
            rtc_model.q_tes_in[hp].setub(float(dict_power_th[hp]))
            rtc_model.q_tes_out[hp].setub(float(dict_power_th[hp]))

        def tes_soc_rule_1(_model, _hp):
            return (dict_soc_old[_hp] - 0.25 * _model.q_tes_out[_hp] / _model.n_tes[_hp]
                    + 0.25 * _model.q_tes_in[_hp] * _model.n_tes[_hp]
                    <= float(dict_capacity_wh[_hp]))

        def tes_soc_rule_2(_model, _hp):
            return (dict_soc_old[_hp] - 0.25 * _model.q_tes_out[_hp] / _model.n_tes[_hp]
                    + 0.25 * _model.q_tes_in[_hp] * _model.n_tes[_hp] >= 0)

        def tes_bin_rule_minus(_model, _hp):
            return _model.q_tes_in[_hp] <= 100000 * (1 - _model.q_tes_milp[_hp])

        def tes_bin_rule_plus(_model, _hp):
            return _model.q_tes_out[_hp] <= 100000 * _model.q_tes_milp[_hp]

        rtc_model.tes_soc_1 = pyo.Constraint(self._get_list_plants(plant_type="hp"), rule=tes_soc_rule_1)
        rtc_model.tes_soc_2 = pyo.Constraint(self._get_list_plants(plant_type="hp"), rule=tes_soc_rule_2)
        rtc_model.tes_bin_minus = pyo.Constraint(self._get_list_plants(plant_type="hp"), rule=tes_bin_rule_minus)
        rtc_model.tes_bin_plus = pyo.Constraint(self._get_list_plants(plant_type="hp"), rule=tes_bin_rule_plus)

    def add_c_bat_rtc(self, rtc_model, df_target_grid_power):
        if self._get_list_plants(plant_type="bat"):
            # battery decision variables
            rtc_model.p_bat_in = pyo.Var(self._get_list_plants(plant_type="bat"), domain=pyo.NonNegativeReals)
            rtc_model.p_bat_out = pyo.Var(self._get_list_plants(plant_type="bat"), domain=pyo.NonNegativeReals)
            rtc_model.p_bat_milp = pyo.Var(self._get_list_plants(plant_type="bat"), domain=pyo.Binary)
            rtc_model.deviation_bat_plus = pyo.Var(self._get_list_plants(plant_type="bat"),
                                                   domain=pyo.NonNegativeReals)
            rtc_model.deviation_bat_minus = pyo.Var(self._get_list_plants(plant_type="bat"),
                                                    domain=pyo.NonNegativeReals)

            # else set battery power to zero
            dict_soc_old = {}
            rtc_model.n_bat = {}
            rtc_model.con_bat_dev = pyo.ConstraintList()

            for bat in self._get_list_plants(plant_type="bat"):
                with open(f"{self.path}/soc_{bat}.json", "r") as read_file:
                    dict_soc_old[bat] = json.load(read_file)
                rtc_model.n_bat[bat] = self.plant_dict[bat]["efficiency"]

                rtc_model.p_bat_in[bat].setub(self.plant_dict[bat]["power"])
                rtc_model.p_bat_out[bat].setub(self.plant_dict[bat]["power"])
                rtc_model.con_bat_dev.add(expr=rtc_model.p_bat_out[bat] - rtc_model.p_bat_in[bat]
                                      == df_target_grid_power[f"power_{bat}"]
                                      - (rtc_model.deviation_bat_plus[bat] - rtc_model.deviation_bat_minus[bat]))

            def bat_soc_rule_1(_model, _bat):
                return (dict_soc_old[_bat] - 0.25 * _model.p_bat_out[_bat] / _model.n_bat[_bat]
                        + 0.25 * _model.p_bat_in[_bat] * _model.n_bat[_bat]
                        <= self.plant_dict[_bat].get("capacity"))

            def bat_soc_rule_2(_model, _bat):
                return (dict_soc_old[_bat] - 0.25 * _model.p_bat_out[_bat] / _model.n_bat[_bat]
                        + 0.25 * _model.p_bat_in[_bat] * _model.n_bat[_bat] >= 0)

            def bat_bin_rule_minus(_model, _bat):
                return _model.p_bat_in[_bat] <= 100000 * (1 - _model.p_bat_milp[_bat])

            def bat_bin_rule_plus(_model, _bat):
                return _model.p_bat_out[_bat] <= 100000 * _model.p_bat_milp[_bat]

            rtc_model.bat_soc_1 = pyo.Constraint(self._get_list_plants(plant_type="bat"), rule=bat_soc_rule_1)
            rtc_model.bat_soc_2 = pyo.Constraint(self._get_list_plants(plant_type="bat"), rule=bat_soc_rule_2)
            rtc_model.bat_bin_minus = pyo.Constraint(self._get_list_plants(plant_type="bat"), rule=bat_bin_rule_minus)
            rtc_model.bat_bin_plus = pyo.Constraint(self._get_list_plants(plant_type="bat"), rule=bat_bin_rule_plus)

            # limit battery charging to pv generation
            rtc_model.con_batt_charge_grid = pyo.ConstraintList()
            make_const = 0
            expr_left = 0
            expr_right = 0
            for bat in self._get_list_plants(plant_type="bat"):
                if not self.plant_dict[bat].get("charge_from_grid"):
                    expr_left += rtc_model.p_bat_in[bat]
                    make_const = 1
            for pv in self._get_list_plants(plant_type="pv"):
                expr_right += rtc_model.p_pv[pv]
            if make_const:
                rtc_model.con_batt_charge_grid.add(expr=(expr_left <= expr_right))

    def add_c_ev_rtc(self, rtc_model, df_target_grid_power):
        # ev decision variables
        if self._get_list_plants(plant_type="ev"):
            rtc_model.p_ev_in = pyo.Var(self._get_list_plants(plant_type="ev"), domain=pyo.NonNegativeReals)
            rtc_model.p_ev_out = pyo.Var(self._get_list_plants(plant_type="ev"), domain=pyo.NonNegativeReals)
            rtc_model.p_ev_milp = pyo.Var(self._get_list_plants(plant_type="ev"), domain=pyo.Binary)
            rtc_model.deviation_ev_plus = pyo.Var(self._get_list_plants(plant_type="ev"), domain=pyo.NonNegativeReals)
            rtc_model.deviation_ev_minus = pyo.Var(self._get_list_plants(plant_type="ev"), domain=pyo.NonNegativeReals)
            rtc_model.dev_ev_milp = pyo.Var(self._get_list_plants(plant_type="ev"), domain=pyo.Binary)

            rtc_model.con_ev_soc = pyo.ConstraintList()
            rtc_model.con_ev_milp = pyo.ConstraintList()
            rtc_model.con_ev_dev = pyo.ConstraintList()

            rtc_model.ev_soc_old = {}
            for ev in self._get_list_plants(plant_type="ev"):
                rtc_model.p_ev_out[ev].setub(0)
                rtc_model.p_ev_in[ev].setub(0)
                raw_data_ev = ft.read_dataframe(f"{self.path}/raw_data_{ev}.ft")
                raw_data_ev.set_index("timestamp", inplace=True)
                raw_data_ev = raw_data_ev[raw_data_ev.index == self.ts_delivery_prev]
                raw_data_ev = dict(raw_data_ev.loc[self.ts_delivery_prev])
                if raw_data_ev["availability"] == 1:
                    rtc_model.p_ev_in[ev].setub(self.plant_dict[ev]["charging_power"])
                    if self.plant_dict[ev].get("v2g"):
                        rtc_model.p_ev_out[ev].setub(self.plant_dict[ev]["charging_power"])

                with open(f"{self.path}/soc_{ev}.json", "r") as read_file:
                    rtc_model.ev_soc_old[ev] = max(0.05 * self.plant_dict[ev]["capacity"],
                                                   json.load(read_file) - raw_data_ev["distance_driven"] / 100
                                                   * self.plant_dict[ev]["consumption"])

                n_ev = self.plant_dict[ev]["efficiency"]

                if raw_data_ev["availability"] == 1:
                    rtc_model.p_ev_in[ev].setub(self.plant_dict[ev]["charging_power"])
                    if self.plant_dict[ev].get("v2g"):
                        rtc_model.p_ev_out[ev].setub(self.plant_dict[ev]["charging_power"])

                    rtc_model.con_ev_soc.add(expr=rtc_model.ev_soc_old[ev]
                                                  - 0.25 * rtc_model.p_ev_out[ev] / n_ev
                                                  + 0.25 * rtc_model.p_ev_in[ev] * n_ev
                                                  <= self.plant_dict[ev].get("capacity"))

                    rtc_model.con_ev_soc.add(expr=rtc_model.ev_soc_old[ev]
                                                  - 0.25 * rtc_model.p_ev_out[ev] / n_ev
                                                  + 0.25 * rtc_model.p_ev_in[ev] * n_ev
                                                  >= df_target_grid_power[f"soc_min_{ev}"])

                    rtc_model.con_ev_milp.add(expr=rtc_model.p_ev_out[ev]
                                                   <= 1000000 * rtc_model.p_ev_milp[ev])

                    rtc_model.con_ev_milp.add(expr=rtc_model.p_ev_in[ev]
                                                   <= 1000000 * (1 - rtc_model.p_ev_milp[ev]))

                    rtc_model.con_ev_milp.add(expr=rtc_model.deviation_ev_plus[ev]
                                                   <= 1000000 * rtc_model.dev_ev_milp[ev])

                    rtc_model.con_ev_milp.add(expr=rtc_model.deviation_ev_minus[ev]
                                                   <= 1000000 * (1 - rtc_model.dev_ev_milp[ev]))

                    rtc_model.con_ev_dev.add(expr=rtc_model.p_ev_out[ev] - rtc_model.p_ev_in[ev]
                                                  == df_target_grid_power[f"power_{ev}"]
                                                  - (rtc_model.deviation_ev_plus[ev]
                                                     - rtc_model.deviation_ev_minus[ev]))

    def add_p_fix_load(self, rtc_model):
        # fixedgen load consumption, sum of household loads
        p_load = float(0)
        for hh in self._get_list_plants(plant_type="hh"):
            p_meas = ft.read_dataframe(f"{self.path}/raw_data_{hh}.ft")
            p_meas.set_index("timestamp", inplace=True)
            p_meas = float(p_meas[p_meas.index == self.ts_delivery_prev]["power"].values)
            p_load += float(p_meas)
        rtc_model.p_load_fix = p_load

    def add_q_fix_load(self, rtc_model):
        # fixed hosehould thermal load
        q_load = float(0)
        for hp in self._get_list_plants(plant_type="hp"):
            q_meas = ft.read_dataframe(f"{self.path}/raw_data_{hp}.ft")
            q_meas.set_index("timestamp", inplace=True)
            q_meas = float(q_meas[q_meas.index == self.ts_delivery_prev]["heat"].values)
            q_load += float(q_meas)
        rtc_model.q_load_fix = q_load

    def add_c_balance_rtc(self, rtc_model, df_target_grid_power):

        # deviation from setpoint (grid fee-in), absolute components
        rtc_model.deviation_gr_plus = pyo.Var(domain=pyo.NonNegativeReals)
        rtc_model.deviation_gr_minus = pyo.Var(domain=pyo.NonNegativeReals)

        # declare heat balancing constraint
        def balance_heat_rule(_model):
            expression_right = 0
            expression_left = _model.q_load_fix
            for _hp in self._get_list_plants(plant_type="hp"):
                expression_left += _model.q_tes_out[_hp] - _model.q_tes_in[_hp] + _model.q_hp[_hp]
            return expression_left == expression_right

        if self._get_list_plants(plant_type="hp"):
            rtc_model.con_heat_balance = pyo.Constraint(rule=balance_heat_rule)

        # declare balancing constraint, same for all controllers
        def balance_rule(_model):
            expression_left = _model.p_load_fix
            for _fixedgen in self._get_list_plants(plant_type="fixedgen"):
                expression_left += _model.p_fixedgen[_fixedgen]
            for _pv in self._get_list_plants(plant_type="pv"):
                expression_left += _model.p_pv[_pv]
            for _wind in self._get_list_plants(plant_type="wind"):
                expression_left += _model.p_wind[_wind]
            for _bat in self._get_list_plants(plant_type="bat"):
                expression_left += _model.p_bat_out[_bat] - _model.p_bat_in[_bat]
            for _hp in self._get_list_plants(plant_type="hp"):
                expression_left += _model.p_hp[_hp]
            for _ev in self._get_list_plants(plant_type="ev"):
                expression_left += _model.p_ev_out[_ev] - _model.p_ev_in[_ev]
            expression_right = float(df_target_grid_power[f"power_{self.config_dict['id_meter_grid']}"])
            expression_right -= _model.deviation_gr_plus - _model.deviation_gr_minus
            return expression_left == expression_right

        rtc_model.con_balance = pyo.Constraint(rule=balance_rule)

    def add_obj_rtc(self, rtc_model):
        # declare objective function, same for all controllers
        # _                             component 1: minimize deviation from target power (0 for self-consumption)
        # _                                          mutual exclusion of absolute components
        def obj_rule(_model):
            obj = 0.5 * (_model.deviation_gr_plus + _model.deviation_gr_minus)
            for _bat in self._get_list_plants(plant_type="bat"):
                obj += 0.1 * _model.deviation_bat_minus[_bat]
            for _ev in self._get_list_plants(plant_type="ev"):
                obj += _model.deviation_ev_minus[_ev]
            return obj

        rtc_model.objective_fun = pyo.Objective(rule=obj_rule, sense=pyo.minimize)

    def get_result_rtc(self, rtc_model):
        meas_grid = 0
        for hh in self._get_list_plants(plant_type="hh"):
            p_meas = ft.read_dataframe(f"{self.path}/raw_data_{hh}.ft")
            p_meas.set_index("timestamp", inplace=True)
            self.meas_val[hh] = float(p_meas[p_meas.index == self.ts_delivery_prev]["power"].values)
            meas_grid += self.meas_val[hh]
        for pv in self._get_list_plants(plant_type="pv"):
            self.meas_val[pv] = rtc_model.p_pv[pv].value
            meas_grid += rtc_model.p_pv[pv].value
        for wind in self._get_list_plants(plant_type="wind"):
            self.meas_val[wind] = rtc_model.p_wind[wind].value
            meas_grid += rtc_model.p_wind[wind].value
        for fixedgen in self._get_list_plants(plant_type="fixedgen"):
            self.meas_val[fixedgen] = rtc_model.p_fixedgen[fixedgen].value
            meas_grid += rtc_model.p_fixedgen[fixedgen].value
        for hp in self._get_list_plants(plant_type="hp"):
            self.meas_val[hp] = rtc_model.p_hp[hp].value
            meas_grid += rtc_model.p_hp[hp].value
            with open(f"{self.path}/soc_{hp}.json", "r") as read_file:
                dict_soc_old = json.load(read_file)
            tes_soc_new = dict_soc_old \
                          - 0.25 * rtc_model.q_tes_out[hp].value / self.plant_dict[hp]["efficiency"] \
                          + 0.25 * rtc_model.q_tes_in[hp].value * self.plant_dict[hp]["efficiency"]
            with open(f"{self.path}/soc_{hp}.json", "w") as write_file:
                json.dump(tes_soc_new, write_file)
        for bat in self._get_list_plants(plant_type="bat"):
            self.meas_val[bat] = rtc_model.p_bat_out[bat].value - rtc_model.p_bat_in[bat].value
            meas_grid += rtc_model.p_bat_out[bat].value - rtc_model.p_bat_in[bat].value
            with open(f"{self.path}/soc_{bat}.json", "r") as read_file:
                dict_soc_old = json.load(read_file)
            bat_soc_new = dict_soc_old \
                          - 0.25 * rtc_model.p_bat_out[bat].value / self.plant_dict[bat]["efficiency"] \
                          + 0.25 * rtc_model.p_bat_in[bat].value * self.plant_dict[bat]["efficiency"]
            with open(f"{self.path}/soc_{bat}.json", "w") as write_file:
                json.dump(bat_soc_new, write_file)

        for ev in self._get_list_plants(plant_type="ev"):
            self.meas_val[ev] = rtc_model.p_ev_out[ev].value - rtc_model.p_ev_in[ev].value
            meas_grid += rtc_model.p_ev_out[ev].value - rtc_model.p_ev_in[ev].value
            ev_soc_new = rtc_model.ev_soc_old[ev] \
                         - 0.25 * rtc_model.p_ev_out[ev].value / self.plant_dict[ev]["efficiency"] \
                         + 0.25 * rtc_model.p_ev_in[ev].value * self.plant_dict[ev]["efficiency"]
            with open(f"{self.path}/soc_{ev}.json", "w") as write_file:
                json.dump(ev_soc_new, write_file)

        self.meas_val[self.config_dict['id_meter_grid']] = int(meas_grid)

    def controller_real_time(self):
        """Calculate the behaviour of the instance in the previous time step by applying a selected
        controller_real_time strategy to measurement data and plant specifications. Output is supplied in the form of a
        .json file saved to the user's folder, so that execution can be parallelized.

        :return: None
        """
        # if the MPC and market results are active, they provide setpoints that the real time controller must stick to
        if self.config_dict["controller_strategy"] == "mpc_opt":
            df_target_grid_power = (ft.read_dataframe(f"{self.path}/target_grid_power.ft")
                                    .pipe(pd.DataFrame.set_index, keys="timestamp")
                                    ).loc[self.ts_delivery_prev]
            # non-default controllers common model parameters initialized here.

            rtc_model = pyo.ConcreteModel()

            self.add_p_fix_load(rtc_model)

            self.add_q_fix_load(rtc_model)

            self.add_c_pv_rtc(rtc_model)

            self.add_c_wind_rtc(rtc_model)

            self.add_c_fixedgen_rtc(rtc_model)

            self.add_c_hp_rtc(rtc_model)

            self.add_c_bat_rtc(rtc_model, df_target_grid_power)

            self.add_c_ev_rtc(rtc_model, df_target_grid_power)

            self.add_c_balance_rtc(rtc_model, df_target_grid_power)

            self.add_obj_rtc(rtc_model)

            # solve model
            pyo.SolverFactory(self.config_dict["solver"]).solve(rtc_model)

            # assign results to instance variables for logging
            self.get_result_rtc(rtc_model)
        # if no forecasting and model predictive control are applied, a simple rule-based control strategy can be run
        elif "rtc" in self.config_dict["controller_strategy"]:
            meas_grid = 0
            # household load is simply logged from raw data
            for hh in self._get_list_plants(plant_type="hh"):
                p_meas = ft.read_dataframe(f"{self.path}/raw_data_{hh}.ft").set_index("timestamp")
                self.meas_val[hh] = float(p_meas[p_meas.index == self.ts_delivery_prev]["power"].values)
                meas_grid += self.meas_val[hh]
            # generators feed in maximum power at all times.
            for pv in self._get_list_plants(plant_type="pv"):
                p_max = ft.read_dataframe(f"{self.path}/raw_data_{pv}.ft", columns=["timestamp", "power"]
                                          ).set_index("timestamp")
                p_max = p_max.loc[self.ts_delivery_prev, "power"]
                self.meas_val[pv] = p_max * self.plant_dict[pv]["power"]
                meas_grid += self.meas_val[pv]
            for wind in self._get_list_plants(plant_type="wind"):
                current_wind_speed = float(self.df_weather_history.loc[self.ts_delivery_prev, "wind_speed"])
                with open(f"{self.path}/spec_{wind}.json") as read_file:
                    spec_file = json.load(read_file)
                lookup_wind_speed = spec_file["wind_speed_m/s"]
                lookup_power = spec_file["power_pu"]
                self.meas_val[wind] = self._lookup(current_wind_speed, lookup_wind_speed, lookup_power) * \
                                      self.plant_dict[wind]["power"]
                meas_grid += self.meas_val[wind]
            for fixedgen in self._get_list_plants(plant_type="fixedgen"):
                p_max = ft.read_dataframe(f"{self.path}/raw_data_{fixedgen}.ft", columns=["timestamp", "power"]
                                          ).set_index("timestamp")
                p_max = p_max.loc[self.ts_delivery_prev, "power"]
                self.meas_val[fixedgen] = float(p_max * self.plant_dict[fixedgen]["power"])

                meas_grid += p_max * self.plant_dict[fixedgen]["power"]
            # the electric vehicle charges at maximum power upon arrival
            for ev in self._get_list_plants(plant_type="ev"):
                raw_data_ev = ft.read_dataframe(f"{self.path}/raw_data_{ev}.ft").set_index("timestamp")
                raw_data_ev = raw_data_ev[raw_data_ev.index == self.ts_delivery_prev]
                raw_data_ev = dict(raw_data_ev.loc[self.ts_delivery_prev])
                if raw_data_ev["availability"] == 0:
                    # soc stays the same, do nothing
                    # power is 0
                    self.meas_val[ev] = 0
                else:
                    # get old soc
                    with open(f"{self.path}/soc_{ev}.json", "r") as read_file:
                        ev_soc_old = max(0.05 * self.plant_dict[ev]["capacity"],
                                         json.load(read_file) - raw_data_ev["distance_driven"] / 100
                                         * self.plant_dict[ev]["consumption"])
                    # fully charge the battery immediately, from old SoC to full
                    soc_missing = self.plant_dict[ev]["capacity"] - ev_soc_old
                    power_to_full = soc_missing / self.plant_dict[ev]["efficiency"] / 0.25
                    # limit charging power to maximum possible power
                    ev_power = min(power_to_full, self.plant_dict[ev]["charging_power"])
                    # calculate new soc
                    ev_soc_new = ev_soc_old + 0.25 * ev_power * self.plant_dict[ev]["efficiency"]
                    with open(f"{self.path}/soc_{ev}.json", "w") as write_file:
                        json.dump(ev_soc_new, write_file)
                    # update measured values
                    self.meas_val[ev] = ev_power * -1
                    meas_grid += ev_power * -1
            # the heat pump allows storage to discharge until empty, then charges the storage until full.
            # Optionally, the heat pump will always turn on at 13:00 to maximize PV self consumption or to profit
            # from local feed-in
            for hp in self._get_list_plants(plant_type="hp"):
                # calculate cop and electric power limit
                temp_amb = float(self.df_weather_history.loc[self.ts_delivery_prev, "temp"]) - 273.15
                hp_param = pd.read_json(f"{self.path}/spec_{hp}.json")
                heatpump = HeatPump(hp_param)
                t_in_secondary = self.plant_dict[hp]["temperature"] - 5
                hp_sim_res = heatpump.simulate(t_in_primary=temp_amb, t_in_secondary=t_in_secondary,
                                               t_amb=temp_amb, mode=1)
                cop = hp_sim_res['COP']
                p_el_max = hp_sim_res['P_el'] * -1

                # was the hp previously on or off?
                state = self.plant_dict[hp].get("rtc_state", "off")
                # load hp soc
                with open(f"{self.path}/soc_{hp}.json", "r") as read_file:
                    hp_soc_old = json.load(read_file)
                # determine building heat demand
                p_heat = ft.read_dataframe(f"{self.path}/raw_data_{hp}.ft").set_index("timestamp")
                p_heat = float(p_heat[p_heat.index == self.ts_delivery_prev]["heat"].values)

                # what happens if we don't charge at all?
                hp_soc_no_charge = hp_soc_old + 0.25 * p_heat / self.plant_dict[hp]["efficiency"]

                # what power do we need to get the hp to be fully charged in 1 ts?
                hp_el_power_to_full = \
                    -1*(self.plant_dict[hp]["capacity"]
                        - hp_soc_no_charge) /cop /self.plant_dict[hp]["efficiency"] /0.25

                if self.config_dict["controller_strategy"] == "rtc_max_pv" \
                        and (pd.Timestamp.fromtimestamp(self.ts_delivery_current,
                                                       tz="Europe/Berlin").time()
                             == datetime.time(13, 0)):
                    state = "on"

                if state == "off":
                    if hp_soc_no_charge >= 0:
                        p_hp = 0
                        hp_soc_new = hp_soc_no_charge
                    else:
                        p_hp = max(p_el_max, hp_el_power_to_full)
                        hp_soc_new = hp_soc_no_charge - 0.25 * p_hp * cop * self.plant_dict[hp]["efficiency"]
                        state = "on"
                else:
                    p_hp = max(p_el_max, hp_el_power_to_full)
                    hp_soc_new = hp_soc_no_charge - 0.25 * p_hp * cop * self.plant_dict[hp]["efficiency"]
                    if hp_soc_new/self.plant_dict[hp]['capacity'] >= 0.95:
                        state = "off"

                # update measured values
                self.meas_val[hp] = p_hp
                meas_grid += p_hp
                self.plant_dict[hp]["rtc_state"] = state

                with open(f"{self.path}/config_plants.json", "w") as write_file:
                    json.dump(self.plant_dict, write_file)

                with open(f"{self.path}/soc_{hp}.json", "w") as write_file:
                    json.dump(hp_soc_new, write_file)
            # the battery attempts to maintain grid connection power at 0 if at all possible.
            for bat in self._get_list_plants(plant_type="bat"):
                # get old soc
                with open(f"{self.path}/soc_{bat}.json", "r") as read_file:
                    bat_soc_old = json.load(read_file)
                # calc power required to get meas_grid to zero
                if meas_grid > 0:
                    grid_power_requirement = -1 * meas_grid
                    max_power_inv = -1 * self.plant_dict[bat]["power"]
                    max_power_cap = -1 * 4 * (self.plant_dict[bat]["capacity"] - bat_soc_old) \
                                    / self.plant_dict[bat]["efficiency"]
                    bat_power = max(max_power_cap, max_power_inv, grid_power_requirement)
                    bat_soc_new = bat_soc_old - 0.25 * bat_power * self.plant_dict[bat]["efficiency"]

                elif meas_grid <= 0:
                    grid_power_requirement = -1 * meas_grid
                    max_power = self.plant_dict[bat]["power"]
                    max_power_possible = 4 * bat_soc_old * self.plant_dict[bat]["efficiency"]
                    # limit result to max_power
                    bat_power = min(max_power, max_power_possible, grid_power_requirement)
                    # update SoC
                    bat_soc_new = bat_soc_old - 0.25 * bat_power / self.plant_dict[bat]["efficiency"]

                with open(f"{self.path}/soc_{bat}.json", "w") as write_file:
                    json.dump(bat_soc_new, write_file)

                self.meas_val[bat] = bat_power
                meas_grid += bat_power

            self.meas_val[self.config_dict['id_meter_grid']] = int(meas_grid)

        # save calculated values to .json file so that results can be used by later methods in case of parallelization

        with open(f"{self.path}/controller_rtc.json", "w") as write_file:
            json.dump(self.meas_val, write_file)

    def log_meter_readings(self, db_obj):
        """Log the result of the controller_real_time method to the database as metering data.

        :param db_obj: Database instance, pass the database connection instance to this method

        :return: None
        """
        # create local dataframe containing all previous metering logs from file

        df_meas_local = ft.read_dataframe(f"{self.path}/log_ems.ft")
        df_meas_local.set_index("timestamp", inplace=True)
        # define local lists for containing new measurement values, once for local logging, once for database logging
        factor_w_to_wh = 1 / 4

        log_ems = [self.meas_val[self.config_dict['id_meter_grid']] * factor_w_to_wh]

        dict_new_readings_local = {self.config_dict['id_meter_grid']:
                                   [self._decomp_float(self.meas_val[self.config_dict['id_meter_grid']]
                                                       * factor_w_to_wh,
                                                       return_val="neg"),
                                    self._decomp_float(self.meas_val[self.config_dict['id_meter_grid']]
                                                       * factor_w_to_wh,
                                                       return_val="pos")]}

        for plant in self._get_list_plants():
            log_ems.append(self.meas_val[plant] * factor_w_to_wh)
            dict_new_readings_local[plant] = [self._decomp_float(self.meas_val[plant] * factor_w_to_wh,
                                                                 return_val="neg"),
                                              self._decomp_float(self.meas_val[plant] * factor_w_to_wh,
                                                                 return_val="pos")]
        # log meter readings to local df_buffer
        self._set_new_meter_reading_local(dict_new_readings_local)

        # log measurement values to local df and overwrite local logging file

        df_meas_local.loc[self.ts_delivery_prev] = log_ems
        df_meas_local.index.name = "timestamp"
        ft.write_dataframe(df_meas_local.round().reset_index(),
                           f"{self.path}/log_ems.ft")

        # log measurement values to database
        df_meter_readings_input = ft.read_dataframe(f"{self.path}/buffer_meter_readings.ft")

        df_db_logging = df_meter_readings_input[
            df_meter_readings_input["t_send"] <= self.t_now].drop(columns={"t_send"})

        df_meter_readings_output = df_meter_readings_input[df_meter_readings_input["t_send"] > self.t_now]

        if len(df_db_logging):
            db_obj.log_meter_readings_cumulative(df_db_logging)

        ft.write_dataframe(df_meter_readings_output, f"{self.path}/buffer_meter_readings.ft")

    def pv_gen(self, x):
        array = np.array(x[f'power_{self._get_list_plants(plant_type=["pv"])[0]}_quantiles'])
        array[int(x['quantile_number']):] = 0
        return array

    def controller_model_predictive_test(self, controller=None):
        """Execute the model predictive controller_real_time for the market participant given the predicted
        generation, consumption, and market prices for a configurable time horizon.

        The controller_real_time will always attempt to maximize its earnings. If no market price optimization is
        desired, a flat market price utilities should be input.

        :return: None
        """
        ts_delivery_start = pd.Timestamp('2021-03-24 00:00',
                                         tz='europe/berlin').timestamp()
        timesteps = range(int(self.ts_delivery_current),
                          int(self.ts_delivery_current + 900 * self.config_dict["mpc_horizon"]), 900)
        user_risk_qtl_num = 1
        self.base_quantiles = np.zeros(9)
        self.base_quantiles_first = np.zeros(len(self.base_quantiles))
        self.base_quantiles_first[0] = 1
        self.base_quantiles[:user_risk_qtl_num] = 1

        # forecast table section
        self.fcast_table = ft.read_dataframe(f"{self.path}/fcasts_current.ft").set_index("timestamp")

        self.fcast_table = self.fcast_table[self.fcast_table.index.isin(timesteps)]
        self.fcast_table[f'power_{self._get_list_plants(plant_type="hh")[0]}'] *= -1

        probability = np.array([0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1])
        self.fcast_table['probability'] = np.tile(probability, (len(self.fcast_table), 1)).tolist()
        # multiplier = np.array([0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1, 1.05, 1.1])
        multiplier = np.array([0.6, 0.7, 0.8, 0.9, 1, 1.1, 1.2, 1.3, 1.4])
        self.fcast_table[f'power_{self._get_list_plants(plant_type=["pv"])[0]}_quantiles'] =\
            self.fcast_table[f'power_{self._get_list_plants(plant_type=["pv"])[0]}'].apply(
                lambda x: list(np.diff(np.array(x*multiplier).clip(max=self.plant_dict[
                    self._get_list_plants(plant_type="pv")[0]]["power"]).astype(int), prepend=0)))

        self.fcast_table[f"power_{self.config_dict['id_meter_grid']}"] = 0
        self.fcast_table[f"power_in_{self.config_dict['id_meter_grid']}"] = 0
        self.fcast_table[f"power_out_{self.config_dict['id_meter_grid']}"] = 0
        self.fcast_table['quantile_number'] = int(self.base_quantiles.sum())
        # self.fcast_table['quantile_number'] = 9
        self.fcast_table[f'base_power_{self._get_list_plants(plant_type=["pv"])[0]}'] = \
            self.fcast_table.apply(lambda x: list(np.array(self.pv_gen(x=x))), axis=1)
        # array_price_quantiles = (1-probability)*(1-self.base_quantiles)
        # self.fcast_table['price_quantiles'] = np.tile(np.round(array_price_quantiles, 3),
        #                                               (len(self.fcast_table), 1)).tolist()
        self.fcast_table['price_quantiles'] = self.fcast_table['probability'].apply(
            lambda x: list(np.round((1-np.array(x))*(1-self.base_quantiles), 3)))
        self.fcast_table['balancing_price'] = float(0.10)
        self.fcast_table[f'power_{self._get_list_plants(plant_type="hh")[0]}_quantiles'] =\
            np.tile(np.zeros(9).astype(int), (len(self.fcast_table), 1)).tolist()
        self.fcast_table['matched_bid_pos'] = 0
        position_number = np.array([0, 1, 2, 3, 4, 5, 6, 7, 8])
        self.fcast_table['position_number'] = np.tile(position_number, (len(self.fcast_table), 1)).tolist()
        self.fcast_table[f"power_{self.config_dict['id_meter_grid']}_quantiles"] =\
            np.tile(np.zeros(9).astype(int), (len(self.fcast_table), 1)).tolist()
        self.fcast_table.index = self.fcast_table.index.astype(dtype='int64')
        # self.fcast_table.to_csv(os.path.join(self.path_out, f'fcast_table_{self.count}.csv'))

        list_col = [f"power_{self.config_dict['id_meter_grid']}_quantiles",
                    f"power_{self.config_dict['id_meter_grid']}", 'price', 'quantile_number', 'matched_bid_pos',
                    f'power_{self._get_list_plants(plant_type="hh")[0]}_quantiles',
                    f'power_{self._get_list_plants(plant_type=["pv"])[0]}_quantiles']

        # read mpc table
        if not os.path.isfile(f"{self.path}/controller_mpc.ft"):
            self.mpc_table = self.fcast_table[list_col]
        else:
            self.mpc_table = ft.read_dataframe(f"{self.path}/controller_mpc.ft").set_index('timestamp')[list_col]

        self.mpc_table = self.mpc_table[self.mpc_table.index.isin(timesteps)].fillna(0)
        self.fcast_table.update(self.mpc_table)
        self.mpc_table = self.fcast_table.fillna(0)
        self.mpc_table.index = self.mpc_table.index.astype(dtype='int64')
        self.mpc_table = self.mpc_table.astype({'quantile_number': 'int'})
        self.mpc_table['zero_quantile'] = np.tile(np.zeros(9).astype(int), (len(self.mpc_table), 1)).tolist()

        # matched bids
        df_matched_bids = self.matched_bids
        # df_matched_bids.to_csv(os.path.join(self.path_out, f'matched_bids_raw_{self.count}.csv'))
        # self.matched_bids_by_timestep.to_csv(os.path.join(self.path_out, f'matched_bids_ts_{self.count}.csv'))
        df_matched_bids['position_number'] = np.where(df_matched_bids['id_user_bid'] == self.config_dict["id_user"],
                                                      df_matched_bids['number_position_bid'],
                                                      df_matched_bids['number_position_offer'])

        if df_matched_bids.empty:
            df_matched_bids = pd.DataFrame(df_matched_bids, index=self.mpc_table.index)
            df_matched_bids['ts_delivery'] = df_matched_bids.index
            df_matched_bids = df_matched_bids.fillna(0)
            df_matched_bids['net_bids'] = 0
        df_matched_bids = df_matched_bids.groupby(by=['ts_delivery',
                                                      'position_number']).agg({'net_bids': 'sum'}).reset_index()
        df_matched_bids.rename(columns={'ts_delivery': 'timestamp'}, inplace=True)
        df_matched_bids = df_matched_bids.sort_values(by=['timestamp', 'position_number'],
                                                      ascending=[True, True]).set_index('timestamp')
        df_matched_bids = df_matched_bids[df_matched_bids.index.isin(timesteps)]
        # df_matched_bids.to_csv(os.path.join(self.path_out, f'matched_bids_raw_after_{self.count}.csv'))
        df_matched_bids['net_bids'] *= 4
        df_matched_bids = df_matched_bids.astype({'position_number': 'int', 'net_bids': 'int'})
        # df_matched_bids.to_csv(os.path.join(self.path_out, f'matched_bids_{self.count}.csv'))

        if not df_matched_bids.empty:
            self.mpc_table["matched_bid_quantile"] = df_matched_bids.groupby(df_matched_bids.index).apply(
                lambda x: list(dict(dict.fromkeys(map(str, range(9)), 0), **dict(zip(x['position_number'].astype(str),
                                                                                     x['net_bids']))).values()))
            self.mpc_table['matched_bid_quantile'] = self.mpc_table['matched_bid_quantile'].fillna(
                self.mpc_table['zero_quantile'])
        else:
            self.mpc_table["matched_bid_quantile"] = self.mpc_table['zero_quantile']

        self.mpc_table['matched_bid_pos_quantile'] = self.mpc_table['matched_bid_quantile'].apply(
            lambda x: list(np.array(x).clip(min=0)))

        self.mpc_table["matched_bid_pos_update"] = self.mpc_table['matched_bid_pos_quantile'].apply(
            lambda x: np.sum(x)) - self.mpc_table["matched_bid_pos"]

        self.mpc_table["matched_bid_pos"] = self.mpc_table['matched_bid_pos_quantile'].apply(
            lambda x: np.sum(x))

        # grid meter
        self.mpc_table[f"power_{self.config_dict['id_meter_grid']}_pos_quantiles"] = self.mpc_table[
            f"power_{self.config_dict['id_meter_grid']}_quantiles"].apply(
            lambda x: list(np.array(x).clip(min=0)))
        self.mpc_table[f"power_{self.config_dict['id_meter_grid']}_pos"] = self.mpc_table[
            f"power_{self.config_dict['id_meter_grid']}_pos_quantiles"].apply(lambda x: np.sum(x))

        self.mpc_table["state_grid"] = self.mpc_table[f"power_{self.config_dict['id_meter_grid']}_pos"] - \
                                       self.mpc_table["matched_bid_pos"]

        self.mpc_table = self.mpc_table[self.mpc_table.index.isin(timesteps)]
        self.mpc_table = self.mpc_table.fillna(0)
        self.mpc_table['price_quantiles'] = self.mpc_table.apply(lambda x: list(np.round(np.array(
            x['price_quantiles']) * x['balancing_price'], 3)), axis=1)

        self.mpc_table[f'power_{self._get_list_plants(plant_type=["pv"])[0]}'] = self.mpc_table.apply(
            lambda x: list(np.array(self.pv_gen(x=x))), axis=1)

        if int(ts_delivery_start + 900) < self.ts_delivery_current:

            # price adaptation
            self.mpc_table["state_grid_pos"] = self.mpc_table.apply(lambda x:
            np.array(x[f"power_{self.config_dict['id_meter_grid']}_pos_quantiles"] -
            np.array(x['matched_bid_pos_quantile'], dtype=float), dtype=float).clip(min=0).sum(), axis=1)

            # self.mpc_table["price"] = np.where(
            #     self.mpc_table["state_grid_pos"] > 0,
            #     self.mpc_table["price"] - float(0.0000001) * self.mpc_table["state_grid_pos"],
            #     self.mpc_table["price"] + (float(0.000001) * self.mpc_table["matched_bid_pos_update"]))
            self.mpc_table["price"] = np.where(
                self.mpc_table["matched_bid_pos_update"] > 0,
                self.mpc_table["price"] + float(0.0001) * self.mpc_table["matched_bid_pos_update"],
                self.mpc_table["price"] - float(0.00001) * self.mpc_table["state_grid_pos"])

            # quantile shift
            df_mpc = self.mpc_table[self.mpc_table.index.isin(timesteps)]
            base_pv = np.sum(pd.DataFrame(
                df_mpc[f'base_power_{self._get_list_plants(plant_type=["pv"])[0]}'].to_list()).sum(axis=0).to_numpy())

            array_diff = pd.DataFrame(df_mpc[f'power_{self._get_list_plants(plant_type=["pv"])[0]}_quantiles'
                                      ].to_list()).sum(axis=0).to_numpy() - pd.DataFrame(
                df_mpc[f'power_{self._get_list_plants(plant_type=["pv"])[0]}'].to_list()).sum(axis=0).to_numpy()

            user_pref = 0.4
            if np.sum(array_diff) > 0:
                if df_mpc['state_grid'].sum() < base_pv * user_pref:
                    if df_mpc['state_grid'].sum() <= 0:
                        df_mpc['quantile_number'] = np.where(
                            df_mpc['quantile_number'] < len(array_diff), df_mpc['quantile_number'] + 1,
                            df_mpc['quantile_number'])
                    else:
                        state_grid_diff = base_pv * user_pref - df_mpc['state_grid'].sum()
                        array_diff_cumsum = np.cumsum(array_diff)
                        length = len(array_diff_cumsum[array_diff_cumsum <= state_grid_diff])
                        df_mpc['quantile_number'] = np.where(df_mpc['quantile_number'] >= length + 1,
                                                             df_mpc['quantile_number'], length + 1)
                    self.mpc_table.update(df_mpc)
            self.mpc_table[f'power_{self._get_list_plants(plant_type=["pv"])[0]}'] = self.mpc_table.apply(
                lambda x: list(np.array(self.pv_gen(x=x))), axis=1)

        # model declaration
        model = pyo.AbstractModel()
        model.t = pyo.Set(initialize=timesteps)
        model.t_restricted = pyo.Set(initialize=range(
            int(self.ts_delivery_current + 900), int(self.ts_delivery_current + 900 * self.config_dict["mpc_horizon"]),
            900), within=model.t)

        # variable definition
        model.power_pv = pyo.Var(model.t, within=pyo.NonNegativeReals)
        bat_power = self.plant_dict[self._get_list_plants(plant_type="bat")[0]]["power"]
        bat_capacity = self.plant_dict[self._get_list_plants(plant_type="bat")[0]]["capacity"]
        eta_bat = self.plant_dict[self._get_list_plants(plant_type="bat")[0]]["efficiency"]
        model.power_bat_in = pyo.Var(model.t, within=pyo.NonNegativeReals, bounds=(0, bat_power))
        model.power_bat_out = pyo.Var(model.t, within=pyo.NonNegativeReals, bounds=(0, bat_power))
        model.soc_bat = pyo.Var(model.t, within=pyo.NonNegativeReals, bounds=(0, bat_capacity))
        model.power_bat_milp = pyo.Var(model.t, within=pyo.Boolean)
        model.power_grid_out = pyo.Var(model.t, within=pyo.NonNegativeReals)
        model.power_grid_in = pyo.Var(model.t, within=pyo.NonNegativeReals)
        model.scale = pyo.Var(within=pyo.NonNegativeReals)
        model.deviation_grid_pos = pyo.Var(model.t, within=pyo.NonNegativeReals)
        model.deviation_grid_neg = pyo.Var(model.t, within=pyo.NonNegativeReals)
        model.balancing_grid_pos = pyo.Var(model.t, within=pyo.NonNegativeReals)
        model.balancing_grid_neg = pyo.Var(model.t, within=pyo.NonNegativeReals)

        # parameters
        dict_power_load = dict(self.mpc_table[f'power_{self._get_list_plants(plant_type="hh")[0]}'])
        dict_power_pv = dict(self.mpc_table[f'power_{self._get_list_plants(plant_type=["pv"])[0]}'].apply(lambda x: np.sum(x)))
        if self.ts_delivery_current % 3600 == 0:
            self.mpc_table["price"] = 0.055
        self.mpc_table['price_norm'] = np.interp(self.mpc_table["price"], (self.mpc_table["price"].min(),
                                                                           self.mpc_table["price"].max()), (0, 1))
        dict_price_norm = dict(self.mpc_table['price_norm'])
        dict_matched_bids = dict(self.mpc_table[self.mpc_table.index.isin(timesteps)]["matched_bid_pos"])
        dict_power_load_base = dict(
            self.mpc_table[f'power_{self._get_list_plants(plant_type="hh")[0]}_quantiles'].apply(lambda x: np.sum(x)))
        with open(f'{self.path}/soc_{self._get_list_plants(plant_type="bat")[0]}.json', "r") as read_file:
            soc_bat_init = json.load(read_file)


        # constraints
        # battery either charge or discharge constraint
        def battery_charge_constraint(model, t):
            return model.power_bat_in[t] <= model.power_bat_milp[t] * 1000000

        model.battery_charge_constraint = pyo.Constraint(model.t, rule=battery_charge_constraint)

        def battery_discharge_constraint(model, t):
            return model.power_bat_out[t] <= (1 - model.power_bat_milp[t]) * 1000000

        model.battery_discharge_constraint = pyo.Constraint(model.t, rule=battery_discharge_constraint)

        # max pv power constraint
        def pv_max_power_constraint(model, t):
            return model.power_pv[t] == dict_power_pv[t]

        model.pv_max_power_constraint = pyo.Constraint(model.t, rule=pv_max_power_constraint)

        # battery charge only from pv constraint
        def battery_charging_pv_only_constraint(model, t):
            return model.power_bat_in[t] <= dict_power_pv[t]

        model.battery_charging_pv_only_constraint = pyo.Constraint(model.t, rule=battery_charging_pv_only_constraint)

        # initial battery soc constraint
        def init_soc_battery_constraint(model):
            return soc_bat_init - (0.25 * model.power_bat_out[self.ts_delivery_current] / eta_bat) + \
                   (0.25 * model.power_bat_in[self.ts_delivery_current] * eta_bat) == model.soc_bat[self.ts_delivery_current]

        model.init_soc_battery_constraint = pyo.Constraint(rule=init_soc_battery_constraint)

        # soc balance constraint
        def soc_battery_constraint(model, t):
            return model.soc_bat[t - int(900)] - (0.25 * model.power_bat_out[t] / eta_bat) + \
                   (0.25 * model.power_bat_in[t] * eta_bat) == model.soc_bat[t]

        model.soc_battery_constraint = pyo.Constraint(model.t_restricted, rule=soc_battery_constraint)

        # battery ramping constraint
        def battery_ramping_lower_constraint(model, t):
            return -0.1 * bat_power <= ((model.power_bat_out[t] - model.power_bat_in[t])
                                                           - (model.power_bat_out[t - int(900)] -
                                                              model.power_bat_in[t - int(900)]))

        model.battery_ramping_lower_constraint = pyo.Constraint(model.t_restricted,
                                                                rule=battery_ramping_lower_constraint)

        def battery_ramping_upper_constraint(model, t):
            return ((model.power_bat_out[t] - model.power_bat_in[t]) -
                    (model.power_bat_out[t - int(900)] - model.power_bat_in[t - int(900)])) <= 0.1 * bat_power
        model.battery_ramping_upper_constraint = pyo.Constraint(model.t_restricted,
                                                                rule=battery_ramping_upper_constraint)

        # ******************************
        # energy balance pos constraint
        def energy_balance_pos_constraint(model, t):
            return model.power_pv[t] + model.power_bat_out[t] - model.power_bat_in[t] == model.power_grid_out[t]
            # return model.power_pv[t] + model.power_bat_out[t] - model.power_bat_in[t] - dict_power_load[t] ==\
            #        model.power_grid_out[t] - model.power_grid_in[t]

        model.energy_balance_pos_constraint = pyo.Constraint(model.t, rule=energy_balance_pos_constraint)

        # energy balance neg constraint
        def energy_balance_neg_constraint(model, t):
            return dict_power_load[t] == model.power_grid_in[t]

        model.energy_balance_neg_constraint = pyo.Constraint(model.t, rule=energy_balance_neg_constraint)

        # energy deviation constraint
        def energy_deviation_constraint(model, t):
            return model.scale * dict_price_norm[t] == model.power_grid_out[t] - dict_matched_bids[t] - \
                   dict_power_load[t] + model.deviation_grid_pos[t] - model.deviation_grid_neg[t]

        model.energy_deviation_constraint = pyo.Constraint(model.t, rule=energy_deviation_constraint)

        # energy balancing from matched bids constraint
        def energy_balancing_bids_constraint(model, t):
            return dict_matched_bids[t] + dict_power_load[t] + model.balancing_grid_pos[t] - \
                   model.balancing_grid_neg[t] == model.power_grid_out[t]

        model.energy_balancing_bids_constraint = pyo.Constraint(model.t, rule=energy_balancing_bids_constraint)

        # Define objective function
        def objective_function(model):
            # cost = sum((model.balancing_grid_neg[t] + model.deviation_grid_pos[t] +
            #             model.deviation_grid_neg[t]) * dict_price_norm[t] for t in model.t)
            cost = sum(model.balancing_grid_neg[t] + model.deviation_grid_pos[t] +
                       model.deviation_grid_neg[t] for t in model.t)
            objective_expression = cost
            return objective_expression

        # Solve model
        model.objective = pyo.Objective(rule=objective_function, sense=pyo.minimize)  # minimize cost

        instance = model.create_instance()
        opt = pyo.SolverFactory(self.config_dict["solver"])
        results = opt.solve(instance, tee=False)
        # print(results)

        for t in timesteps:
            self.mpc_table.loc[t, f'power_{self._get_list_plants(plant_type="bat")[0]}'] =\
                instance.power_bat_out[t].value - instance.power_bat_in[t].value
            self.mpc_table.loc[t, f'soc_{self._get_list_plants(plant_type="bat")[0]}'] = instance.soc_bat[t].value
            self.mpc_table.loc[t, f"power_out_{self.config_dict['id_meter_grid']}"] = instance.power_grid_out[t].value
            self.mpc_table.loc[t, f"power_in_{self.config_dict['id_meter_grid']}"] = instance.power_grid_in[t].value
            self.mpc_table.loc[t, f"power_{self.config_dict['id_meter_grid']}"] = instance.power_grid_out[t].value -\
                                                                                  instance.power_grid_in[t].value
            # self.mpc_table.loc[t, 'delta_grid_meter'] = instance.scale.value * dict_price_norm[t]
        # self.mpc_table['price_norm'] = np.array(dict_price_norm.values())

        self.mpc_table = self.mpc_table[self.mpc_table.index.isin(timesteps)]
        # self.mpc_table.to_csv(os.path.join(self.path_out, f'mpc_{self.count}.csv'))
        ft.write_dataframe(self.mpc_table, f"{self.path}/controller_mpc.ft")

    def market_agent_test(self, db_obj, clear_positions=False):

        timesteps = range(int(self.ts_delivery_current),
                          int(self.ts_delivery_current + 900 * self.config_dict["mpc_horizon"]), 900)

        df_potential_bids = ft.read_dataframe(f"{self.path}/controller_mpc.ft")
        df_potential_bids = df_potential_bids[df_potential_bids.index.isin(timesteps)]
        # df_potential_bids = pd.DataFrame(self.mpc_table[self.mpc_table.index.isin(timesteps)])
        # ft.write_dataframe(df_potential_bids.reset_index(),
        #                    os.path.join(self.path_out, f'controller_mpc_{self.count}.ft'))
        global list_soc
        list_soc = self.base_quantiles
        # previous ts soc
        for bat in self._get_list_plants(plant_type="bat"):
            with open(f"{self.path}/soc_{bat}.json", "r") as read_file:
                soc_bat_old = json.load(read_file)
        list_soc = soc_bat_old * list_soc

        df_potential_bids[[f'soc_{self._get_list_plants(plant_type="bat")[0]}_quantiles',
                           f'power_{self._get_list_plants(plant_type="bat")[0]}_quantiles',
                           f'power_{self._get_list_plants(plant_type="hh")[0]}_quantiles',
                           f"power_{self.config_dict['id_meter_grid']}_quantiles",
                           f"power_{self._get_list_plants(plant_type=['pv'])[0]}"
                           f"_out_{self.config_dict['id_meter_grid']}_quantiles",
                           f"power_{self._get_list_plants(plant_type='bat')[0]}"
                           f"_out_{self.config_dict['id_meter_grid']}_quantiles"]] = pd.DataFrame(pd.Series(
            df_potential_bids.apply(lambda x: self.bat_quantile(x=x), axis=1),
            index=df_potential_bids.index).to_list(), index=df_potential_bids.index)

        ft.write_dataframe(df_potential_bids.reset_index(), f"{self.path}/controller_mpc.ft")
        if self.save_output_result:
            df_potential_bids.to_csv(os.path.join(self.path_out, f'mpc_after_{self.count}.csv'))
            ft.write_dataframe(df_potential_bids.reset_index(),
                               os.path.join(self.path_out, f'mpc_after_{self.count}.ft'))

        df_potential_bids['price_linear_desc'] = np.linspace(start=0.1, stop=0.01, num=len(df_potential_bids))
        df_potential_bids = df_potential_bids.explode([f"power_{self.config_dict['id_meter_grid']}_quantiles",
                                                       'price_quantiles', 'position_number'])
        df_potential_bids = pd.DataFrame(
            df_potential_bids[[f"power_{self.config_dict['id_meter_grid']}_quantiles",
                               'price_quantiles', 'position_number', 'price_linear_desc']])
        df_potential_bids.rename(columns={f"power_{self.config_dict['id_meter_grid']}_quantiles": "net_bids",
                                          "price_quantiles": "price"}, inplace=True)

        df_potential_bids['net_bids'] = df_potential_bids['net_bids'].fillna(0)
        df_potential_bids['net_bids'] /= 4
        df_potential_bids = df_potential_bids.astype({'price': 'float', 'net_bids': 'int', 'position_number': 'int',
                                                      'price_linear_desc': 'float'})
        df_potential_bids = df_potential_bids.set_index(['position_number'], append=True)

        ts = np.unique(np.array(df_potential_bids.index.get_level_values(level='timestamp')))
        price = np.linspace(start=0.01, stop=0.1, num=len(ts))
        df_potential_bids_temp = pd.DataFrame({'price': price, 'timestamp': ts}).set_index('timestamp')

        position_number = np.array([0, 1, 2, 3, 4, 5, 6, 7, 8])
        df_potential_bids_temp['position_number'] = np.tile(position_number[:int(self.base_quantiles.sum())],
                                             (len(df_potential_bids_temp), 1)).tolist()
        df_potential_bids_temp = df_potential_bids_temp.explode(['position_number']).set_index('position_number',
                                                                                               append=True)
        df_potential_bids['price_linear'] = df_potential_bids.index.map(df_potential_bids_temp['price'])
        df_potential_bids['price'] = df_potential_bids['price_linear'].fillna(df_potential_bids['price'])

        df_potential_bids_temp = pd.DataFrame(df_potential_bids['net_bids'], columns=['net_bids'])
        df_potential_bids_temp['net_bids_res'] = df_potential_bids_temp['net_bids']

        # matched bids
        if not self.matched_bids.empty:
            df_matched_bids = self.matched_bids
            df_matched_bids['position_number'] = np.where(df_matched_bids['id_user_bid'] == self.config_dict["id_user"],
                                                          df_matched_bids['number_position_bid'],
                                                          df_matched_bids['number_position_offer'])
            df_matched_bids = df_matched_bids.groupby(by=['ts_delivery',
                                                          'position_number']).agg({'net_bids': 'sum'}).reset_index()
            df_matched_bids.rename(columns={'ts_delivery': 'timestamp'}, inplace=True)
            df_matched_bids = df_matched_bids.sort_values(by=['timestamp', 'position_number'],
                                                          ascending=[True, True]).set_index('timestamp')
            df_matched_bids = df_matched_bids[df_matched_bids.index.isin(timesteps)]
            df_matched_bids = df_matched_bids.astype({'position_number': 'int', 'net_bids': 'int'})
            df_matched_bids = df_matched_bids.set_index('position_number', append=True)
            df_matched_bids = df_matched_bids[df_matched_bids.index.isin(df_potential_bids_temp.index)]
            df_potential_bids_temp['net_bids_res'] = df_potential_bids_temp['net_bids'] - df_matched_bids['net_bids']
            df_potential_bids_temp['net_bids_res'] = df_potential_bids_temp['net_bids_res'].fillna(
                df_potential_bids_temp['net_bids'])

        df_potential_bids_temp = df_potential_bids_temp.fillna(0).round(0).astype({'net_bids_res': 'int'})
        df_potential_bids_temp = df_potential_bids_temp[df_potential_bids_temp['net_bids_res'] != 0]

        df_positions = pd.DataFrame(columns=[db_obj.db_param.ID_USER, db_obj.db_param.QTY_ENERGY,
                                             db_obj.db_param.PRICE_ENERGY, db_obj.db_param.QUALITY_ENERGY,
                                             db_obj.db_param.PREMIUM_PREFERENCE_QUALITY, db_obj.db_param.TYPE_POSITION,
                                             db_obj.db_param.NUMBER_POSITION, db_obj.db_param.STATUS_POSITION,
                                             db_obj.db_param.T_SUBMISSION, db_obj.db_param.TS_DELIVERY,
                                             'price_linear_desc'],
                                    index=df_potential_bids_temp.index)
        euro_kwh_to_sigma_wh = db_obj.db_param.EURO_TO_SIGMA / 1000

        has_renewable = np.zeros(len(df_positions), dtype=bool)
        has_non_ren = np.zeros(len(df_positions), dtype=bool)
        for plant in self._get_list_plants("pv") + self._get_list_plants("fixedgen"):
            if self.plant_dict[plant]["quality"] in ["green_local"]:
                has_renewable = np.ones(len(df_positions), dtype=bool)
            if self.plant_dict[plant]["quality"] in ["local"]:
                has_non_ren = np.ones(len(df_positions), dtype=bool)

        df_positions[db_obj.db_param.ID_USER] = self.config_dict['id_market_agent']

        df_positions[db_obj.db_param.NUMBER_POSITION] = df_positions.index.get_level_values(level='position_number')
        df_positions[db_obj.db_param.QTY_ENERGY] = df_potential_bids_temp['net_bids_res']
        df_positions[db_obj.db_param.QTY_ENERGY] = np.where((~df_positions[db_obj.db_param.NUMBER_POSITION].isin([0]))
                                                            & (df_positions[db_obj.db_param.QTY_ENERGY] < 0),
                                                            0, df_positions[db_obj.db_param.QTY_ENERGY].abs())

        df_positions[db_obj.db_param.TYPE_POSITION] = np.where(df_potential_bids_temp['net_bids_res'] > 0, "offer", "bid")
        df_positions[db_obj.db_param.STATUS_POSITION] = 0
        df_positions[db_obj.db_param.QUALITY_ENERGY] = np.where(
            df_potential_bids_temp['net_bids_res'] < 0, self.config_dict["ma_preference_quality"],
            np.where(df_potential_bids_temp['net_bids_res'] > 0 & has_non_ren, "local",
                     np.where(df_potential_bids_temp['net_bids_res'] > 0 & has_renewable, 'green_local', "na")))
        df_positions[db_obj.db_param.PREMIUM_PREFERENCE_QUALITY] = np.where(
            df_potential_bids_temp['net_bids_res'] < 0, self.config_dict["ma_premium_preference_quality"], 0)
        df_positions[db_obj.db_param.T_SUBMISSION] = self.t_now
        df_positions[db_obj.db_param.TS_DELIVERY] = df_positions.index.get_level_values(level='timestamp')
        df_positions[db_obj.db_param.PRICE_ENERGY].fillna(df_potential_bids['price'], inplace=True)
        df_positions['price_linear_desc'].fillna(df_potential_bids['price_linear_desc'], inplace=True)
        df_positions[[db_obj.db_param.PRICE_ENERGY, 'price_linear_desc']] = df_positions[[db_obj.db_param.PRICE_ENERGY,
                                                                                          'price_linear_desc']].round(2)
        # df_positions[db_obj.db_param.PRICE_ENERGY] = np.where(
        #     df_potential_bids_temp['net_bids_res'] < 0, round(self.config_dict["max_bid"], 6) * euro_kwh_to_sigma_wh,
        #     np.where(df_positions[db_obj.db_param.PRICE_ENERGY] < self.config_dict["min_offer"],
        #              round(self.config_dict["min_offer"], 6) * euro_kwh_to_sigma_wh,
        #              df_positions[db_obj.db_param.PRICE_ENERGY] * euro_kwh_to_sigma_wh))
        df_positions[db_obj.db_param.PRICE_ENERGY] = np.where(
            df_potential_bids_temp['net_bids_res'] < 0, df_positions['price_linear_desc'] * euro_kwh_to_sigma_wh,
            df_positions[db_obj.db_param.PRICE_ENERGY] * euro_kwh_to_sigma_wh)
        df_positions = df_positions.drop('price_linear_desc', axis=1)
        df_positions = df_positions[df_positions[db_obj.db_param.QTY_ENERGY] != 0]

        if clear_positions:
            db_obj.clear_positions(id_user=self.config_dict['id_market_agent'])
        if not df_positions.empty:
            if self.save_output_result:
                df_positions.to_csv(os.path.join(self.path_out, f'positions_{self.count}.csv'))
            db_obj.post_positions(df_positions, t_override=self.t_now)

    def bat_quantile(self, x):
        global list_soc
        list_soc = np.where(list_soc > 0, list_soc, 0)
        if x[f'power_{self._get_list_plants(plant_type="bat")[0]}'] <= 0:
            # battery charging method

            # pv to load/grid in
            pv_base = x[f'power_{self._get_list_plants(plant_type=["pv"])[0]}'] * self.base_quantiles
            pv_base_cum = np.subtract(np.cumsum(pv_base), x[f"power_in_{self.config_dict['id_meter_grid']}"])

            length = len(pv_base_cum[pv_base_cum < 0])
            pv_base_cum[:length] = 0
            pv_base_cum[length + 1:] = 0

            pv_base_grid_in = np.subtract(pv_base, pv_base_cum)
            pv_base_grid_in[length + 1:] = 0

            pv_rem = x[f'power_{self._get_list_plants(plant_type=["pv"])[0]}'] - pv_base_grid_in

            if pv_rem.sum() >= x[f'power_{self._get_list_plants(plant_type="bat")[0]}']:
                # matched bid aligning for pv
                matched_bids_pos = np.array(x['matched_bid_pos_quantile']).astype(float)
                matched_bids_pos_rem = np.subtract(matched_bids_pos, pv_rem).clip(min=0)
                matched_bids_pos_pv = matched_bids_pos - matched_bids_pos_rem
                pv_res = pv_rem - matched_bids_pos_pv

                if pv_res.sum() >= x[f'power_{self._get_list_plants(plant_type="bat")[0]}']:
                    # pv to battery
                    pv_cum = np.add(np.cumsum(pv_res), x[f'power_{self._get_list_plants(plant_type="bat")[0]}'])
                    length = len(pv_cum[pv_cum < 0])
                    pv_cum[:length] = 0
                    pv_cum[length + 1:] = 0
                    pv_bat = np.subtract(pv_res, pv_cum)
                    pv_bat[length + 1:] = 0
                    pv_rem = np.subtract(pv_res, pv_bat)
                else:
                    # pv to battery
                    pv_cum = np.add(np.cumsum(pv_rem), x[f'power_{self._get_list_plants(plant_type="bat")[0]}'])
                    length = len(pv_cum[pv_cum < 0])
                    pv_cum[:length] = 0
                    pv_cum[length + 1:] = 0
                    pv_bat = np.subtract(pv_rem, pv_cum)
                    pv_bat[length + 1:] = 0
                    pv_rem -= pv_bat

                    # matched bid aligning for pv
                    matched_bids_pos = np.array(x['matched_bid_pos_quantile']).astype(float)
                    matched_bids_pos_rem = np.subtract(matched_bids_pos, pv_rem).clip(min=0)
                    matched_bids_pos_pv = matched_bids_pos - matched_bids_pos_rem
                    pv_rem -= matched_bids_pos_pv

            else:
                # pv to battery
                pv_cum = np.add(np.cumsum(x[f'power_{self._get_list_plants(plant_type=["pv"])[0]}']),
                                x[f'power_{self._get_list_plants(plant_type="bat")[0]}'])
                length = len(pv_cum[pv_cum < 0])
                pv_cum[:length] = 0
                pv_cum[length + 1:] = 0
                pv_bat = np.subtract(x[f'power_{self._get_list_plants(plant_type=["pv"])[0]}'], pv_cum)
                pv_bat[length + 1:] = 0
                pv_rem = x[f'power_{self._get_list_plants(plant_type=["pv"])[0]}'] - pv_bat

                # pv to load/grid in
                pv_base = pv_rem * self.base_quantiles
                pv_base_cum = np.subtract(np.cumsum(pv_base), x[f"power_in_{self.config_dict['id_meter_grid']}"])

                length = len(pv_base_cum[pv_base_cum < 0])
                pv_base_cum[:length] = 0
                pv_base_cum[length + 1:] = 0

                pv_base_grid_in = np.subtract(pv_base, pv_base_cum)
                pv_base_grid_in[length + 1:] = 0

                pv_rem -= pv_base_grid_in

                # matched bid aligning for pv
                matched_bids_pos = np.array(x['matched_bid_pos_quantile']).astype(float)
                matched_bids_pos_rem = np.subtract(matched_bids_pos, pv_rem).clip(min=0)
                matched_bids_pos_pv = matched_bids_pos - matched_bids_pos_rem
                pv_rem -= matched_bids_pos_pv

            grid_in_rem = x[f"power_in_{self.config_dict['id_meter_grid']}"] - np.sum(pv_base_grid_in)
            list_soc = np.add(list_soc, pv_bat * 0.25).round(2)
            pv_bat *= -1

            # # matched bid aligning for pv
            # # matched_bids_pos_non_base = x['matched_bid_pos_quantile'] * (1 - self.base_quantiles)
            # # matched_bids_pos_rem = np.subtract(matched_bids_pos_non_base,
            # #                                    x[f'power_{self._get_list_plants(plant_type=["pv"])[0]}']).clip(min=0)
            # # matched_bids_pos_pv = matched_bids_pos_non_base - matched_bids_pos_rem
            #
            # matched_bids_pos = np.array(x['matched_bid_pos_quantile']).astype(float)
            # matched_bids_pos_rem = np.subtract(matched_bids_pos,
            #                                    x[f'power_{self._get_list_plants(plant_type=["pv"])[0]}']).clip(min=0)
            # matched_bids_pos_pv = matched_bids_pos - matched_bids_pos_rem
            # pv_rem = x[f'power_{self._get_list_plants(plant_type=["pv"])[0]}'] - matched_bids_pos_pv
            #
            # # pv to battery
            # pv_cum = np.add(np.cumsum(pv_rem[::-1])[::-1],
            #                    x[f'power_{self._get_list_plants(plant_type="bat")[0]}'])
            # length = len(pv_cum[pv_cum >= 0])
            # pv_cum[:length - 1] = 0
            # pv_cum[length:] = 0
            # pv_bat = np.subtract(pv_rem, pv_cum)
            # pv_bat[:length - 1] = 0
            # pv_rem -= pv_bat
            #
            # # pv to load/grid in
            # pv_base = pv_rem * self.base_quantiles
            # pv_base_cum = np.subtract(np.cumsum(pv_base), x[f"power_in_{self.config_dict['id_meter_grid']}"])
            #
            # length = len(pv_base_cum[pv_base_cum < 0])
            # pv_base_cum[:length] = 0
            # pv_base_cum[length + 1:] = 0
            #
            # pv_base_grid_in = np.subtract(pv_base, pv_base_cum)
            # pv_base_grid_in[length + 1:] = 0
            #
            # grid_in_rem = x[f"power_in_{self.config_dict['id_meter_grid']}"] - np.sum(pv_base_grid_in)
            # pv_rem -= pv_base_grid_in
            # list_soc = np.add(list_soc, pv_bat * 0.25).round(2)
            # pv_bat *= -1

            bat_quantile = np.array(pv_bat).round(2)
            bat_grid_out = np.zeros(len(self.base_quantiles))
            grid_in = np.add(grid_in_rem * self.base_quantiles_first, pv_base_grid_in).round(2)
            hh_quantile = np.array(pv_base_grid_in).round(2)

            matched_bids_pos_rem *= (1 - self.base_quantiles)

            if matched_bids_pos_rem.sum() > 0:
                # pv_grid_out = np.add(matched_bids_pos_pv, pv_base - pv_base_grid_in).round(2)
                pv_grid_out = np.add(matched_bids_pos_pv, pv_rem * self.base_quantiles).round(2)
                grid_meter = np.subtract(pv_grid_out, grid_in_rem * self.base_quantiles_first).round(2)
            else:
                pv_grid_out = np.subtract((x[f'power_{self._get_list_plants(plant_type=["pv"])[0]}'] + bat_quantile),
                                          pv_base_grid_in).round(2)
                grid_meter = np.subtract((x[f'power_{self._get_list_plants(plant_type=["pv"])[0]}'] + bat_quantile),
                                         grid_in).round(2)

        else:
            # battery discharging method

            # pv to load/grid in
            pv_base = x[f'power_{self._get_list_plants(plant_type=["pv"])[0]}'] * self.base_quantiles
            pv_base_cum = np.subtract(np.cumsum(pv_base), x[f"power_in_{self.config_dict['id_meter_grid']}"])

            length = len(pv_base_cum[pv_base_cum < 0])
            pv_base_cum[:length] = 0
            pv_base_cum[length + 1:] = 0

            pv_base_grid_in = np.subtract(pv_base, pv_base_cum)
            pv_base_grid_in[length + 1:] = 0

            pv_rem = x[f'power_{self._get_list_plants(plant_type=["pv"])[0]}'] - pv_base_grid_in
            grid_in_rem = x[f"power_in_{self.config_dict['id_meter_grid']}"] - np.sum(pv_base_grid_in)

            # bat to grid in
            bat_rem = x[f'power_{self._get_list_plants(plant_type="bat")[0]}']
            list_soc_base = list_soc * self.base_quantiles
            if grid_in_rem < bat_rem:
                soc_base_cum = np.subtract(np.cumsum(list_soc_base), grid_in_rem * 0.25)
            else:
                soc_base_cum = np.subtract(np.cumsum(list_soc_base), bat_rem * 0.25)

            length = len(soc_base_cum[soc_base_cum < 0])
            soc_base_cum[:length] = 0
            soc_base_cum[length + 1:] = 0

            grid_in_bat = np.subtract(list_soc_base, soc_base_cum)
            grid_in_bat[length + 1:] = 0

            list_soc = np.subtract(list_soc, grid_in_bat)
            grid_in_bat *= 4

            grid_in_rem -= np.sum(grid_in_bat).round(2)
            bat_rem -= np.sum(grid_in_bat).round(2)

            # matched bids aligning for pv
            matched_bids_pos = np.array(x['matched_bid_pos_quantile']).astype(float)
            matched_bids_pos_rem = np.subtract(matched_bids_pos, pv_rem).clip(min=0)
            matched_bids_pos_pv = matched_bids_pos - matched_bids_pos_rem
            pv_rem -= matched_bids_pos_pv

            # matched bids aligning for battery
            soc_rem = np.subtract(list_soc, matched_bids_pos_rem * 0.25).clip(min=0)
            matched_bids_pos_bat = (list_soc - soc_rem) * 4
            matched_bids_pos_bat_cum = np.subtract(np.cumsum(matched_bids_pos_bat), bat_rem)

            length = len(matched_bids_pos_bat_cum[matched_bids_pos_bat_cum < 0])
            matched_bids_pos_bat_cum[:length] = 0
            matched_bids_pos_bat_cum[length + 1:] = 0

            matched_bids_pos_bat = np.subtract(matched_bids_pos_bat, matched_bids_pos_bat_cum)
            matched_bids_pos_bat[length + 1:] = 0

            matched_bids_pos_rem -= matched_bids_pos_bat
            list_soc = np.subtract(list_soc, matched_bids_pos_bat * 0.25).round(2)
            bat_rem -= np.sum(matched_bids_pos_bat)

            # # matched bids aligning for pv
            # # matched_bids_pos_non_base = x['matched_bid_pos_quantile'] * (1 - self.base_quantiles)
            # # matched_bids_pos_rem = np.subtract(matched_bids_pos_non_base,
            # #                                    x[f'power_{self._get_list_plants(plant_type=["pv"])[0]}']).clip(min=0)
            # # matched_bids_pos_pv = matched_bids_pos_non_base - matched_bids_pos_rem
            #
            # matched_bids_pos = np.array(x['matched_bid_pos_quantile']).astype(float)
            # matched_bids_pos_rem = np.subtract(matched_bids_pos,
            #                                    x[f'power_{self._get_list_plants(plant_type=["pv"])[0]}']).clip(min=0)
            # matched_bids_pos_pv = matched_bids_pos - matched_bids_pos_rem
            # pv_rem = x[f'power_{self._get_list_plants(plant_type=["pv"])[0]}'] - matched_bids_pos_pv
            #
            # # matched bids aligning for battery
            # soc_rem = np.subtract(list_soc, matched_bids_pos_rem * 0.25).clip(min=0)
            # matched_bids_pos_bat = (list_soc - soc_rem) * 4
            # matched_bids_pos_bat_cum = np.subtract(np.cumsum(matched_bids_pos_bat),
            #                                        x[f'power_{self._get_list_plants(plant_type="bat")[0]}'])
            #
            # length = len(matched_bids_pos_bat_cum[matched_bids_pos_bat_cum < 0])
            # matched_bids_pos_bat_cum[:length] = 0
            # matched_bids_pos_bat_cum[length + 1:] = 0
            #
            # matched_bids_pos_bat = np.subtract(matched_bids_pos_bat, matched_bids_pos_bat_cum)
            # matched_bids_pos_bat[length + 1:] = 0
            #
            # matched_bids_pos_rem -= matched_bids_pos_bat
            # list_soc = np.subtract(list_soc, matched_bids_pos_bat * 0.25).round(2)
            # bat_rem = x[f'power_{self._get_list_plants(plant_type="bat")[0]}'] - np.sum(matched_bids_pos_bat)
            #
            # # pv to load/grid in
            # pv_base = pv_rem * self.base_quantiles
            # pv_base_cum = np.subtract(np.cumsum(pv_base), x[f"power_in_{self.config_dict['id_meter_grid']}"])
            #
            # length = len(pv_base_cum[pv_base_cum < 0])
            # pv_base_cum[:length] = 0
            # pv_base_cum[length + 1:] = 0
            #
            # pv_base_grid_in = np.subtract(pv_base, pv_base_cum)
            # pv_base_grid_in[length + 1:] = 0
            # grid_in_rem = x[f"power_in_{self.config_dict['id_meter_grid']}"] - np.sum(pv_base_grid_in)
            #
            # # bat to grid in
            # list_soc_base = list_soc * self.base_quantiles
            # if grid_in_rem < bat_rem:
            #     soc_base_cum = np.subtract(np.cumsum(list_soc_base), grid_in_rem * 0.25)
            # else:
            #     soc_base_cum = np.subtract(np.cumsum(list_soc_base), bat_rem * 0.25)
            #
            # length = len(soc_base_cum[soc_base_cum < 0])
            # soc_base_cum[:length] = 0
            # soc_base_cum[length + 1:] = 0
            #
            # grid_in_bat = np.subtract(list_soc_base, soc_base_cum)
            # grid_in_bat[length + 1:] = 0
            #
            # list_soc = np.subtract(list_soc, grid_in_bat)
            # grid_in_bat *= 4
            #
            # grid_in_rem -= np.sum(grid_in_bat).round(2)
            # bat_rem -= np.sum(grid_in_bat).round(2)

            matched_bids_pos_rem *= (1 - self.base_quantiles)

            if matched_bids_pos_rem.sum() > 0:

                # final values battery quantiles and battery grid out quantiles
                bat_quantile = np.array(matched_bids_pos_bat + grid_in_bat).round(2)
                bat_grid_out = np.array(matched_bids_pos_bat).round(2)
                pv_grid_out = np.add((x[f'power_{self._get_list_plants(plant_type=["pv"])[0]}'] * self.base_quantiles -
                                      pv_base_grid_in), matched_bids_pos_pv * (1 - self.base_quantiles)).round(2)
            else:
                # bat to grid out non-base
                list_soc_non_base = list_soc * (1 - self.base_quantiles)
                soc_non_base_cum = np.subtract(np.cumsum(list_soc_non_base), bat_rem * 0.25)

                length = len(soc_non_base_cum[soc_non_base_cum < 0])
                soc_non_base_cum[:length] = 0
                soc_non_base_cum[length + 1:] = 0

                grid_out_bat_non_base = np.subtract(list_soc_non_base, soc_non_base_cum)
                grid_out_bat_non_base[length + 1:] = 0

                list_soc = np.subtract(list_soc, grid_out_bat_non_base)
                grid_out_bat_non_base *= 4

                bat_rem -= np.sum(grid_out_bat_non_base).round(2)

                # bat to grid out base
                list_soc_base = list_soc * self.base_quantiles
                soc_base_cum = np.subtract(np.cumsum(list_soc_base), bat_rem * 0.25)

                length = len(soc_base_cum[soc_base_cum < 0])
                soc_base_cum[:length] = 0
                soc_base_cum[length + 1:] = 0

                grid_out_bat_base = np.subtract(list_soc_base, soc_base_cum)
                grid_out_bat_base[length + 1:] = 0

                list_soc = np.subtract(list_soc, grid_out_bat_base)
                grid_out_bat_base *= 4

                # final values battery quantiles and battery grid out quantiles
                bat_quantile = np.array(matched_bids_pos_bat + grid_in_bat + grid_out_bat_non_base + grid_out_bat_base
                                        ).round(2)
                bat_grid_out = np.array(matched_bids_pos_bat + grid_out_bat_non_base + grid_out_bat_base).round(2)
                pv_grid_out = np.subtract(x[f'power_{self._get_list_plants(plant_type=["pv"])[0]}'],
                                          pv_base_grid_in).round(2)

            hh_quantile = np.array(grid_in_bat + pv_base_grid_in).round(2)
            grid_meter = np.add(pv_grid_out - grid_in_rem * self.base_quantiles_first, bat_grid_out).round(2)


        list_soc = np.where(list_soc > 0, list_soc, 0)
        return [list_soc.tolist(), bat_quantile.tolist(), hh_quantile.tolist(), grid_meter.tolist(),
                pv_grid_out.tolist(), np.array(bat_grid_out).round(2).tolist()]

    def bat_quantile_test(self, x):
        global list_soc
        list_soc = np.where(list_soc > 0, list_soc, 0)
        if x[f'power_{self._get_list_plants(plant_type="bat")[0]}'] <= 0:
            array_add = np.add(np.cumsum(x[f'power_{self._get_list_plants(plant_type=["pv"])[0]}'][::-1])[::-1],
                               x[f'power_{self._get_list_plants(plant_type="bat")[0]}'])
            length = len(array_add[array_add >= 0])
            array_add[:length - 1] = 0
            array_add[length:] = 0
            array_bat_sub = np.subtract(x[f'power_{self._get_list_plants(plant_type=["pv"])[0]}'], array_add)
            array_bat_sub[:length - 1] = 0
            array_pv_rem = np.subtract(x[f'power_{self._get_list_plants(plant_type=["pv"])[0]}'], array_bat_sub)
            array_pv_rem_base = array_pv_rem * np.array([1, 0, 0, 0, 0, 0, 0, 0, 0])
            array_sub = np.subtract(np.cumsum(array_pv_rem_base), x[f"power_in_{self.config_dict['id_meter_grid']}"])
            length = len(array_sub[array_sub < 0])
            array_sub[:length] = 0
            array_sub[length + 1:] = 0
            array_sub_rem_base = np.subtract(array_pv_rem_base, array_sub)
            array_sub_rem_base[length + 1:] = 0

            list_soc = np.add(list_soc, array_bat_sub * 0.25).round(2)
            array_bat_sub *= -1
            array_bat = np.array(array_bat_sub).round(2)
            array_grid_in = np.add((x[f"power_in_{self.config_dict['id_meter_grid']}"] -
                                    np.sum(array_sub_rem_base)) * np.array([1, 0, 0, 0, 0, 0, 0, 0, 0]),
                                   array_sub_rem_base).round(2)
            array_hh_base = np.array(array_sub_rem_base).round(2)
            array_grid_meter = np.subtract(array_pv_rem, array_grid_in).round(2)
            array_pv_out = np.subtract(array_pv_rem, array_sub_rem_base).round(2)
        else:
            array_pv_base = x[f'power_{self._get_list_plants(plant_type=["pv"])[0]}']\
                            * np.array([1, 0, 0, 0, 0, 0, 0, 0, 0])
            array_sub = np.subtract(np.cumsum(array_pv_base), x[f"power_in_{self.config_dict['id_meter_grid']}"])
            length = len(array_sub[array_sub < 0])
            array_sub[:length] = 0
            array_sub[length + 1:] = 0
            array_sub_pv_base = np.subtract(array_pv_base, array_sub)
            array_sub_pv_base[length + 1:] = 0
            grid_in_rem = x[f"power_in_{self.config_dict['id_meter_grid']}"] - np.sum(array_sub_pv_base)
            list_soc_base = list_soc * np.array([1, 0, 0, 0, 0, 0, 0, 0, 0])

            if grid_in_rem < x[f'power_{self._get_list_plants(plant_type="bat")[0]}']:
                array_soc_sub = np.subtract(np.cumsum(list_soc_base[::-1])[::-1], grid_in_rem * 0.25)
            else:
                array_soc_sub = np.subtract(np.cumsum(list_soc_base[::-1])[::-1],
                                            x[f'power_{self._get_list_plants(plant_type="bat")[0]}'] * 0.25)

            length = len(array_soc_sub[array_soc_sub >= 0])
            array_soc_sub[:length - 1] = 0
            array_soc_sub[length:] = 0
            array_grid_in_sub = np.subtract(list_soc_base, array_soc_sub)
            array_grid_in_sub[:length - 1] = 0
            list_soc = np.subtract(list_soc, array_grid_in_sub)
            grid_in_rem -= np.sum(array_grid_in_sub * 4).round(2)
            bat_rem = x[f'power_{self._get_list_plants(plant_type="bat")[0]}'] -\
                      np.sum(array_grid_in_sub * 4).round(2)
            array_soc_sub = np.subtract(np.cumsum(list_soc[::-1])[::-1], bat_rem * 0.25)
            length = len(array_soc_sub[array_soc_sub >= 0])
            array_soc_sub[:length - 1] = 0
            array_soc_sub[length:] = 0
            array_bat_sub = np.subtract(list_soc, array_soc_sub)
            array_bat_sub[:length - 1] = 0

            list_soc = np.subtract(list_soc, array_bat_sub).round(2)
            array_bat_sub *= 4
            array_bat = np.array(array_grid_in_sub * 4 + array_bat_sub).round(2)
            array_grid_in = np.array(grid_in_rem * np.array([1, 0, 0, 0, 0, 0, 0, 0, 0]) +
                                     array_grid_in_sub * 4 + array_sub_pv_base).round(2)
            array_hh_base = np.array(array_grid_in_sub * 4 + array_sub_pv_base).round(2)
            array_grid_meter = np.subtract((array_bat + x[f'power_{self._get_list_plants(plant_type=["pv"])[0]}']),
                                           array_grid_in).round(2)
            array_pv_out = np.subtract(x[f'power_{self._get_list_plants(plant_type=["pv"])[0]}'],
                                       array_sub_pv_base).round(2)
        list_soc = np.where(list_soc > 0, list_soc, 0)
        return [list_soc.tolist(), array_bat.tolist(), array_hh_base.tolist(), array_grid_meter.tolist(),
                array_pv_out.tolist(), np.array(array_bat_sub).round(2).tolist()]

    def mpc_qtl(self, controller=None):
        """Execute the model predictive controller_real_time for the market participant given the predicted
        generation, consumption, and market prices for a configurable time horizon.

        The controller_real_time will always attempt to maximize its earnings. If no market price optimization is
        desired, a flat market price utilities should be input.

        :return: None
        """

        timesteps = range(int(self.ts_delivery_current),
                          int(self.ts_delivery_current + 900 * self.config_dict["mpc_horizon"]), 900)
        user_risk_qtl_num = 1
        self.base_quantiles = np.zeros(9)
        self.base_quantiles_first = np.zeros(len(self.base_quantiles))
        self.base_quantiles_first[0] = 1
        self.base_quantiles[:user_risk_qtl_num] = 1

        # forecast table section
        self.fcast_table = ft.read_dataframe(f"{self.path}/fcasts_current.ft").set_index("timestamp")

        self.fcast_table = self.fcast_table[self.fcast_table.index.isin(timesteps)]
        self.fcast_table[f'power_{self._get_list_plants(plant_type="hh")[0]}'] *= -1

        probability = np.array([0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1])
        self.fcast_table['probability'] = np.tile(probability, (len(self.fcast_table), 1)).tolist()
        # multiplier = np.array([0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1, 1.05, 1.1])
        multiplier = np.array([0.6, 0.7, 0.8, 0.9, 1, 1.1, 1.2, 1.3, 1.4])
        self.fcast_table[f'power_{self._get_list_plants(plant_type=["pv"])[0]}_quantiles'] =\
            self.fcast_table[f'power_{self._get_list_plants(plant_type=["pv"])[0]}'].apply(
                lambda x: list(np.diff(np.array(x*multiplier).clip(max=self.plant_dict[
                    self._get_list_plants(plant_type="pv")[0]]["power"]).astype(int), prepend=0)))

        self.fcast_table[f"power_{self.config_dict['id_meter_grid']}"] = 0
        self.fcast_table[f"power_in_{self.config_dict['id_meter_grid']}"] = 0
        self.fcast_table[f"power_out_{self.config_dict['id_meter_grid']}"] = 0

        self.fcast_table['price_quantiles'] = self.fcast_table['probability'].apply(
            lambda x: list(np.round((1-np.array(x))*(1-self.base_quantiles), 3)))
        self.fcast_table['balancing_price'] = float(0.10)
        self.fcast_table['matched_bid_pos'] = 0
        self.fcast_table.index = self.fcast_table.index.astype(dtype='int64')
        position_number = np.array([0, 1, 2, 3, 4, 5, 6, 7, 8])
        self.fcast_table['position_number'] = np.tile(position_number, (len(self.fcast_table), 1)).tolist()

        self.mpc_table = self.fcast_table.fillna(0)
        self.mpc_table.index = self.mpc_table.index.astype(dtype='int64')
        self.mpc_table = self.mpc_table[self.mpc_table.index.isin(timesteps)]
        self.mpc_table['price_quantiles'] = self.mpc_table.apply(lambda x: list(np.round(np.array(
            x['price_quantiles']) * x['balancing_price'], 3)), axis=1)

        # model declaration
        model = pyo.AbstractModel()
        model.t = pyo.Set(initialize=timesteps)
        model.t_restricted = pyo.Set(initialize=range(
            int(self.ts_delivery_current + 900), int(self.ts_delivery_current + 900 * self.config_dict["mpc_horizon"]),
            900), within=model.t)

        # variable definition
        model.power_pv = pyo.Var(model.t, within=pyo.NonNegativeReals)
        bat_power = self.plant_dict[self._get_list_plants(plant_type="bat")[0]]["power"]
        bat_capacity = self.plant_dict[self._get_list_plants(plant_type="bat")[0]]["capacity"]
        eta_bat = self.plant_dict[self._get_list_plants(plant_type="bat")[0]]["efficiency"]

        model.power_bat_in = pyo.Var(model.t, within=pyo.NonNegativeReals, bounds=(0, bat_power))
        model.power_bat_out = pyo.Var(model.t, within=pyo.NonNegativeReals, bounds=(0, bat_power))
        model.soc_bat = pyo.Var(model.t, within=pyo.NonNegativeReals, bounds=(0, bat_capacity))
        model.power_bat_milp = pyo.Var(model.t, within=pyo.Boolean)
        model.power_grid_out = pyo.Var(model.t, within=pyo.NonNegativeReals)
        model.power_grid_in = pyo.Var(model.t, within=pyo.NonNegativeReals)

        # parameters
        dict_power_load = dict(self.mpc_table[f'power_{self._get_list_plants(plant_type="hh")[0]}'])
        dict_power_pv = dict(self.mpc_table[f'power_{self._get_list_plants(plant_type=["pv"])[0]}_quantiles'].apply(
            lambda x: np.sum(np.array(x) * self.base_quantiles)))
        dict_price = dict(self.mpc_table[f'price'])
        dict_levi_pos = dict(self.mpc_table[f'price_energy_levies_positive'])
        dict_levi_neg = dict(self.mpc_table[f'price_energy_levies_negative'])
        with open(f'{self.path}/soc_{self._get_list_plants(plant_type="bat")[0]}.json', "r") as read_file:
            soc_bat_init = json.load(read_file)

        # constraints
        # battery either charge or discharge constraint
        def battery_charge_constraint(model, t):
            return model.power_bat_in[t] <= model.power_bat_milp[t] * 1000000

        model.battery_charge_constraint = pyo.Constraint(model.t, rule=battery_charge_constraint)

        def battery_discharge_constraint(model, t):
            return model.power_bat_out[t] <= (1 - model.power_bat_milp[t]) * 1000000

        model.battery_discharge_constraint = pyo.Constraint(model.t, rule=battery_discharge_constraint)

        # max pv power constraint
        def pv_max_power_constraint(model, t):
            return model.power_pv[t] == dict_power_pv[t]

        model.pv_max_power_constraint = pyo.Constraint(model.t, rule=pv_max_power_constraint)

        # battery charge only from pv constraint
        def battery_charging_pv_only_constraint(model, t):
            return model.power_bat_in[t] <= dict_power_pv[t]

        model.battery_charging_pv_only_constraint = pyo.Constraint(model.t, rule=battery_charging_pv_only_constraint)

        # initial battery soc constraint
        def init_soc_battery_constraint(model):
            return soc_bat_init - (0.25 * model.power_bat_out[self.ts_delivery_current] / eta_bat) + \
                   (0.25 * model.power_bat_in[self.ts_delivery_current] * eta_bat) == model.soc_bat[self.ts_delivery_current]

        model.init_soc_battery_constraint = pyo.Constraint(rule=init_soc_battery_constraint)

        # soc balance constraint
        def soc_battery_constraint(model, t):
            return model.soc_bat[t - int(900)] - (0.25 * model.power_bat_out[t] / eta_bat) + \
                   (0.25 * model.power_bat_in[t] * eta_bat) == model.soc_bat[t]

        model.soc_battery_constraint = pyo.Constraint(model.t_restricted, rule=soc_battery_constraint)

        # battery ramping constraint
        def battery_ramping_lower_constraint(model, t):
            return -0.1 * bat_power <= ((model.power_bat_out[t] - model.power_bat_in[t])
                                                           - (model.power_bat_out[t - int(900)] -
                                                              model.power_bat_in[t - int(900)]))

        model.battery_ramping_lower_constraint = pyo.Constraint(model.t_restricted,
                                                                rule=battery_ramping_lower_constraint)

        def battery_ramping_upper_constraint(model, t):
            return ((model.power_bat_out[t] - model.power_bat_in[t]) -
                    (model.power_bat_out[t - int(900)] - model.power_bat_in[t - int(900)])) <= 0.1 * bat_power
        model.battery_ramping_upper_constraint = pyo.Constraint(model.t_restricted,
                                                                rule=battery_ramping_upper_constraint)

        # ******************************
        # energy balance pos constraint
        def energy_balance_constraint(model, t):
            return model.power_pv[t] + model.power_bat_out[t] - model.power_bat_in[t] - dict_power_load[t] ==\
                   model.power_grid_out[t] - model.power_grid_in[t]

        model.energy_balance_constraint = pyo.Constraint(model.t, rule=energy_balance_constraint)

        # Define objective function
        def objective_function(model):
            cost = sum(model.power_grid_out[t] * (dict_price[t] + dict_levi_pos[t])
                       - model.power_grid_in[t] * (dict_price[t] + dict_levi_neg[t]) for t in model.t)
            objective_expression = cost
            return objective_expression

        # Solve model
        model.objective = pyo.Objective(rule=objective_function, sense=pyo.maximize)  # minimize cost

        instance = model.create_instance()
        opt = pyo.SolverFactory(self.config_dict["solver"])
        opt.solve(instance, tee=False)

        for t in timesteps:
            self.mpc_table.loc[t, f'power_{self._get_list_plants(plant_type="bat")[0]}'] =\
                instance.power_bat_out[t].value - instance.power_bat_in[t].value
            self.mpc_table.loc[t, f'soc_{self._get_list_plants(plant_type="bat")[0]}'] = instance.soc_bat[t].value
            self.mpc_table.loc[t, f"power_out_{self.config_dict['id_meter_grid']}"] = instance.power_grid_out[t].value
            self.mpc_table.loc[t, f"power_in_{self.config_dict['id_meter_grid']}"] = instance.power_grid_in[t].value
            self.mpc_table.loc[t, f"power_{self.config_dict['id_meter_grid']}"] = instance.power_grid_out[t].value -\
                                                                                  instance.power_grid_in[t].value

        self.mpc_table[f'power_{self._get_list_plants(plant_type=["pv"])[0]}'] = np.array(dict_power_pv.values())

        self.mpc_table = self.mpc_table[self.mpc_table.index.isin(timesteps)]
        if self.save_output_result:
            self.mpc_table.to_csv(os.path.join(self.path_out, f'mpc_qtl_{self.count}.csv'))
        ft.write_dataframe(self.mpc_table, f"{self.path}/controller_mpc.ft")

    def market_agent_qtl(self, db_obj, clear_positions=False):

        timesteps = range(int(self.ts_delivery_current),
                          int(self.ts_delivery_current + 900 * self.config_dict["mpc_horizon"]), 900)

        df_potential_bids = ft.read_dataframe(f"{self.path}/controller_mpc.ft")
        df_potential_bids = df_potential_bids[df_potential_bids.index.isin(timesteps)]

        df_potential_bids['price_linear_desc'] = np.linspace(start=0.1, stop=0.01, num=len(df_potential_bids))

        df_potential_bids[f"power_{self.config_dict['id_meter_grid']}_quantiles"] = df_potential_bids.apply(
            lambda x: list(np.round(np.array(x[f"power_{self.config_dict['id_meter_grid']}"])
                                    * self.base_quantiles +
                                    np.array(x[f'power_{self._get_list_plants(plant_type=["pv"])[0]}_quantiles'])
                                    * (1 - self.base_quantiles), 0)), axis=1)

        if self.save_output_result:
            df_potential_bids.to_csv(os.path.join(self.path_out, f'mpc_after_qtl_{self.count}.csv'))
        ft.write_dataframe(df_potential_bids.reset_index(), f"{self.path}/controller_mpc.ft")

        df_potential_bids = df_potential_bids.explode([f"power_{self.config_dict['id_meter_grid']}_quantiles",
                                                       'price_quantiles', 'position_number'])
        df_potential_bids = pd.DataFrame(
            df_potential_bids[[f"power_{self.config_dict['id_meter_grid']}_quantiles",
                               'price_quantiles', 'position_number', 'price_linear_desc']])
        df_potential_bids.rename(columns={f"power_{self.config_dict['id_meter_grid']}_quantiles": "net_bids",
                                          "price_quantiles": "price"}, inplace=True)

        df_potential_bids['net_bids'] = df_potential_bids['net_bids'].fillna(0)
        df_potential_bids['net_bids'] /= 4
        df_potential_bids = df_potential_bids.astype({'price': 'float', 'net_bids': 'int', 'position_number': 'int',
                                                      'price_linear_desc': 'float'})
        df_potential_bids = df_potential_bids.set_index(['position_number'], append=True)

        ts = np.unique(np.array(df_potential_bids.index.get_level_values(level='timestamp')))
        price = np.linspace(start=0.01, stop=0.1, num=len(ts))
        df_potential_bids_temp = pd.DataFrame({'price': price, 'timestamp': ts}).set_index('timestamp')

        position_number = np.array([0, 1, 2, 3, 4, 5, 6, 7, 8])
        df_potential_bids_temp['position_number'] = np.tile(position_number[:int(self.base_quantiles.sum())],
                                             (len(df_potential_bids_temp), 1)).tolist()
        df_potential_bids_temp = df_potential_bids_temp.explode(['position_number']).set_index('position_number',
                                                                                               append=True)
        df_potential_bids['price_linear'] = df_potential_bids.index.map(df_potential_bids_temp['price'])
        df_potential_bids['price'] = df_potential_bids['price_linear'].fillna(df_potential_bids['price'])

        df_potential_bids_temp = pd.DataFrame(df_potential_bids['net_bids'], columns=['net_bids'])
        df_potential_bids_temp['net_bids_res'] = df_potential_bids_temp['net_bids']

        # matched bids
        if not self.matched_bids.empty:
            df_matched_bids = self.matched_bids
            df_matched_bids['position_number'] = np.where(df_matched_bids['id_user_bid'] == self.config_dict["id_user"],
                                                          df_matched_bids['number_position_bid'],
                                                          df_matched_bids['number_position_offer'])
            df_matched_bids = df_matched_bids.groupby(by=['ts_delivery',
                                                          'position_number']).agg({'net_bids': 'sum'}).reset_index()
            df_matched_bids.rename(columns={'ts_delivery': 'timestamp'}, inplace=True)
            df_matched_bids = df_matched_bids.sort_values(by=['timestamp', 'position_number'],
                                                          ascending=[True, True]).set_index('timestamp')
            df_matched_bids = df_matched_bids[df_matched_bids.index.isin(timesteps)]
            df_matched_bids = df_matched_bids.astype({'position_number': 'int', 'net_bids': 'int'})
            df_matched_bids = df_matched_bids.set_index('position_number', append=True)
            df_matched_bids = df_matched_bids[df_matched_bids.index.isin(df_potential_bids_temp.index)]

            df_potential_bids_temp['net_bids_res'] = df_potential_bids_temp['net_bids'] - df_matched_bids['net_bids']
            df_potential_bids_temp['net_bids_res'] = df_potential_bids_temp['net_bids_res'].fillna(
                df_potential_bids_temp['net_bids'])

        df_potential_bids_temp = df_potential_bids_temp.fillna(0).round(0).astype({'net_bids_res': 'int'})
        df_potential_bids_temp = df_potential_bids_temp[df_potential_bids_temp['net_bids_res'] != 0]

        df_positions = pd.DataFrame(columns=[db_obj.db_param.ID_USER, db_obj.db_param.QTY_ENERGY,
                                             db_obj.db_param.PRICE_ENERGY, db_obj.db_param.QUALITY_ENERGY,
                                             db_obj.db_param.PREMIUM_PREFERENCE_QUALITY, db_obj.db_param.TYPE_POSITION,
                                             db_obj.db_param.NUMBER_POSITION, db_obj.db_param.STATUS_POSITION,
                                             db_obj.db_param.T_SUBMISSION, db_obj.db_param.TS_DELIVERY,
                                             'price_linear_desc'],
                                    index=df_potential_bids_temp.index)
        euro_kwh_to_sigma_wh = db_obj.db_param.EURO_TO_SIGMA / 1000

        has_renewable = np.zeros(len(df_positions), dtype=bool)
        has_non_ren = np.zeros(len(df_positions), dtype=bool)
        for plant in self._get_list_plants("pv") + self._get_list_plants("fixedgen"):
            if self.plant_dict[plant]["quality"] in ["green_local"]:
                has_renewable = np.ones(len(df_positions), dtype=bool)
            if self.plant_dict[plant]["quality"] in ["local"]:
                has_non_ren = np.ones(len(df_positions), dtype=bool)

        df_positions[db_obj.db_param.ID_USER] = self.config_dict['id_market_agent']

        df_positions[db_obj.db_param.NUMBER_POSITION] = df_positions.index.get_level_values(level='position_number')

        df_positions[db_obj.db_param.QTY_ENERGY] = df_potential_bids_temp['net_bids_res']

        df_positions[db_obj.db_param.QTY_ENERGY] = np.where((~df_positions[db_obj.db_param.NUMBER_POSITION].isin([0]))
                                                            & (df_positions[db_obj.db_param.QTY_ENERGY] < 0),
                                                            0, df_positions[db_obj.db_param.QTY_ENERGY].abs())

        df_positions[db_obj.db_param.TYPE_POSITION] = np.where(df_potential_bids_temp['net_bids_res'] > 0, "offer", "bid")
        df_positions[db_obj.db_param.STATUS_POSITION] = 0
        df_positions[db_obj.db_param.QUALITY_ENERGY] = np.where(
            df_potential_bids_temp['net_bids_res'] < 0, self.config_dict["ma_preference_quality"],
            np.where(df_potential_bids_temp['net_bids_res'] > 0 & has_non_ren, "local",
                     np.where(df_potential_bids_temp['net_bids_res'] > 0 & has_renewable, 'green_local', "na")))
        df_positions[db_obj.db_param.PREMIUM_PREFERENCE_QUALITY] = np.where(
            df_potential_bids_temp['net_bids_res'] < 0, self.config_dict["ma_premium_preference_quality"], 0)
        df_positions[db_obj.db_param.T_SUBMISSION] = self.t_now
        df_positions[db_obj.db_param.TS_DELIVERY] = df_positions.index.get_level_values(level='timestamp')

        df_positions[db_obj.db_param.PRICE_ENERGY].fillna(df_potential_bids['price'], inplace=True)
        df_positions['price_linear_desc'].fillna(df_potential_bids['price_linear_desc'], inplace=True)
        df_positions[[db_obj.db_param.PRICE_ENERGY, 'price_linear_desc']] = df_positions[[db_obj.db_param.PRICE_ENERGY,
                                                                                          'price_linear_desc']].round(2)
        df_positions[db_obj.db_param.PRICE_ENERGY] = np.where(
            df_potential_bids_temp['net_bids_res'] < 0, df_positions['price_linear_desc'] * euro_kwh_to_sigma_wh,
            df_positions[db_obj.db_param.PRICE_ENERGY] * euro_kwh_to_sigma_wh)
        df_positions = df_positions.drop('price_linear_desc', axis=1)
        df_positions = df_positions[df_positions[db_obj.db_param.QTY_ENERGY] != 0]

        if clear_positions:
            db_obj.clear_positions(id_user=self.config_dict['id_market_agent'])
        if not df_positions.empty:
            if self.save_output_result:
                df_positions.to_csv(os.path.join(self.path_out, f'positions_qtl_{self.count}.csv'))
            db_obj.post_positions(df_positions, t_override=self.t_now)

    def plot_graph_before(self):

        timesteps = range(int(self.ts_delivery_current),
                          int(self.ts_delivery_current + 900 * self.config_dict["ma_horizon"]), 900)

        plt.style.use("seaborn-ticks")
        df_mpc = ft.read_dataframe(f"{self.path}/controller_mpc.ft").set_index("timestamp")
        df_mpc = df_mpc[df_mpc.index.isin(timesteps)].reset_index()

        df_mpc["matched_bid_pos"] = df_mpc['matched_bid_quantile'].apply(lambda x: np.sum(np.array(x).clip(min=0)))
        df_mpc["matched_bid_neg"] = df_mpc['matched_bid_quantile'].apply(lambda x: np.sum(np.array(x).clip(max=0)))

        df_mpc = df_mpc.fillna(0)
        df_mpc['grid_meter_pos'] = df_mpc[f"power_{self.config_dict['id_meter_grid']}_quantiles"].apply(lambda x: np.sum(np.array(x).clip(min=0)))
        df_mpc['grid_meter_neg'] = df_mpc[f"power_{self.config_dict['id_meter_grid']}_quantiles"].apply(lambda x: np.sum(np.array(x).clip(max=0)))

        df_mpc["bat_out_grid"] = df_mpc.apply(lambda x: np.sum(
            x[f"power_{self._get_list_plants(plant_type='bat')[0]}_out_{self.config_dict['id_meter_grid']}_quantiles"])
        if x[f'power_{self._get_list_plants(plant_type="bat")[0]}'] > 0 else 0, axis=1)

        pv_array = np.stack(df_mpc[f"power_{self._get_list_plants(plant_type=['pv'])[0]}"].values).T

        fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True, figsize=(20, 15), gridspec_kw={'height_ratios': [3, 1]})

        colors = sns.color_palette("inferno_r", 9, as_cmap=False, desat=0.8)
        colors.append(sns.color_palette("Blues_r", 1, as_cmap=False)[0])
        labels = ['pv quantile 1', 'pv quantile 2', 'pv quantile 3', 'pv quantile 4', 'pv quantile 5',
                  'pv quantile 6', 'pv quantile 7', 'pv quantile 8', 'pv quantile 9', 'battery discharge']

        colors_neg = sns.color_palette("Greens", 2, as_cmap=False, desat=0.8)
        labels_neg = ['household load', 'battery charge']

        graph_1 = ax1.stackplot(df_mpc['timestamp'], pv_array, df_mpc["bat_out_grid"], step="post", colors=colors,
                                labels=labels)
        graph_2 = ax1.stackplot(df_mpc['timestamp'], -df_mpc[f'power_{self._get_list_plants(plant_type="hh")[0]}'],
                                np.where(df_mpc[f'power_{self._get_list_plants(plant_type="bat")[0]}'] < 0,
                                         df_mpc[f'power_{self._get_list_plants(plant_type="bat")[0]}'], 0),
                                step="post", colors=colors_neg, labels=labels_neg)

        graph_3 = ax1.plot(df_mpc['timestamp'], df_mpc['grid_meter_pos'], color='black', drawstyle="steps-post",
                           label='grid meter pos')
        graph_4 = ax1.plot(df_mpc['timestamp'], df_mpc["grid_meter_neg"], color='black', drawstyle="steps-post",
                           linestyle='dashed', label='grid meter neg')
        graph_5 = ax1.plot(df_mpc['timestamp'], df_mpc['matched_bid_pos'], color='cyan', drawstyle="steps-post",
                           label='matched bids pos')
        graph_6 = ax1.plot(df_mpc['timestamp'], df_mpc["matched_bid_neg"], color='cyan', drawstyle="steps-post",
                           linestyle='dashed', label='matched bids neg')

        ax3 = ax1.twinx()
        graph_7 = ax3.plot(df_mpc['timestamp'], df_mpc["price_norm"], color='lightcoral', drawstyle="steps-post",
                           label='price', alpha=0.5)
        ax2.fill_between(df_mpc['timestamp'], df_mpc[f'soc_{self._get_list_plants(plant_type="bat")[0]}'],
                         color='steelblue', step='post', alpha=0.5)
        graph_8 = ax2.plot(df_mpc['timestamp'], df_mpc[f'soc_{self._get_list_plants(plant_type="bat")[0]}'],
                           color='steelblue', drawstyle="steps-post", label='battery soc')

        graph = graph_1 + graph_2 + graph_3 + graph_4 + graph_5 + graph_6 + graph_7 + graph_8
        labs = [l.get_label() for l in graph]
        ax1.legend(graph, labs, loc="upper center", ncol=6)
        ax1.grid()
        ax2.set_xlabel("Timestamp")
        ax1.set_ylabel(r"Power in W")
        ax2.set_ylabel(r"SOC in Wh")
        ax3.set_ylabel(r"Price")
        ax1.set_ylim(-int(self.plant_dict[self._get_list_plants(plant_type="bat")[0]]["power"]),
                     int(self.plant_dict[self._get_list_plants(plant_type="pv")[0]]["power"]*2))
        ax2.set_ylim(0, int(self.plant_dict[self._get_list_plants(plant_type="bat")[0]]["capacity"]))
        ax3.set_ylim(-1, 2)

        if self.save_output_result:
            plt.savefig(os.path.join(self.path_plot, f'{self.count}_plot_before.png'), bbox_inches='tight')
        plt.close(fig)
        # plt.show()

    def plot_graph_after(self):

        timesteps = range(int(self.ts_delivery_current),
                          int(self.ts_delivery_current + 900 * self.config_dict["ma_horizon"]), 900)

        # matched bids
        df_matched_bids = self.matched_bids
        if self.save_output_result:
            df_matched_bids.to_csv(os.path.join(self.path_out, f'bids_cleared_first_{self.count}.csv'))
        df_matched_bids['position_number'] = np.where(df_matched_bids['id_user_bid'] == self.config_dict["id_user"],
                                                      df_matched_bids['number_position_bid'],
                                                      df_matched_bids['number_position_offer'])

        if df_matched_bids.empty:
            df_matched_bids = pd.DataFrame(df_matched_bids, index=self.mpc_table.index)
            df_matched_bids['ts_delivery'] = df_matched_bids.index
            df_matched_bids = df_matched_bids.fillna(0)
            df_matched_bids['net_bids'] = 0
        df_matched_bids = df_matched_bids.groupby(by=['ts_delivery',
                                                      'position_number']).agg({'net_bids': 'sum'}).reset_index()
        df_matched_bids.rename(columns={'ts_delivery': 'timestamp'}, inplace=True)
        df_matched_bids = df_matched_bids.sort_values(by=['timestamp', 'position_number'],
                                                      ascending=[True, True]).set_index('timestamp')
        df_matched_bids = df_matched_bids[df_matched_bids.index.isin(timesteps)]
        df_matched_bids['net_bids'] *= 4
        df_matched_bids = df_matched_bids.astype({'position_number': 'int', 'net_bids': 'int'})
        # df_matched_bids.to_csv(os.path.join(self.path_out, f'bids_cleared_sec_{self.count}.csv'))

        plt.style.use("seaborn-ticks")
        df_mpc = ft.read_dataframe(f"{self.path}/controller_mpc.ft").set_index("timestamp")
        df_mpc = df_mpc[df_mpc.index.isin(timesteps)]

        if not df_matched_bids.empty:
            df_mpc["matched_bid_quantile"] = df_matched_bids.groupby(df_matched_bids.index).apply(
                lambda x: list(dict(dict.fromkeys(map(str, range(9)), 0), **dict(zip(x['position_number'].astype(str),
                                                                                     x['net_bids']))).values()))
            df_mpc['matched_bid_quantile'] = df_mpc['matched_bid_quantile'].fillna(
                df_mpc['zero_quantile'])
        else:
            df_mpc["matched_bid_quantile"] = df_mpc['zero_quantile']

        df_mpc = df_mpc.reset_index()
        # df_mpc.to_csv(os.path.join(self.path_out, f'bids_cleared_third_{self.count}.csv'))

        df_mpc["matched_bid_pos"] = df_mpc['matched_bid_quantile'].apply(lambda x: np.sum(np.array(x).clip(min=0)))
        df_mpc["matched_bid_neg"] = df_mpc['matched_bid_quantile'].apply(lambda x: np.sum(np.array(x).clip(max=0)))

        df_mpc = df_mpc.fillna(0)
        df_mpc['grid_meter_pos'] = df_mpc[f"power_{self.config_dict['id_meter_grid']}_quantiles"].apply(lambda x: np.sum(np.array(x).clip(min=0)))
        df_mpc['grid_meter_neg'] = df_mpc[f"power_{self.config_dict['id_meter_grid']}_quantiles"].apply(lambda x: np.sum(np.array(x).clip(max=0)))

        df_mpc["bat_out_grid"] = df_mpc.apply(lambda x: np.sum(
            x[f"power_{self._get_list_plants(plant_type='bat')[0]}_out_{self.config_dict['id_meter_grid']}_quantiles"])
        if x[f'power_{self._get_list_plants(plant_type="bat")[0]}'] > 0 else 0, axis=1)

        pv_array = np.stack(df_mpc[f"power_{self._get_list_plants(plant_type=['pv'])[0]}"].values).T

        fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True, figsize=(20, 15), gridspec_kw={'height_ratios': [3, 1]})

        colors = sns.color_palette("inferno_r", 9, as_cmap=False, desat=0.8)
        colors.append(sns.color_palette("Blues_r", 1, as_cmap=False)[0])
        labels = ['pv quantile 1', 'pv quantile 2', 'pv quantile 3', 'pv quantile 4', 'pv quantile 5',
                  'pv quantile 6', 'pv quantile 7', 'pv quantile 8', 'pv quantile 9', 'battery discharge']

        colors_neg = sns.color_palette("Greens", 2, as_cmap=False, desat=0.8)
        labels_neg = ['household load', 'battery charge']

        graph_1 = ax1.stackplot(df_mpc['timestamp'], pv_array, df_mpc["bat_out_grid"], step="post", colors=colors,
                                labels=labels)
        graph_2 = ax1.stackplot(df_mpc['timestamp'], -df_mpc[f'power_{self._get_list_plants(plant_type="hh")[0]}'],
                                np.where(df_mpc[f'power_{self._get_list_plants(plant_type="bat")[0]}'] < 0,
                                         df_mpc[f'power_{self._get_list_plants(plant_type="bat")[0]}'], 0),
                                step="post", colors=colors_neg, labels=labels_neg)

        graph_3 = ax1.plot(df_mpc['timestamp'], df_mpc['grid_meter_pos'], color='black', drawstyle="steps-post",
                           label='grid meter pos')
        graph_4 = ax1.plot(df_mpc['timestamp'], df_mpc["grid_meter_neg"], color='black', drawstyle="steps-post",
                           linestyle='dashed', label='grid meter neg')
        graph_5 = ax1.plot(df_mpc['timestamp'], df_mpc['matched_bid_pos'], color='cyan', drawstyle="steps-post",
                           label='matched bids pos')
        graph_6 = ax1.plot(df_mpc['timestamp'], df_mpc["matched_bid_neg"], color='cyan', drawstyle="steps-post",
                           linestyle='dashed', label='matched bids neg')

        ax3 = ax1.twinx()
        graph_7 = ax3.plot(df_mpc['timestamp'], df_mpc["price_norm"], color='lightcoral', drawstyle="steps-post",
                           label='price', alpha=0.5)
        ax2.fill_between(df_mpc['timestamp'], df_mpc[f'soc_{self._get_list_plants(plant_type="bat")[0]}'],
                         color='steelblue', step='post', alpha=0.5)
        graph_8 = ax2.plot(df_mpc['timestamp'], df_mpc[f'soc_{self._get_list_plants(plant_type="bat")[0]}'],
                           color='steelblue', drawstyle="steps-post", label='battery soc')

        graph = graph_1 + graph_2 + graph_3 + graph_4 + graph_5 + graph_6 + graph_7 + graph_8
        labs = [l.get_label() for l in graph]
        ax1.legend(graph, labs, loc="upper center", ncol=6)
        ax1.grid()
        ax2.set_xlabel("Timestamp")
        ax1.set_ylabel(r"Power in W")
        ax2.set_ylabel(r"SOC in Wh")
        ax3.set_ylabel(r"Price")
        ax1.set_ylim(-int(self.plant_dict[self._get_list_plants(plant_type="bat")[0]]["power"]),
                     int(self.plant_dict[self._get_list_plants(plant_type="pv")[0]]["power"]*2))
        ax2.set_ylim(0, int(self.plant_dict[self._get_list_plants(plant_type="bat")[0]]["capacity"]))
        ax3.set_ylim(-1, 2)

        if self.save_output_result:
            plt.savefig(os.path.join(self.path_plot, f'{self.count}_plot_after.png'), bbox_inches='tight')
        plt.close(fig)
        # plt.show()

    def controller_model_predictive(self, controller=None):
        """Execute the model predictive controller_real_time for the market participant given the predicted
        generation, consumption, and market prices for a configurable time horizon.

        The controller_real_time will always attempt to maximize its earnings. If no market price optimization is
        desired, a flat market price utilities should be input.

        :return: None
        """
        self.mpc_table = ft.read_dataframe(f"{self.path}/fcasts_current.ft").set_index("timestamp")

        # if no plants, revert to simple rule-based controller
        if controller is None and len(self._get_list_plants()) - len(self._get_list_plants(plant_type="hh")) == 0:
            controller = "hh"
        else:
            controller = "mpc"

        if controller == "mpc":
            # Declare the pyomo model
            model = pyo.ConcreteModel()
            # declare decision variables (vectors of same length as MPC horizon)
            # pv power variable

            if self._get_list_plants(plant_type="pv"):
                model.p_pv = pyo.Var(self._get_list_plants(plant_type="pv"),
                                     range(0, self.config_dict["mpc_horizon"]),
                                     domain=pyo.NonNegativeReals)

            if self._get_list_plants(plant_type="wind"):
                model.p_wind = pyo.Var(self._get_list_plants(plant_type="wind"),
                                       range(0, self.config_dict["mpc_horizon"]),
                                       domain=pyo.NonNegativeReals)

            # fixedgen power variable
            if self._get_list_plants(plant_type="fixedgen"):
                model.p_fixedgen = pyo.Var(self._get_list_plants(plant_type="fixedgen"),
                                           range(0, self.config_dict["mpc_horizon"]),
                                           domain=pyo.NonNegativeReals)

            # hp variable
            if self._get_list_plants(plant_type="hp"):
                model.p_hp = pyo.Var(self._get_list_plants(plant_type="hp"),
                                     range(0, self.config_dict["mpc_horizon"]),
                                     domain=pyo.NonPositiveReals)
                model.q_hp = pyo.Var(self._get_list_plants(plant_type="hp"),
                                     range(0, self.config_dict["mpc_horizon"]),
                                     domain=pyo.NonNegativeReals)
                model.p_hp_milp = pyo.Var(self._get_list_plants(plant_type="hp"),
                                          range(0, self.config_dict["mpc_horizon"]),
                                          domain=pyo.Binary)

                # thermal energy system variable, the number is the same as the number of hps
                model.q_tes_in = pyo.Var(self._get_list_plants(plant_type="hp"),
                                         range(self.config_dict["mpc_horizon"]),
                                         domain=pyo.NonNegativeReals)
                model.q_tes_out = pyo.Var(self._get_list_plants(plant_type="hp"),
                                          range(self.config_dict["mpc_horizon"]),
                                          domain=pyo.NonNegativeReals)
                model.q_tes_milp = pyo.Var(self._get_list_plants(plant_type="hp"),
                                           range(self.config_dict["mpc_horizon"]),
                                           domain=pyo.Binary)
                model.soc_tes = pyo.Var(self._get_list_plants(plant_type="hp"),
                                        range(self.config_dict["mpc_horizon"]),
                                        domain=pyo.NonNegativeReals)

                hp_param = {}
                for hp in self._get_list_plants(plant_type="hp"):
                    hp_param[hp] = pd.read_json(f"{self.path}/spec_{hp}.json")
                    for i in range(self.config_dict["mpc_horizon"]):
                        model.q_tes_in[hp, i].setub(float(self.plant_dict[hp]["power_th"]))
                        model.q_tes_out[hp, i].setub(float(self.plant_dict[hp]["power_th"]))
                        model.soc_tes[hp, i].setub(float(self.plant_dict[hp]["capacity"]))

            # battery power, absolute components
            if self._get_list_plants(plant_type="bat"):
                model.p_bat_in = pyo.Var(self._get_list_plants(plant_type="bat"),
                                         range(self.config_dict["mpc_horizon"]),
                                         domain=pyo.NonNegativeReals)
                model.p_bat_out = pyo.Var(self._get_list_plants(plant_type="bat"),
                                          range(self.config_dict["mpc_horizon"]),
                                          domain=pyo.NonNegativeReals)
                model.p_bat_milp = pyo.Var(self._get_list_plants(plant_type="bat"),
                                           range(self.config_dict["mpc_horizon"]),
                                           domain=pyo.Binary)
                model.soc_bat = pyo.Var(self._get_list_plants(plant_type="bat"),
                                        range(self.config_dict["mpc_horizon"]),
                                        domain=pyo.NonNegativeReals)

            for bat in self._get_list_plants(plant_type="bat"):
                for i in range(self.config_dict["mpc_horizon"]):
                    model.p_bat_in[bat, i].setub(self.plant_dict[bat]["power"])
                    model.p_bat_out[bat, i].setub(self.plant_dict[bat]["power"])
                    model.soc_bat[bat, i].setub(self.plant_dict[bat]["capacity"])

            # EV decision variables, absolute components
            if self._get_list_plants(plant_type="ev"):
                model.p_ev_in = pyo.Var(self._get_list_plants(plant_type="ev"),
                                        range(self.config_dict["mpc_horizon"]),
                                        domain=pyo.NonNegativeReals)
                model.p_ev_out = pyo.Var(self._get_list_plants(plant_type="ev"),
                                         range(self.config_dict["mpc_horizon"]),
                                         domain=pyo.NonNegativeReals)
                model.p_ev_milp = pyo.Var(self._get_list_plants(plant_type="ev"),
                                          range(self.config_dict["mpc_horizon"]),
                                          domain=pyo.Binary)
                model.soc_ev = pyo.Var(self._get_list_plants(plant_type="ev"),
                                       range(self.config_dict["mpc_horizon"]),
                                       domain=pyo.NonNegativeReals)
                # model.ev_slack = pyo.Var(self._get_list_plants(plant_type="ev"),
                #                          range(self.config_dict["mpc_horizon"]),
                #                          domain=pyo.NonNegativeReals)
                model.con_soc_ev = pyo.ConstraintList()

                model.con_p_ev_minus = pyo.ConstraintList()
                model.con_p_ev_plus = pyo.ConstraintList()

            dict_soc_ev_min = {}
            dict_soc_ev_old = {}
            for ev in self._get_list_plants(plant_type="ev"):
                with open(f"{self.path}/soc_{ev}.json", "r") as read_file:
                    dict_soc_ev_old[ev] = json.load(read_file)

                n_ev = self.plant_dict[ev]["efficiency"]
                dict_soc_ev_min[ev] = [0] * self.config_dict["mpc_horizon"]
                soc_potential = dict_soc_ev_old[ev]

                for i in range(self.config_dict["mpc_horizon"]):
                    # Compute the maximum possible distance that the EV can drive to return with a minimum SoC of 5 %. This
                    #   simulates that prosumers charge their car outside of home when driving longer distances
                    max_distance = (soc_potential - 0.05 * self.plant_dict[ev]["capacity"]) \
                                   / self.plant_dict[ev].get("consumption") * 100

                    # Save value to new column that contains the adjusted distances
                    self.mpc_table.loc[self.mpc_table.index[i], f"distance_driven_adjusted_{ev}"] = min(
                        self.mpc_table[f"distance_driven_{ev}"].iloc[i], max_distance)

                    # Add constraint that SoC can never surpass the maximum capacity of the vehicle
                    model.con_soc_ev.add(expr=model.soc_ev[ev, i] <= self.plant_dict[ev].get("capacity"))

                    # Subtract consumption based on the driven kilometers since last time step (0.25 --> 15 min --> 900 s)
                    soc_potential -= self.plant_dict[ev].get("consumption") \
                                     * self.mpc_table[f"distance_driven_adjusted_{ev}"].iloc[i] * 1 / 100

                    # Add charged energy since last time step (0.25 --> 15 min --> 900 s)
                    soc_potential += self.plant_dict[ev].get("charging_power") * 0.25 * n_ev \
                                     * self.mpc_table[f"availability_{ev}"].iloc[i]

                    # Keep charged energy between 5 % and 85 % of maximum capacity
                    soc_potential = max(min(soc_potential, 0.85 * self.plant_dict[ev].get("capacity")),
                                        0.05 * self.plant_dict[ev].get("capacity"))

                    # Case 1: currently not last time step, EV is available but will have left next time step
                    # Case 2: currently last time step, EV is available
                    # Action: Add constraint that SoC needs to be at least the potential SoC
                    if (i < self.config_dict["mpc_horizon"] - 1 and self.mpc_table[f"availability_{ev}"].iloc[i] == 1
                        and self.mpc_table[f"availability_{ev}"].iloc[i + 1] == 0) \
                        or (i == self.config_dict["mpc_horizon"] - 1
                            and self.mpc_table[f"availability_{ev}"].iloc[i] == 1):
                        model.con_soc_ev.add(expr=model.soc_ev[ev, i] >= soc_potential) # - model.ev_slack[ev, i])
                        dict_soc_ev_min[ev][i] = soc_potential

                    # Set availability and powers depending on if EV is available or not
                    if self.mpc_table[f"availability_{ev}"].iloc[i] == 1:
                        # Set if EV can charge or discharge
                        model.con_p_ev_minus.add(expr=model.p_ev_in[ev, i] <= 100000 * (1 - model.p_ev_milp[ev, i]))
                        model.con_p_ev_plus.add(expr=model.p_ev_out[ev, i] <= 100000 * model.p_ev_milp[ev, i])

                        # Set maximum charging and discharging power (upper boundaries)
                        model.p_ev_in[ev, i].setub(self.plant_dict[ev].get("charging_power"))

                        p_discharge = self.plant_dict[ev].get("charging_power") if self.plant_dict[ev].get("v2g") else 0
                        model.p_ev_out[ev, i].setub(p_discharge)

                    else:
                        model.con_p_ev_minus.add(expr=model.p_ev_in[ev, i] == 0)
                        model.con_p_ev_plus.add(expr=model.p_ev_out[ev, i] == 0)

                # Loop through mpc horizon backwards to calculate minimal SoC the EV must reach in each timestep
                # hold in order to be charged before departure
                for i in range(self.config_dict["mpc_horizon"] - 1, 0, -1):
                    if dict_soc_ev_min[ev][i] > 0:
                        dict_soc_ev_min[ev][i - 1] = dict_soc_ev_min[ev][i] - \
                                                     0.25 * self.plant_dict[ev]["charging_power"] * n_ev
                        dict_soc_ev_min[ev][i - 1] = max(dict_soc_ev_min[ev][i - 1], 0)

            # Add constraint that the SoC of each time step needs to be the SoC of the previous one plus the charge and
            #   minus the discharge and the consumption due to the driven distance
            model.con_soc_ev_calc = pyo.ConstraintList()
            for ev in self._get_list_plants(plant_type="ev"):
                n_ev = self.plant_dict[ev]["efficiency"]
                model.con_soc_ev_calc.add(expr=dict_soc_ev_old[ev]
                                               - 0.25 * model.p_ev_out[ev, 0] / n_ev
                                               + 0.25 * model.p_ev_in[ev, 0] * n_ev
                                               - (self.plant_dict[ev].get("consumption")
                                                  * self.mpc_table[f"distance_driven_adjusted_{ev}"].iloc[0]
                                                  * 1 / 100)
                                               == model.soc_ev[ev, 0])
                for t in range(1, self.config_dict["mpc_horizon"]):
                    model.con_soc_ev_calc.add(expr=model.soc_ev[ev, t - 1]
                                                   - 0.25 * model.p_ev_out[ev, t] / n_ev
                                                   + 0.25 * model.p_ev_in[ev, t] * n_ev
                                                   - (self.plant_dict[ev].get("consumption")
                                                      * self.mpc_table[f"distance_driven_adjusted_{ev}"].iloc[t]
                                                      * 1 / 100)
                                                   == model.soc_ev[ev, t])

            # Add variables for grid powerflow
            model.p_grid_out = pyo.Var(range(self.config_dict["mpc_horizon"]), domain=pyo.NonNegativeReals)
            model.p_grid_in = pyo.Var(range(self.config_dict["mpc_horizon"]), domain=pyo.NonNegativeReals)
            model.p_grid_milp = pyo.Var(range(self.config_dict["mpc_horizon"]), domain=pyo.Binary)

            # sum of household electrical loads
            p_load = [0] * self.config_dict["mpc_horizon"]
            for hh in self._get_list_plants(plant_type="hh"):
                for i, ts_d in enumerate(range(self.ts_delivery_current,
                                               self.ts_delivery_current + self.config_dict["mpc_horizon"] * 900, 900)):
                    p_load[i] += float(self.mpc_table.loc[ts_d, f"power_{hh}"])

            # sum of household thermal loads
            q_load = [0] * self.config_dict["mpc_horizon"]
            for hp in self._get_list_plants(plant_type="hp"):
                for i, ts_d in enumerate(range(self.ts_delivery_current,
                                               self.ts_delivery_current + self.config_dict["mpc_horizon"] * 900, 900)):
                    q_load[i] += float(self.mpc_table.loc[ts_d, f"heat_{hp}"])

            model.con_p_bat_bin = pyo.ConstraintList()
            for bat in self._get_list_plants(plant_type="bat"):
                for t in range(self.config_dict["mpc_horizon"]):
                    model.con_p_bat_bin.add(expr=model.p_bat_in[bat, t] <= 1000000 * (1 - model.p_bat_milp[bat, t]))
                    model.con_p_bat_bin.add(expr=model.p_bat_out[bat, t] <= 1000000 * model.p_bat_milp[bat, t])

            # Declare model constraints
            model.con_grid_bin = pyo.ConstraintList()
            for t in range(self.config_dict["mpc_horizon"]):
                model.con_grid_bin.add(expr=model.p_grid_in[t] <= 1000000 * (1 - model.p_grid_milp[t]))
                model.con_grid_bin.add(expr=model.p_grid_out[t] <= 1000000 * model.p_grid_milp[t])

            # define pv power upper bound from input file
            model.con_p_pv = pyo.ConstraintList()
            model.sum_pv = [0] * self.config_dict["mpc_horizon"]

            for plant in self._get_list_plants(plant_type=["pv"]):
                for t, t_d in enumerate(range(self.ts_delivery_current,
                                              self.ts_delivery_current + 900 * self.config_dict["mpc_horizon"], 900)):
                    model.sum_pv[t] += self.mpc_table.loc[t_d, f"power_{plant}"]
                    if self.plant_dict[plant].get("controllable"):
                        model.con_p_pv.add(expr=model.p_pv[plant, t] <= round(self.mpc_table.loc[t_d, f"power_{plant}"], 1))
                    else:
                        model.con_p_pv.add(expr=model.p_pv[plant, t] == round(self.mpc_table.loc[t_d, f"power_{plant}"], 1))

            # define wind power upper bound from input file
            model.con_p_wind = pyo.ConstraintList()
            model.sum_wind = [0] * self.config_dict["mpc_horizon"]

            for plant in self._get_list_plants(plant_type=["wind"]):
                for t, t_d in enumerate(range(self.ts_delivery_current,
                                              self.ts_delivery_current + 900 * self.config_dict["mpc_horizon"], 900)):
                    model.sum_wind[t] += self.mpc_table.loc[t_d, f"power_{plant}"]
                    if self.plant_dict[plant].get("controllable"):
                        model.con_p_wind.add(expr=model.p_wind[plant, t] <= round(self.mpc_table.loc[t_d, f"power_{plant}"], 1))
                    else:
                        model.con_p_wind.add(expr=model.p_wind[plant, t] == round(self.mpc_table.loc[t_d, f"power_{plant}"], 1))

            # define fixedgen power upper bound from input file
            model.con_p_fixedgen = pyo.ConstraintList()
            model.sum_fixedgen = [0] * self.config_dict["mpc_horizon"]
            for fixedgen in self._get_list_plants(plant_type="fixedgen"):
                for t, t_d in enumerate(range(self.ts_delivery_current,
                                              self.ts_delivery_current + 900 * self.config_dict["mpc_horizon"], 900)):
                    model.sum_fixedgen[t] += self.mpc_table.loc[t_d, f"power_{fixedgen}"]
                    if self.plant_dict[fixedgen].get("controllable"):
                        model.con_p_fixedgen.add(expr=model.p_fixedgen[fixedgen, t]
                                                      <= self.mpc_table.loc[t_d, f"power_{fixedgen}"])
                    else:
                        model.con_p_fixedgen.add(expr=model.p_fixedgen[fixedgen, t]
                                                      == self.mpc_table.loc[t_d, f"power_{fixedgen}"])

            # define the constraints for heat pump,
            model.con_hp = pyo.ConstraintList()
            for hp in self._get_list_plants(plant_type="hp"):
                temp_amb = self.mpc_table[f"temp_{hp}"].values - 273.15
                heatpump = HeatPump(hp_param[hp])
                t_in_secondary = self.plant_dict[hp]["temperature"] - 5
                hp_sim_res = heatpump.simulate(t_in_primary=temp_amb, t_in_secondary=t_in_secondary,
                                               t_amb=temp_amb, mode=1)
                hp_p = -1 * hp_sim_res['P_el']
                hp_cop = hp_sim_res['COP']

                for t, t_d in enumerate(range(self.ts_delivery_current,
                                              self.ts_delivery_current + 900 * self.config_dict["mpc_horizon"], 900)):
                    # model.con_hp.add(expr=model.p_hp[hp, t] <= hp_p[t] * model.p_hp_milp[hp, t])
                    model.con_hp.add(expr=model.p_hp[hp, t] >= hp_p[t])
                    model.con_hp.add(expr=model.q_hp[hp, t] == -1 * hp_cop[t] * model.p_hp[hp, t])

            # define initial thermal energy storage soc, determined using first term of thermal energy system power and
            # thermal energy system soc in the prev step
            model.con_tes_soc_calc = pyo.ConstraintList()
            model.con_tes_bin = pyo.ConstraintList()

            for hp in self._get_list_plants(plant_type="hp"):
                n_tes = self.plant_dict[hp]["efficiency"]
                with open(f"{self.path}/soc_{hp}.json", "r") as read_file:
                    soc_tes_init = json.load(read_file)
                model.con_tes_soc_calc.add(expr=soc_tes_init
                                                - 0.25 * model.q_tes_out[hp, 0] / n_tes
                                                + 0.25 * model.q_tes_in[hp, 0] * n_tes
                                                == model.soc_tes[hp, 0])
                for t in range(1, self.config_dict["mpc_horizon"]):
                    model.con_tes_soc_calc.add(expr=model.soc_tes[hp, t - 1]
                                                    - 0.25 * model.q_tes_out[hp, t] / n_tes
                                                    + 0.25 * model.q_tes_in[hp, t] * n_tes
                                                    == model.soc_tes[hp, t])

                #exclusivity for thermal energy system charging
                for t in range(self.config_dict["mpc_horizon"]):
                    model.con_tes_bin.add(expr=model.q_tes_in[hp, t] <= 1000000 * (1 - model.q_tes_milp[hp, t]))
                    model.con_tes_bin.add(expr=model.q_tes_out[hp, t] <= 1000000 * model.q_tes_milp[hp, t])

            # limit battery charging to pv generation
            model.con_batt_charge_grid = pyo.ConstraintList()
            for t in range(0, self.config_dict["mpc_horizon"]):
                expr_left = 0
                expr_right = model.sum_pv[t] + model.sum_wind[t]
                make_const = 0
                for bat in self._get_list_plants(plant_type="bat"):
                    if not self.plant_dict[bat].get("charge_from_grid"):
                        expr_left += model.p_bat_in[bat, t]
                        make_const = 1
                if make_const:
                    model.con_batt_charge_grid.add(expr=(expr_left <= expr_right))

            # define initial battery soc, determined using first term of battery power and battery soc in the prev step
            model.con_soc_calc = pyo.ConstraintList()

            for bat in self._get_list_plants(plant_type="bat"):
                n_bat = self.plant_dict[bat]["efficiency"]
                with open(f"{self.path}/soc_{bat}.json", "r") as read_file:
                    soc_bat_init = json.load(read_file)
                model.con_soc_calc.add(expr=soc_bat_init
                                            - 0.25 * model.p_bat_out[bat, 0] / n_bat
                                            + 0.25 * model.p_bat_in[bat, 0] * n_bat
                                            == model.soc_bat[bat, 0])
                for t in range(1, self.config_dict["mpc_horizon"]):
                    model.con_soc_calc.add(expr=model.soc_bat[bat, t - 1]
                                                - 0.25 * model.p_bat_out[bat, t] / n_bat
                                                + 0.25 * model.p_bat_in[bat, t] * n_bat
                                                == model.soc_bat[bat, t])

            # heat balance
            if self._get_list_plants(plant_type="hp"):
                model.con_heat_balance = pyo.ConstraintList()
                for _t in range(self.config_dict["mpc_horizon"]):
                    expression_heat_right = 0
                    expression_heat_left = q_load[_t]
                    for _hp in self._get_list_plants(plant_type="hp"):
                        expression_heat_left += model.q_tes_out[_hp, _t] - model.q_tes_in[_hp, _t] + model.q_hp[_hp, _t]
                    model.con_heat_balance.add(expr=(expression_heat_left == expression_heat_right))

            # power balance
            model.con_balance = pyo.ConstraintList()
            for _t in range(self.config_dict["mpc_horizon"]):
                expression_left = p_load[_t]
                for _pv in self._get_list_plants(plant_type="pv"):
                    expression_left += model.p_pv[_pv, _t]
                for _wind in self._get_list_plants(plant_type="wind"):
                    expression_left += model.p_wind[_wind, _t]
                for _bat in self._get_list_plants(plant_type="bat"):
                    expression_left += model.p_bat_out[_bat, _t] - model.p_bat_in[_bat, _t]
                for _ev in self._get_list_plants(plant_type="ev"):
                    expression_left += model.p_ev_out[_ev, _t] - model.p_ev_in[_ev, _t]
                for _fixedgen in self._get_list_plants(plant_type="fixedgen"):
                    expression_left += model.p_fixedgen[_fixedgen, _t]
                for _hp in self._get_list_plants(plant_type="hp"):
                    expression_left += model.p_hp[_hp, _t]
                expression_right = model.p_grid_out[_t] - model.p_grid_in[_t]
                model.con_balance.add(expr=(expression_left == expression_right))

            model.price = list(self.mpc_table["price"])
            model.price_levies_pos = list(self.mpc_table["price_energy_levies_positive"])
            model.price_levies_neg = list(self.mpc_table["price_energy_levies_negative"])

            # Define objective function
            def obj_rule(_model):
                step_obj = 0
                for j in range(0, self.config_dict["mpc_horizon"]):
                    #            component 1:   grid feed in valued at predicted price
                    #            component 2:   grid consumption valued at predicted price plus fixed levies
                    step_obj += _model.p_grid_out[j] * (-_model.price[j] + model.price_levies_pos[j]) \
                                + _model.p_grid_in[j] * (_model.price[j] + model.price_levies_neg[j])
                    # ensure non-degeneracy of the MILP
                    # cannot be used by GLPK as non-linear objectives cannot be solved
                    if self.config_dict["solver"] != "glpk":
                        step_obj += 5e-10 * _model.p_grid_out[j] * _model.p_grid_out[j] # -10 is where its at
                        step_obj += 5e-10 * _model.p_grid_in[j] * _model.p_grid_in[j]
                    # legacy check for electric vehicle constraint violation
                    # for item in self._get_list_plants(plant_type="ev"):
                    #     step_obj += _model.ev_slack[item, j] * 100000000
                return step_obj

            # Solve model
            model.objective_fun = pyo.Objective(rule=obj_rule, sense=pyo.minimize)
            pyo.SolverFactory(self.config_dict["solver"]).solve(model)
            # Update mpc_table with results of model
            dict_mpc_table = self.mpc_table.to_dict()
            for i, t_d in enumerate(range(self.ts_delivery_current,
                                          self.ts_delivery_current + 900 * self.config_dict["mpc_horizon"], 900)):
                # PV
                for pv in self._get_list_plants(plant_type="pv"):
                    dict_mpc_table[f"power_{pv}"][t_d] = model.p_pv[pv, i]()

                # Wind
                for wind in self._get_list_plants(plant_type="wind"):
                    dict_mpc_table[f"power_{wind}"][t_d] = model.p_wind[wind, i]()

                # Battery
                for bat in self._get_list_plants(plant_type="bat"):
                    dict_mpc_table[f"power_{bat}"][t_d] = model.p_bat_out[bat, i]() - model.p_bat_in[bat, i]()
                    dict_mpc_table[f"soc_{bat}"][t_d] = model.soc_bat[bat, i]()

                # EV
                for ev in self._get_list_plants(plant_type="ev"):
                    dict_mpc_table[f"power_{ev}"][t_d] = model.p_ev_out[ev, i]() - model.p_ev_in[ev, i]()
                    dict_mpc_table[f"soc_{ev}"][t_d] = model.soc_ev[ev, i]()
                    dict_mpc_table[f"soc_min_{ev}"][t_d] = min(dict_soc_ev_min[ev][i],
                                                               self.plant_dict[ev].get("capacity"))
                    # Temporary check for errors in the electric vehicle charging routine

                    # if model.ev_slack[ev, i]() >= 10**-6:
                    #     # for object name file1.
                    #     logfile = open(f"{'/'.join(self.path.split('/')[:-2])}/log.txt", "a")
                    #     logfile.write(f"Warning: User {self.config_dict['id_user']}'s EV #{ev} violated its charging "
                    #                   f"constraint at {self.t_now} resulting in a slack value of {model.ev_slack[ev, i]()} "
                    #                   f"in MPC step {i}")
                    #     logfile.close()
                # Fixed generation
                for fixedgen in self._get_list_plants(plant_type="fixedgen"):
                    dict_mpc_table[f"power_{fixedgen}"][t_d] = model.p_fixedgen[fixedgen, i]()

                # Heat pump
                for hp in self._get_list_plants(plant_type="hp"):
                    dict_mpc_table[f"power_{hp}"][t_d] = model.p_hp[hp, i]()
                    dict_mpc_table[f"soc_{hp}"][t_d] = model.soc_tes[hp, i]()

                # Grid power
                dict_mpc_table[f"power_{self.config_dict['id_meter_grid']}"][t_d] \
                    = model.p_grid_out[i]() - model.p_grid_in[i]()

            # Save results to file, which will be used as basis for controller_real_time set points and market trading
            self.mpc_table = pd.DataFrame.from_dict(dict_mpc_table)
        elif controller == "hh":
            # fixedgen power, sum of household loads
            p_load = [0] * self.config_dict["mpc_horizon"]
            for hh in self._get_list_plants(plant_type="hh"):
                for i, ts_d in enumerate(range(self.ts_delivery_current,
                                               self.ts_delivery_current + self.config_dict["mpc_horizon"] * 900, 900)):
                    p_load[i] += float(self.mpc_table.loc[ts_d, f"power_{hh}"])
            dict_mpc_table = self.mpc_table.to_dict()
            for i, t_d in enumerate(range(self.ts_delivery_current,
                                          self.ts_delivery_current + 900 * self.config_dict["mpc_horizon"], 900)):
                # Grid power
                dict_mpc_table[f"power_{self.config_dict['id_meter_grid']}"][t_d] = p_load[i]
            # Save results to file, which will be used as basis for controller_real_time set points and market trading
            self.mpc_table = pd.DataFrame.from_dict(dict_mpc_table)

        ft.write_dataframe(self.mpc_table.reset_index().rename(columns={"index": "timestamp"}),
                           f"{self.path}/controller_mpc.ft")
        ft.write_dataframe(self.mpc_table.reset_index().rename(columns={"index": "timestamp"}),
                           f"{self.path}/controller_mpc_{self.ts_delivery_current}.ft")

    def update_price_history(self, db_obj, market_type="ex_ante"):
        """Calculate price history from market results, save output to price_history.ft

        :param: none
        :return: none
        """
        euro_kwh_to_sigma_wh = db_obj.db_param.EURO_TO_SIGMA / 1000

        df_price_history = ft.read_dataframe(f"{self.path}/price_history.ft").set_index("timestamp")

        settlement_prices = db_obj.get_prices_settlement(
            ts_delivery_first=self.ts_delivery_prev - 24 * 3600,
            ts_delivery_last=self.ts_delivery_prev).set_index("ts_delivery")

        dict_price_history = \
            {"price_energy_balancing_positive":
                 settlement_prices.loc[self.ts_delivery_prev,
                                       db_obj.db_param.PRICE_ENERGY_BALANCING_POSITIVE] / euro_kwh_to_sigma_wh,
             "price_energy_balancing_negative":
                 settlement_prices.loc[self.ts_delivery_prev,
                                       db_obj.db_param.PRICE_ENERGY_BALANCING_NEGATIVE] / euro_kwh_to_sigma_wh,
             "price_energy_levies_positive":
                 settlement_prices.loc[self.ts_delivery_prev,
                                       db_obj.db_param.PRICE_ENERGY_LEVIES_POSITIVE] / euro_kwh_to_sigma_wh,
             "price_energy_levies_negative":
                 settlement_prices.loc[self.ts_delivery_prev,
                                       db_obj.db_param.PRICE_ENERGY_LEVIES_NEGATIVE] / euro_kwh_to_sigma_wh
             }

        if market_type == "ex_ante":
            # calculate price history if ex-ante market enabled
            # summarize market results
            market_results = self.matched_bids[self.matched_bids["ts_delivery"] == self.ts_delivery_prev]

            if self.config_dict["mpc_price_fcast"] == "flat":
                dict_price_history.update(
                    {"weighted_average_price": (self.config_dict["max_bid"] + self.config_dict["min_offer"]) / 2,
                     "total_energy_traded": 0
                     }
                )
            elif len(market_results) == 0:
                dict_price_history.update({"weighted_average_price": self.config_dict["max_bid"],
                                           "total_energy_traded": 0})
            else:
                total_energy = market_results[db_obj.db_param.QTY_ENERGY_TRADED].sum()
                total_paid = (market_results[db_obj.db_param.QTY_ENERGY_TRADED] * market_results[
                    db_obj.db_param.PRICE_ENERGY_MARKET_
                    + db_obj.lem_config["types_pricing_ex_ante"][0]]).sum()
                if total_energy != 0:
                    dict_price_history.update(
                        {"weighted_average_price": total_paid / total_energy / euro_kwh_to_sigma_wh,
                         "total_energy_traded": total_energy})
                else:
                    dict_price_history.update({"weighted_average_price": self.config_dict["max_bid"],
                                               "total_energy_traded": 0})
            df_price_history.loc[self.ts_delivery_prev] = dict_price_history
        else:
            market_results = db_obj.get_results_market_ex_post(
                ts_delivery_first=self.ts_delivery_prev - 24 * 3600,
                ts_delivery_last=self.ts_delivery_prev).set_index("ts_delivery")

            if self.config_dict["mpc_price_fcast"] == "flat":
                dict_price_history.update({
                    "weighted_average_price": (self.config_dict["max_bid"] - self.config_dict["min_offer"]) / 2,
                    "total_energy_traded": 0})
            elif self.ts_delivery_prev in market_results.index:
                dict_price_history.update({
                    "weighted_average_price":
                        market_results.loc[self.ts_delivery_prev, db_obj.db_param.PRICE_ENERGY_MARKET_
                                           + db_obj.lem_config["types_pricing_ex_post"][0]] / euro_kwh_to_sigma_wh,
                    "total_energy_traded": 0})
            else:
                dict_price_history.update({
                    "weighted_average_price": self.config_dict["max_bid"],
                    "total_energy_traded": 0})
            df_price_history.loc[self.ts_delivery_prev] = dict_price_history

        # return most recent settlement prices
        # these are considered during MPC planning
        ft.write_dataframe(df_price_history.reset_index(), f"{self.path}/price_history.ft")

    def get_market_results(self, db_obj, market_type="ex_ante"):
        """Query and return currently matched and unmatched market positions of the market
        participant in question.

        :param db_obj: Database instance, pass the database connection instance to this method
        :param market_type:
        :return: none
        """
        if market_type == "ex_ante":
            self.matched_bids, self.matched_bids_by_timestep = db_obj.get_results_market_ex_ante(
                id_user=self.config_dict['id_market_agent'],
                ts_delivery_first=self.ts_delivery_prev,
                ts_delivery_last=self.ts_delivery_current + self.config_dict["ma_horizon"] * 900
            )
            self.update_price_history(db_obj, market_type="ex_ante")
        else:
            self.matched_bids, self.matched_bids_by_timestep = None, None
            self.update_price_history(db_obj,
                                      market_type="ex_post")

    def update_user_preferences(self, db_obj):
        user_info = db_obj.get_info_user(self.config_dict["id_user"])

        self.config_dict["max_bid"] = \
            float(user_info.loc[0, db_obj.db_param.PRICE_ENERGY_BID_MAX]) \
            / db_obj.db_param.EURO_TO_SIGMA * 1000
        self.config_dict["min_offer"] = \
            float(user_info.loc[0, db_obj.db_param.PRICE_ENERGY_OFFER_MIN]) \
            / db_obj.db_param.EURO_TO_SIGMA * 1000
        self.config_dict["ma_strategy"] = \
            user_info.loc[0, db_obj.db_param.STRATEGY_MARKET_AGENT]
        self.config_dict["ma_horizon"] = \
            int(user_info.loc[0, db_obj.db_param.HORIZON_TRADING])
        self.config_dict["ma_preference_quality"] = \
            user_info.loc[0, db_obj.db_param.PREFERENCE_QUALITY]
        self.config_dict["id_market_agent"] = \
            user_info.loc[0, db_obj.db_param.ID_MARKET_AGENT]
        self.config_dict["ma_premium_preference_quality"] = \
            float(user_info.loc[0, db_obj.db_param.PREMIUM_PREFERENCE_QUALITY])
        with open(f"{self.path}/config_account.json", "w") as write_file:
            json.dump(self.config_dict, write_file)

    def set_target_grid_power(self, market_type="ex_ante"):
        """Determine and save the controller_real_time setpoint for the real time controller_real_time to a .ft
        file. In the Strommunity setup, this method must only be called after a re-optimization. In the DSA setup, this
        method should be called after each market clearing, immediately before the beginning of a new market interval.

        :return: None
        """
        df_target_grid_power = ft.read_dataframe(f"{self.path}/controller_mpc.ft").set_index("timestamp")

        if market_type == "ex_post":
            '''When operating in a Strommunity market design, the target grid power is
            set simply to the power calculated by the mpc algorithm.
            '''
        elif market_type == "ex_ante":
            '''When operating in a double-sided electricity market,
               the target grid power is set based on the market results.
               '''
            df_target_grid_power[f"power_{self.config_dict['id_meter_grid']}"] = \
                self.matched_bids_by_timestep["net_bids"] * 4

        ft.write_dataframe(df_target_grid_power.reset_index(),
                           f"{self.path}/target_grid_power.ft")

    def market_agent(self, db_obj, clear_positions=False):
        """Calculate and post/update market positions to the double sided market.

        :param db_obj: Database instance, pass the database connection instance to this method

        :return:
        """
        # generate list of potential bids from MPC results
        # all grid flows are potential bids

        df_potential_bids = pd.DataFrame(
            self.mpc_table[(self.ts_delivery_current <= self.mpc_table.index)
                           & (self.mpc_table.index <= self.ts_delivery_current
                              + 15 * 60 * self.config_dict["ma_horizon"])]
            [f"power_{self.config_dict['id_meter_grid']}"]) / 4

        df_potential_bids.rename(columns={f"power_{self.config_dict['id_meter_grid']}": "net_bids"}, inplace=True)
        df_potential_bids["net_bids"] = df_potential_bids["net_bids"] - self.matched_bids_by_timestep["net_bids"]

        dict_pot_bids = df_potential_bids.to_dict()
        index_pot_bids = sorted(list(dict_pot_bids["net_bids"]))

        dict_positions = {
            db_obj.db_param.ID_USER: [],
            db_obj.db_param.QTY_ENERGY: [],
            db_obj.db_param.PRICE_ENERGY: [],
            db_obj.db_param.QUALITY_ENERGY: [],
            db_obj.db_param.PREMIUM_PREFERENCE_QUALITY: [],
            db_obj.db_param.TYPE_POSITION: [],
            db_obj.db_param.NUMBER_POSITION: [],
            db_obj.db_param.STATUS_POSITION: [],
            db_obj.db_param.T_SUBMISSION: [],
            db_obj.db_param.TS_DELIVERY: []}
        euro_kwh_to_sigma_wh = db_obj.db_param.EURO_TO_SIGMA / 1000

        delta = 0
        gradient = (self.config_dict["max_bid"] - self.config_dict["min_offer"]) / (
                self.config_dict["ma_horizon"] - 1)

        for t_s in index_pot_bids:
            """Market logic to be inserted at this point. In order to be posted, bids must be assigned prices"""
            if t_s > self.ts_delivery_current:
                # determine energy to be traded
                energy_position = round(dict_pot_bids["net_bids"][t_s])
                if abs(energy_position) >= 10:
                    post_position = True
                else:
                    post_position = False

                has_renewable = False
                has_non_ren = False
                for plant in self._get_list_plants("pv") + self._get_list_plants("fixedgen"):
                    if self.plant_dict[plant]["quality"] in ["green_local"]:
                        has_renewable = True
                    if self.plant_dict[plant]["quality"] in ["local"]:
                        has_non_ren = True
                if energy_position < 0:
                    quality = self.config_dict["ma_preference_quality"]
                    premium = self.config_dict["ma_premium_preference_quality"]
                elif energy_position > 0 and has_non_ren:
                    quality = "local"
                    premium = 0
                elif energy_position > 0 and has_renewable:
                    quality = "green_local"
                    premium = 0
                else:
                    quality = "na"
                    premium = 0

                if self.config_dict["ma_strategy"] == "zi":
                    # determine energy price,
                    price = self.config_dict["min_offer"] \
                            + random() * (self.config_dict["max_bid"] - self.config_dict["min_offer"])
                    price = round(price, 4)
                    price *= euro_kwh_to_sigma_wh

                else:
                    if energy_position < 0:
                        price = round(self.config_dict["max_bid"] - delta, 6)
                        price *= euro_kwh_to_sigma_wh
                    else:
                        price = round(self.config_dict["min_offer"] + delta, 6)
                        price *= euro_kwh_to_sigma_wh

                if post_position:
                    dict_positions[db_obj.db_param.ID_USER].append(self.config_dict['id_market_agent'])
                    dict_positions[db_obj.db_param.QTY_ENERGY].append(abs(energy_position))
                    dict_positions[db_obj.db_param.TYPE_POSITION].append("offer" if energy_position > 0 else "bid")
                    dict_positions[db_obj.db_param.NUMBER_POSITION].append(0)
                    dict_positions[db_obj.db_param.STATUS_POSITION].append(0)
                    dict_positions[db_obj.db_param.PRICE_ENERGY].append(price)
                    dict_positions[db_obj.db_param.QUALITY_ENERGY].append(quality)
                    dict_positions[db_obj.db_param.PREMIUM_PREFERENCE_QUALITY].append(premium)
                    dict_positions[db_obj.db_param.T_SUBMISSION].append(self.t_now)
                    dict_positions[db_obj.db_param.TS_DELIVERY].append(t_s)
                delta += gradient
        if clear_positions:
            db_obj.clear_positions(id_user=self.config_dict['id_market_agent'])
        if len(dict_positions[db_obj.db_param.ID_USER]) > 0:
            df_bids = pd.DataFrame(dict_positions)
            db_obj.post_positions(df_bids,
                                  t_override=self.t_now)

    def _get_list_plants(self, plant_type=None):
        list_plants = []
        if type(plant_type) is list:
            for plant in self.config_dict["list_plants"]:
                if self.plant_dict[plant].get("type") in plant_type and self.plant_dict[plant].get("activated") is True:
                    list_plants.append(plant)
        elif type(plant_type) is str:
            for plant in self.config_dict["list_plants"]:
                if self.plant_dict[plant].get("type") == plant_type and self.plant_dict[plant].get("activated") is True:
                    list_plants.append(plant)
        else:
            list_plants = self.config_dict["list_plants"]
        return list_plants

    def _get_old_meter_reading_local(self, id_meter):
        with open(f"{self.path}/meter_{id_meter}.json", "r") as read_file:
            reading = json.load(read_file)
        return [reading[0], reading[1]]

    def _set_new_meter_reading_local(self, dict_new_readings):
        dict_buffer_meter_readings = ft.read_dataframe(f"{self.path}/buffer_meter_readings.ft").to_dict()
        for id_meter in dict_new_readings:
            if id_meter == self.config_dict["id_meter_grid"] \
                    or self.plant_dict[id_meter].get("has_submeter") is not False:
                reading_old = self._get_old_meter_reading_local(id_meter)
                energy_in_cum_new = dict_new_readings[id_meter][0] + reading_old[0]
                energy_out_cum_new = dict_new_readings[id_meter][1] + reading_old[1]

                rand_late = random() + self.config_dict["meter_prob_late"]
                if rand_late > 1:
                    time_late = abs(np.random.normal(0, self.config_dict["meter_prob_late_95"] / 2, 1)[0])
                else:
                    time_late = 0

                with open(f"{self.path}/meter_{id_meter}.json",
                          "w") as write_file:
                    json.dump([energy_in_cum_new, energy_out_cum_new], write_file)

                index = len(dict_buffer_meter_readings["t_reading"])

                dict_buffer_meter_readings["t_reading"][index] = self.ts_delivery_current
                dict_buffer_meter_readings["energy_in_cum"][index] = energy_in_cum_new
                dict_buffer_meter_readings["energy_out_cum"][index] = energy_out_cum_new
                dict_buffer_meter_readings["id_meter"][index] = id_meter
                dict_buffer_meter_readings["t_send"][index] = int(self.ts_delivery_current + time_late)

        df_meter_readings = pd.DataFrame.from_dict(dict_buffer_meter_readings)
        # load meter readings file
        if random() + self.config_dict["meter_prob_missing"] <= 1:
            ft.write_dataframe(df_meter_readings,
                               f"{self.path}/buffer_meter_readings.ft")

    # Internal methods and functions

    @staticmethod
    def _decomp_float(float_in, return_val="pos", dec_places=0):
        """
        Static internal method:
        Decompose float into positive and negative components. Returns one of the two components.

        :param float_in: float to be decomposed.
        :param return_val: if "pos" return positive component. else if "neg" return negative component.
        :param dec_places: number of decimal places to round the return value to

        :return: positive or negative component of the input value
        """

        if float_in >= 0:
            pos_comp = round(float_in, dec_places)
            neg_comp = 0
        else:
            pos_comp = 0
            neg_comp = round(float_in, dec_places)
        if return_val == "pos":
            return abs(pos_comp)
        return abs(neg_comp)

    @staticmethod
    def _lookup(x, x_axis, y_axis):
        """
        Static internal method:
        Perform lookup on provided table. Find y-value for desired x-value

        :param x: x-value to look up
        :param x_axis: x-axis of lookup table
        :param y_axis: y-value of lookup table

        :return: float, y-value corresponding to x-value input
        """
        if x <= x_axis[0]:
            return y_axis[0]
        if x >= x_axis[-1]:
            return y_axis[-1]

        i = bisect_left(x_axis, x)
        k = (x - x_axis[i - 1]) / (x_axis[i] - x_axis[i - 1])
        y = k * (y_axis[i] - y_axis[i - 1]) + y_axis[i - 1]
        return y


class HeatPump:
    """
    Heat pump model calculating the outputs one of 6 different generic heat pump types.
    Model source:

    [Group ID]: Count	    Reglulated	On-Off
    Outdoor Air / Water	      [1] 	     [4]
    Brine / Water	          [2]	     [5]
    Water / Water	          [3]	     [6]

    Public methods:
    --------------
    __init__ : Create an instance of the HeatPump class from a pre-defined parameter groups to be used for simulation
    simulate : Performs the simulation of the heat pump model

    """

    def __init__(self, parameters: pd.DataFrame):
        self.group_id = float(parameters['Group'].array[0])
        self.p1_p_el_h = float(parameters['p1_P_el_h [1/°C]'].array[0])
        self.p2_p_el_h = float(parameters['p2_P_el_h [1/°C]'].array[0])
        self.p3_p_el_h = float(parameters['p3_P_el_h [-]'].array[0])
        self.p4_p_el_h = float(parameters['p4_P_el_h [1/°C]'].array[0])
        self.p1_cop = float(parameters['p1_COP [-]'].array[0])
        self.p2_cop = float(parameters['p2_COP [-]'].array[0])
        self.p3_cop = float(parameters['p3_COP [-]'].array[0])
        self.p4_cop = float(parameters['p4_COP [-]'].array[0])
        self.p_el_ref = float(parameters['P_el_h_ref [W]'].array[0])
        self.p_th_ref = float(parameters['P_th_h_ref [W]'].array[0])
        try:
            self.p1_eer = parameters['p1_EER [-]'].array[0]
            self.p2_eer = parameters['p2_EER [-]'].array[0]
            self.p3_eer = parameters['p3_EER [-]'].array[0]
            self.p4_eer = parameters['p4_EER [-]'].array[0]
            self.p1_p_el_c = parameters['p1_P_el_c [1/°C]'].array[0]
            self.p2_p_el_c = parameters['p2_P_el_c [1/°C]'].array[0]
            self.p3_p_el_c = parameters['p3_P_el_c [-]'].array[0]
            self.p4_p_el_c = parameters['p4_P_el_c [1/°C]'].array[0]
            self.p_el_col_ref = parameters['P_el_c_ref [W]'].array[0]
        except:
            self.p1_eer = np.nan
            self.p2_eer = np.nan
            self.p3_eer = np.nan
            self.p4_eer = np.nan
            self.p1_p_el_c = np.nan
            self.p2_p_el_c = np.nan
            self.p3_p_el_c = np.nan
            self.p4_p_el_c = np.nan
            self.p_el_col_ref = np.nan

        self.delta_t = 5  # Inlet temperature is supposed to be heated up by 5 K
        self.cp = 4200  # J/(kg*K), specific heat capacity of water

    def simulate(self, t_in_primary: Union[float, np.ndarray], t_in_secondary: Union[float, np.ndarray],
                 t_amb: Union[float, np.ndarray], mode: int = 1, p_th_min: Union[float, np.ndarray] = 0) -> dict:
        """
        Performs the simulation of the heat pump model.

        Parameters
        ----------
        t_in_primary : numeric or iterable (e.g. pd.Series)
            Input temperature on primry side :math:`T` (air, brine, water). [°C]
        t_in_secondary : numeric or iterable (e.g. pd.Series)
            Input temperature on secondary side :math:`T` from heating storage or system. [°C]
        t_amb : numeric or iterable (e.g. pd.Series)
            Ambient temperature :math:'T' of the air. [°C]
        mode : int
            for heating: 1, for cooling: 2
        p_th_min : Minimum thermal power output [W]. Inverter heat pumps increase electrical Power input. At maximum electrical input a electrical heating rod turns on.

        Returns
        -------
        result : dict
            with the following columns
            T_in = Input temperature :math:`T` at primary side of the heat pump. [°C]
            T_out = Output temperature :math:`T` at secondary side of the heat pump. [°C]
            T_amb = Ambient / Outdoor temperature :math:`T`. [°C]
            COP = Coefficient of Performance.
            EER = Energy Efficiency Ratio.
            P_el = Electrical input Power. [W]
            P_th = Thermal output power. [W]
            m_dot = Mass flow at secondary side of the heat pump. [kg/s]
        """

        if mode == 2 and self.group_id > 1:
            raise ValueError('Cooling is only possible with heat pumps of group id = 1.')

        t_in = t_in_primary  # info value for dataframe
        if mode == 1:
            t_out = t_in_secondary + self.delta_t  # Inlet temperature is supposed to be heated up by 5 K
            eer = 0
        if mode == 2:  # Inlet temperature is supposed to be cooled down by 5 K
            t_out = t_in_secondary - self.delta_t
            cop = 0
        # for subtype = air/water heat pump
        if self.group_id in (1, 4):
            t_amb = t_in
        t_ambient = t_amb
        # for regulated heat pumps
        if self.group_id in (1, 2, 3):
            if mode == 1:
                cop = self.p1_cop * t_in + self.p2_cop * t_out + self.p3_cop + self.p4_cop * t_amb
                p_el = self.p_el_ref * (self.p1_p_el_h * t_in
                                        + self.p2_p_el_h * t_out
                                        + self.p3_p_el_h
                                        + self.p4_p_el_h * t_amb)
                if self.group_id == 1:
                    if isinstance(t_in, np.ndarray):
                        t_in = np.full_like(t_in, -7)
                    else:
                        t_in = -7
                    t_amb = t_in

                elif self.group_id == 2:
                    if isinstance(t_amb, np.ndarray):
                        t_amb = np.full_like(t_amb, -7)
                    else:
                        t_amb = -7
                p_el_25 = 0.25 * self.p_el_ref * (self.p1_p_el_h * t_in
                                                  + self.p2_p_el_h * t_out
                                                  + self.p3_p_el_h
                                                  + self.p4_p_el_h * t_amb)
                if isinstance(p_el, np.ndarray):
                    p_el = np.where(p_el < p_el_25, p_el_25, p_el)
                elif p_el < p_el_25:
                    p_el = p_el_25

                p_th = p_el * cop

                if isinstance(cop, np.ndarray):
                    # turn on heating rod and compressor
                    p_el = np.where((cop > 1) & (p_th < p_th_min) & (self.p_el_ref < p_th_min / cop),
                                    self.p_el_ref + self.p_th_ref, p_el)
                    p_th = np.where((cop > 1) & (p_th < p_th_min) & (self.p_el_ref < p_th_min / cop),
                                    self.p_el_ref * cop + self.p_th_ref, p_th)
                    # increase electrical power for compressor
                    p_el = np.where((cop > 1) & (p_th < p_th_min) & (self.p_el_ref > p_th_min / cop),
                                    p_th_min / cop, p_el)
                    p_th = np.where((cop > 1) & (p_th < p_th_min) & (self.p_el_ref > p_th_min / cop), p_th_min,
                                    p_th)
                    # only turn on heating rod
                    p_el = np.where(cop <= 1, self.p_th_ref, p_el)
                    p_th = np.where(cop <= 1, self.p_th_ref, p_th)
                    cop = p_th / p_el
                else:
                    if cop <= 1:
                        cop = 1
                        p_el = self.p_th_ref
                        p_th = self.p_th_ref
                    elif p_th < p_th_min:
                        if self.p_el_ref > p_th_min / cop:
                            p_el = p_th_min / cop
                            p_th = p_th_min
                        else:
                            p_el = self.p_el_ref + self.p_th_ref
                            p_th = self.p_el_ref * cop + self.p_th_ref
                            cop = p_th / p_el

            if mode == 2:
                eer = (self.p1_eer * t_in + self.p2_eer * t_out + self.p3_eer + self.p4_eer * t_amb)
                if isinstance(t_in, np.ndarray):
                    t_in = np.where(t_in < 25, 25, t_in)
                elif t_in < 25:
                    t_in = 25
                t_amb = t_in
                p_el = (
                               self.p1_p_el_c * t_in + self.p2_p_el_c * t_out + self.p3_p_el_c + self.p4_p_el_c * t_amb) * self.p_el_col_ref
                if isinstance(p_el, np.ndarray):
                    eer = np.where(p_el < 0, 0, eer)
                    p_el = np.where(p_el < 0, 0, p_el)
                elif p_el < 0:
                    eer = 0
                    p_el = 0
                p_th = -(eer * p_el)
                if isinstance(eer, np.ndarray):
                    p_el = np.where(eer <= 1, 0, p_el)
                    p_th = np.where(eer <= 1, 0, p_th)
                    eer = np.where(eer <= 1, 0, eer)
                elif eer < 1:
                    eer = 0
                    p_el = 0
                    p_th = 0

        # for subtype = On-Off
        elif self.group_id in (4, 5, 6):
            p_el = (self.p1_p_el_h * t_in
                    + self.p2_p_el_h * t_out
                    + self.p3_p_el_h
                    + self.p4_p_el_h * t_amb) * self.p_el_ref

            cop = self.p1_cop * t_in + self.p2_cop * t_out + self.p3_cop + self.p4_cop * t_amb

            p_th = p_el * cop

            if isinstance(cop, np.ndarray):
                p_el = np.where((cop > 1) & (p_th < p_th_min), p_el + self.p_th_ref, p_el)
                p_th = np.where((cop > 1) & (p_th < p_th_min), p_th + self.p_th_ref, p_th)
                p_el = np.where(cop <= 1, self.p_th_ref, p_el)
                p_th = np.where(cop <= 1, self.p_th_ref, p_th)
                cop = p_th / p_el

            else:
                if cop <= 1:
                    cop = 1
                    p_el = self.p_th_ref
                    p_th = self.p_th_ref
                elif p_th < p_th_min:
                    p_th = p_th + self.p_th_ref
                    p_el = p_el + self.p_th_ref
                    cop = p_th / p_el

        # massflow
        m_dot = abs(p_th / (self.delta_t * self.cp))

        # round
        result = dict()

        result['T_in'] = t_in_primary
        result['T_out'] = t_out
        result['T_amb'] = t_ambient
        result['COP'] = cop
        result['EER'] = eer
        result['P_el'] = p_el
        result['P_th'] = p_th
        result['m_dot'] = m_dot

        return result
