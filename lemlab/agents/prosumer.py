__author__ = "sdlumpp"
__credits__ = []
__license__ = ""
__maintainer__ = "sdlumpp"
__email__ = "sebastian.lumpp@tum.de"

import json
import feather as ft
import pandas as pd
import numpy as np
import pyomo.environ as pyo
from random import random
import lemlab.forecasting.forecasting as fcast
import warnings


class Prosumer:
    """Prosumer defines objects and methods used to simulate a single family home in a local energy market

       This class is intended for use in conjunction with the Retailer and Simulation classes. In addition, the
       functionality of the Platform/clearing_ex_ante.py module is required in order for market clearing to be enacted.

       A single family home can simulate the following physical plants, depending on the selected configuration:
                        1)  Fixed household loads with or without electric vehicles
                        2)  PV plants with or without battery storage
                        3)  Market agents for trading in double sided electricity markets

       Instances of the Prosumer class are created and destroyed once each Simulation step.

        Public methods:

        __init__ :       Create an instance of the Prosumer class from a configuration folder created using the
                         Simulation class

        controller_real_time:      Calculate the behaviour of the instance in the previous time step by applying a
                            selected controller_real_time strategy to measurement data and plant specifications.

        log_meter_values: Log the result of the controller_real_time method to the database as metering data.

        get_predictions: Return generation prediction for PV plants, consumption predictions for fixed loads, as well
                         as well as market price predictions.

        controller_model_predictive: Execute the model predictive controller_real_time for the market participant given
                         the predicted generation, consumption, and market prices for a configurable time horizon.


        get_market_positions: Query and return currently matched and unmatched market positions of the market
                                 participant in question

        set_target_grid_power: Set the target power (to/from grid) for the next controller_real_time execution. Either
                    MPC result for the Strommunity market design, or market result for double sided market simulations.

        market_agent: Calculate and post/update market positions to the double sided market

        retrain_forecasts: retrains the forecasting model
    """

    def __init__(self, path, t_override=None):
        """Create a Prosumer instance from a configuration folder created using the Simulation class.

        :param path: path to prosumer configuration directory
        :param t_override: pandas Timestamp, if supplied, this parameter forces the MP to use the supplied
                            timestamp, otherwise the current time is used.
        """
        # set current timestamp from system clock or keyword arg
        self.t_now = t_override if t_override else pd.Timestamp.now().timestamp()
        # derive previous and next timestamps
        self.ts_delivery_prev = round(pd.Timestamp(self.t_now, unit="s").floor("15min").timestamp() - 15*60)
        self.ts_delivery_current = self.ts_delivery_prev + 15*60
        self.path = path
        with open(f"{self.path}/config_account.json", "r") as read_file:
            self.config_dict = json.load(read_file)

        with open(f"{self.path}/config_plants.json", "r") as read_file:
            self.plant_dict = json.load(read_file)

        self.meas_val = {"timestamp": self.ts_delivery_prev}

        # initialize instance dataframes to be used in later methods
        # df containing all MPC inputs and results for the MP
        self.mpc_table = None
        # df containing matched market results for the MP
        self.matched_bids = None
        # df containing net matched market volumes by timestep (multiple matched offers for each timestamp summated)
        self.matched_bids_by_timestep = None

    def pre_clearing_activity(self, db_obj, flag_retrain_forecasts, clear_positions=False):
        if flag_retrain_forecasts:
            self.update_user_preferences(db_obj)
        if len(self._get_list_plants()) - len(self._get_list_plants(plant_type="hh")) == 0:
            self.controller_real_time(controller="hh")
        else:
            self.controller_real_time(controller="mpc")
        self.log_meter_readings(db_obj=db_obj)

        # set market type currently running:
        market_type = "ex_ante" if db_obj.lem_config["types_clearing_ex_ante"] else "ex_post"

        # get most recent market results, update price history
        self.get_market_results(market_type=market_type,
                                db_obj=db_obj)
        # if ex post market, update price history

        # then, retrain forecasts if necessary
        if flag_retrain_forecasts:
            self.retrain_forecasts()

        self.get_predictions()
        # then, execute model predictive control
        if len(self._get_list_plants()) - len(self._get_list_plants(plant_type="hh")) == 0:
            self.controller_model_predictive(controller="hh")
        else:
            self.controller_model_predictive(controller="mpc")

        # then, execute market agent if ex-ante market
        if db_obj.lem_config["types_clearing_ex_ante"]:
            self.market_agent(db_obj=db_obj, clear_positions=clear_positions)

    def post_clearing_activity(self, db_obj):
        market_type = "ex_ante" if db_obj.lem_config["types_clearing_ex_ante"] else "ex_post"
        self.get_market_results(market_type=market_type,
                                db_obj=db_obj)
        self.set_target_grid_power(market_type)

    # internal functions

    def controller_real_time(self, controller="mpc"):
        """Calculate the behaviour of the instance in the previous time step by applying a selected
        controller_real_time strategy to measurement data and plant specifications. Output is supplied in the form of a
        .json file saved to the user's folder, so that execution can be parallelized.

        :return: None
        """

        # default controller_real_time
        if controller == "mpc":
            # grid power is merely the sum of pv and the fixedgen consumers. The battery remains unused.
            df_target_grid_power = ft.read_dataframe(f"{self.path}/target_grid_power.ft")
            df_target_grid_power.set_index("timestamp", inplace=True)
            df_target_grid_power = df_target_grid_power.loc[self.ts_delivery_prev]
            # non-default controllers common model parameters initialized here.
            model = pyo.ConcreteModel()

            # Declare decision variables

            # deviation from setpoint (grid fee-in), absolute components
            model.deviation_gr_plus = pyo.Var(domain=pyo.NonNegativeReals)
            model.deviation_gr_minus = pyo.Var(domain=pyo.NonNegativeReals)

            # pv variables
            model.p_pv = pyo.Var(self._get_list_plants(plant_type="pv"),
                                 domain=pyo.NonNegativeReals)

            # fixedgen decision variables
            model.p_fixedgen = pyo.Var(self._get_list_plants(plant_type="fixedgen"),
                                       domain=pyo.NonNegativeReals)

            # pv maximum power constraint
            def pv_rule(_model, _plant):
                p_max = ft.read_dataframe(f"{self.path}/raw_data_{_plant}.ft")
                p_max.set_index("timestamp", inplace=True)
                p_max = float(p_max[p_max.index == self.ts_delivery_prev]["power"].values)
                p_max *= self.plant_dict[_plant]["power"]
                if self.plant_dict[_plant].get("controllable"):
                    return _model.p_pv[_plant] <= p_max
                return _model.p_pv[_plant] == p_max

            def fixedgen_rule(_model, _plant):
                p_max = ft.read_dataframe(f"{self.path}/raw_data_{_plant}.ft")
                p_max.set_index("timestamp", inplace=True)
                p_max = float(p_max[p_max.index == self.ts_delivery_prev]["power"].values)
                p_max *= self.plant_dict[_plant]["power"]
                if self.plant_dict[_plant].get("controllable"):
                    return _model.p_fixedgen[_plant] <= p_max
                return _model.p_fixedgen[_plant] == p_max

            if self._get_list_plants(plant_type="pv"):
                model.con_pv = pyo.Constraint(self._get_list_plants(plant_type="pv"),
                                              rule=pv_rule)

            if self._get_list_plants(plant_type="fixedgen"):
                model.con_fixedgen = pyo.Constraint(self._get_list_plants(plant_type="fixedgen"),
                                                    rule=fixedgen_rule)

            if self._get_list_plants(plant_type="bat"):
                # battery decision variables
                model.p_bat_in = pyo.Var(self._get_list_plants(plant_type="bat"), domain=pyo.NonNegativeReals)
                model.p_bat_out = pyo.Var(self._get_list_plants(plant_type="bat"), domain=pyo.NonNegativeReals)
                model.p_bat_milp = pyo.Var(self._get_list_plants(plant_type="bat"), domain=pyo.Binary)
                model.deviation_bat_plus = pyo.Var(self._get_list_plants(plant_type="bat"), domain=pyo.NonNegativeReals)
                model.deviation_bat_minus = pyo.Var(self._get_list_plants(plant_type="bat"), domain=pyo.NonNegativeReals)

                # else set battery power to zero
                dict_soc_old = {}
                model.n_bat = {}
                model.con_bat_dev = pyo.ConstraintList()

                for bat in self._get_list_plants(plant_type="bat"):
                    with open(f"{self.path}/soc_{bat}.json", "r") as read_file:
                        dict_soc_old[bat] = json.load(read_file)
                    model.n_bat[bat] = self.plant_dict[bat]["efficiency"]

                    model.p_bat_in[bat].setub(self.plant_dict[bat]["power"])
                    model.p_bat_out[bat].setub(self.plant_dict[bat]["power"])
                    model.con_bat_dev.add(expr=model.p_bat_out[bat] - model.p_bat_in[bat]
                                          == df_target_grid_power[f"power_{bat}"]
                                          - (model.deviation_bat_plus[bat] - model.deviation_bat_minus[bat]))

                def bat_soc_rule_1(_model, _bat):
                    return (dict_soc_old[_bat] - 0.25 * _model.p_bat_out[_bat] / _model.n_bat[_bat]
                            + 0.25 * _model.p_bat_in[_bat] * _model.n_bat[_bat] <= self.plant_dict[_bat].get("capacity"))

                def bat_soc_rule_2(_model, _bat):
                    return (dict_soc_old[_bat] - 0.25 * _model.p_bat_out[_bat] / _model.n_bat[_bat]
                            + 0.25 * _model.p_bat_in[_bat] * _model.n_bat[_bat] >= 0)

                def bat_bin_rule_minus(_model, _bat):
                    return _model.p_bat_in[_bat] <= 100000 * (1 - _model.p_bat_milp[_bat])

                def bat_bin_rule_plus(_model, _bat):
                    return _model.p_bat_out[_bat] <= 100000 * _model.p_bat_milp[_bat]

                model.bat_soc_1 = pyo.Constraint(self._get_list_plants(plant_type="bat"), rule=bat_soc_rule_1)
                model.bat_soc_2 = pyo.Constraint(self._get_list_plants(plant_type="bat"), rule=bat_soc_rule_2)
                model.bat_bin_minus = pyo.Constraint(self._get_list_plants(plant_type="bat"), rule=bat_bin_rule_minus)
                model.bat_bin_plus = pyo.Constraint(self._get_list_plants(plant_type="bat"), rule=bat_bin_rule_plus)

                # limit battery charging to pv generation
                model.con_batt_charge_grid = pyo.ConstraintList()
                make_const = 0
                expr_left = 0
                expr_right = 0
                for bat in self._get_list_plants(plant_type="bat"):
                    if not self.plant_dict[bat].get("charge_from_grid"):
                        expr_left += model.p_bat_in[bat]
                        make_const = 1
                for pv in self._get_list_plants(plant_type="pv"):
                    expr_right += model.p_pv[pv]
                if make_const:
                    model.con_batt_charge_grid.add(expr=(expr_left <= expr_right))

            # fixedgen load consumption, sum of household loads
            p_load = float(0)
            for hh in self._get_list_plants(plant_type="hh"):
                p_meas = ft.read_dataframe(f"{self.path}/raw_data_{hh}.ft")
                p_meas.set_index("timestamp", inplace=True)
                p_meas = float(p_meas[p_meas.index == self.ts_delivery_prev]["power"].values)
                p_load += float(p_meas)

            # ev decision variables
            if self._get_list_plants(plant_type="ev"):
                model.p_ev_in = pyo.Var(self._get_list_plants(plant_type="ev"), domain=pyo.NonNegativeReals)
                model.p_ev_out = pyo.Var(self._get_list_plants(plant_type="ev"), domain=pyo.NonNegativeReals)
                model.p_ev_milp = pyo.Var(self._get_list_plants(plant_type="ev"), domain=pyo.Binary)
                model.deviation_ev_plus = pyo.Var(self._get_list_plants(plant_type="ev"), domain=pyo.NonNegativeReals)
                model.deviation_ev_minus = pyo.Var(self._get_list_plants(plant_type="ev"), domain=pyo.NonNegativeReals)
                model.dev_ev_milp = pyo.Var(self._get_list_plants(plant_type="ev"), domain=pyo.Binary)

                model.con_ev_soc = pyo.ConstraintList()
                model.con_ev_milp = pyo.ConstraintList()
                model.con_ev_dev = pyo.ConstraintList()

                model.ev_soc_old = {}
                for ev in self._get_list_plants(plant_type="ev"):
                    model.p_ev_out[ev].setub(0)
                    model.p_ev_in[ev].setub(0)
                    raw_data_ev = ft.read_dataframe(f"{self.path}/raw_data_{ev}.ft")
                    raw_data_ev.set_index("timestamp", inplace=True)
                    raw_data_ev = raw_data_ev[raw_data_ev.index == self.ts_delivery_prev]
                    raw_data_ev = dict(raw_data_ev.loc[self.ts_delivery_prev])
                    if raw_data_ev["availability"] == 1:
                        model.p_ev_in[ev].setub(self.plant_dict[ev]["charging_power"])
                        if self.plant_dict[ev].get("v2g"):
                            model.p_ev_out[ev].setub(self.plant_dict[ev]["charging_power"])

                    with open(f"{self.path}/soc_{ev}.json", "r") as read_file:
                        model.ev_soc_old[ev] = max(0.05 * self.plant_dict[ev]["capacity"],
                                                   json.load(read_file) - raw_data_ev["distance_driven"]/100
                                                   * self.plant_dict[ev]["consumption"])

                    n_ev = self.plant_dict[ev]["efficiency"]

                    if raw_data_ev["availability"] == 1:
                        model.p_ev_in[ev].setub(self.plant_dict[ev]["charging_power"])
                        if self.plant_dict[ev].get("v2g"):
                            model.p_ev_out[ev].setub(self.plant_dict[ev]["charging_power"])

                        model.con_ev_soc.add(expr=model.ev_soc_old[ev]
                                             - 0.25 * model.p_ev_out[ev] / n_ev
                                             + 0.25 * model.p_ev_in[ev] * n_ev
                                             <= self.plant_dict[ev].get("capacity"))

                        model.con_ev_soc.add(expr=model.ev_soc_old[ev]
                                             - 0.25 * model.p_ev_out[ev] / n_ev
                                             + 0.25 * model.p_ev_in[ev] * n_ev
                                             >= df_target_grid_power[f"soc_min_{ev}"])

                        model.con_ev_milp.add(expr=model.p_ev_out[ev]
                                              <= 1000000 * model.p_ev_milp[ev])

                        model.con_ev_milp.add(expr=model.p_ev_in[ev]
                                              <= 1000000 * (1 - model.p_ev_milp[ev]))

                        model.con_ev_milp.add(expr=model.deviation_ev_plus[ev]
                                              <= 1000000 * model.dev_ev_milp[ev])

                        model.con_ev_milp.add(expr=model.deviation_ev_minus[ev]
                                              <= 1000000 * (1 - model.dev_ev_milp[ev]))

                        model.con_ev_dev.add(expr=model.p_ev_out[ev] - model.p_ev_in[ev]
                                             == df_target_grid_power[f"power_{ev}"]
                                             - (model.deviation_ev_plus[ev]
                                                - model.deviation_ev_minus[ev]))

            # declare balancing constraint, same for all controllers
            def balance_rule(_model):
                expression_left = 0
                if len(self._get_list_plants(plant_type="hh")):
                    self.meas_val[hh] = int(p_load)
                for _hh in self._get_list_plants(plant_type="hh"):
                    if self.plant_dict[_hh]["fcast"] != "aggregator":
                        expression_left += p_load
                for _fixedgen in self._get_list_plants(plant_type="fixedgen"):
                    expression_left += _model.p_fixedgen[_fixedgen]
                for _pv in self._get_list_plants(plant_type="pv"):
                    expression_left += model.p_pv[_pv]
                for _bat in self._get_list_plants(plant_type="bat"):
                    expression_left += _model.p_bat_out[_bat] - _model.p_bat_in[_bat]
                for _ev in self._get_list_plants(plant_type="ev"):
                    expression_left += model.p_ev_out[_ev] - model.p_ev_in[_ev]
                expression_right = float(df_target_grid_power[f"power_{self.config_dict['id_meter_grid']}"])
                expression_right -= _model.deviation_gr_plus - _model.deviation_gr_minus
                return expression_left == expression_right

            model.con_balance = pyo.Constraint(rule=balance_rule)

            # declare objective function, same for all controllers
            # _                             component 1: minimize deviation from target power (0 for self-consumption)
            # _                                          mutual exclusion of absolute components

            def obj_rule(_model):
                obj = 0.5*(model.deviation_gr_plus + model.deviation_gr_minus)
                for _bat in self._get_list_plants(plant_type="bat"):
                    obj += 0.1 * _model.deviation_bat_minus[_bat]
                for _ev in self._get_list_plants(plant_type="ev"):
                    obj += _model.deviation_ev_minus[_ev]
                return obj

            model.objective_fun = pyo.Objective(rule=obj_rule, sense=pyo.minimize)

            # solve model
            pyo.SolverFactory(self.config_dict["solver"]).solve(model)

            # assign results to instance variables for logging
            meas_grid = p_load
            for pv in self._get_list_plants(plant_type="pv"):
                self.meas_val[pv] = model.p_pv[pv].value
                meas_grid += model.p_pv[pv].value
            for fixedgen in self._get_list_plants(plant_type="fixedgen"):
                self.meas_val[fixedgen] = model.p_fixedgen[fixedgen].value
                meas_grid += model.p_fixedgen[fixedgen].value
            for bat in self._get_list_plants(plant_type="bat"):
                self.meas_val[bat] = model.p_bat_out[bat].value - model.p_bat_in[bat].value
                meas_grid += model.p_bat_out[bat].value - model.p_bat_in[bat].value
                bat_soc_new = dict_soc_old[bat] \
                    - 0.25 * model.p_bat_out[bat].value / self.plant_dict[bat]["efficiency"] \
                    + 0.25 * model.p_bat_in[bat].value * self.plant_dict[bat]["efficiency"]
                with open(f"{self.path}/soc_{bat}.json", "w") as write_file:
                    json.dump(bat_soc_new, write_file)
            for ev in self._get_list_plants(plant_type="ev"):
                self.meas_val[ev] = model.p_ev_out[ev].value - model.p_ev_in[ev].value
                meas_grid += model.p_ev_out[ev].value - model.p_ev_in[ev].value
                ev_soc_new = model.ev_soc_old[ev] \
                    - 0.25 * model.p_ev_out[ev].value / self.plant_dict[ev]["efficiency"] \
                    + 0.25 * model.p_ev_in[ev].value * self.plant_dict[ev]["efficiency"]
                with open(f"{self.path}/soc_{ev}.json", "w") as write_file:
                    json.dump(ev_soc_new, write_file)

            self.meas_val[self.config_dict['id_meter_grid']] = int(meas_grid)

        else:
            # fixedgen load consumption, sum of household loads
            p_load = float(0)
            for hh in self._get_list_plants(plant_type="hh"):
                p_meas = ft.read_dataframe(f"{self.path}/raw_data_{hh}.ft")
                p_meas.set_index("timestamp", inplace=True)
                p_meas = float(p_meas[p_meas.index == self.ts_delivery_prev]["power"].values)
                self.meas_val[hh] = p_meas
                p_load += p_meas

            self.meas_val[self.config_dict['id_meter_grid']] = int(p_load)

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

        for plant in self.plant_dict:
            log_ems.append(self.meas_val[plant] * factor_w_to_wh)
            dict_new_readings_local[plant] = [self._decomp_float(self.meas_val[plant] * factor_w_to_wh,
                                                                 return_val="neg"),
                                              self._decomp_float(self.meas_val[plant] * factor_w_to_wh,
                                                                 return_val="pos")]
        # log meter readings to local df_buffer
        self._set_new_meter_reading_local(dict_new_readings_local)

        # log measurement values to local df and overwrite local logging file
        df_meas_local.loc[self.ts_delivery_prev] = log_ems
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

    def get_predictions(self):
        """Return generation prediction for PV plants, consumption predictions for fixedgen loads, as well
        as well as market price predictions for the instance prediction horizon

        Current status: all forecasts are currently perfect predictions determined by looking at future values in the
                        input data

        :return: None
        """
        ts_pred = {f"power_{self.config_dict['id_meter_grid']}": [0]*self.config_dict["mpc_horizon"]}
        zero_vector = [0] * self.config_dict["mpc_horizon"]
        if self.config_dict["mpc_horizon"] > 0:
            for plant in self.plant_dict:
                if self.plant_dict[plant].get("type") in ["pv", "fixedgen"]:
                    temp = fcast.get_forecast(
                        fcast=self.plant_dict[plant].get("fcast"),
                        fcast_horizon=self.config_dict["mpc_horizon"],
                        fcast_param=self.plant_dict[plant].get("fcast_param"),
                        fcast_order=self.plant_dict[plant].get("fcast_order"),
                        ts_delivery_current=self.ts_delivery_current,
                        filepath=f"{self.path}/raw_data_{plant}.ft"
                        )
                    ts_pred[f"power_{plant}"] = [i * self.plant_dict[plant].get("power") for i in temp]

                elif self.plant_dict[plant].get("type") == "hh":
                    ts_pred[f"power_{plant}"] = fcast.get_forecast(
                        fcast=self.plant_dict[plant].get("fcast"),
                        fcast_horizon=self.config_dict["mpc_horizon"],
                        fcast_param=self.plant_dict[plant].get("fcast_param"),
                        fcast_order=self.plant_dict[plant].get("fcast_order"),
                        ts_delivery_current=self.ts_delivery_current,
                        filepath=f"{self.path}/raw_data_{plant}.ft"
                        )

                elif self.plant_dict[plant].get("type") == "bat":
                    ts_pred[f"power_{plant}"] = zero_vector
                    ts_pred[f"soc_{plant}"] = zero_vector
                elif self.plant_dict[plant].get("type") == "ev":
                    ts_pred[f"power_{plant}"] = zero_vector
                    ts_pred[f"soc_{plant}"] = zero_vector
                    ts_pred[f"soc_min_{plant}"] = zero_vector
                    ts_pred[f"availability_{plant}"] = fcast.get_forecast(
                        fcast=self.plant_dict[plant].get("fcast"),
                        fcast_horizon=self.config_dict["mpc_horizon"],
                        fcast_param=self.plant_dict[plant].get("fcast_param"),
                        fcast_order=self.plant_dict[plant].get("fcast_order"),
                        ts_delivery_current=self.ts_delivery_current,
                        filepath=f"{self.path}/raw_data_{plant}.ft"
                        )[0]
                    ts_pred[f"distance_driven_{plant}"] = fcast.get_forecast(
                        fcast=self.plant_dict[plant].get("fcast"),
                        fcast_horizon=self.config_dict["mpc_horizon"],
                        fcast_param=self.plant_dict[plant].get("fcast_param"),
                        fcast_order=self.plant_dict[plant].get("fcast_order"),
                        ts_delivery_current=self.ts_delivery_current,
                        filepath=f"{self.path}/raw_data_{plant}.ft"
                        )[1]

            if self.config_dict["mpc_price_fcast"] != "flat":
                # return all predicted values in one list
                ts_pred["price"] = fcast.get_forecast(
                            fcast="naive",
                            column="weighted_average_price",
                            fcast_horizon=self.config_dict["mpc_horizon"],
                            fcast_param=None,
                            fcast_order=None,
                            ts_delivery_current=self.ts_delivery_current,
                            filepath=f"{self.path}/price_history.ft"
                            )
            else:
                ts_pred["price"] = [(self.config_dict["max_bid"] - self.config_dict["min_offer"]) / 2]\
                                   * self.config_dict["mpc_horizon"]
            # predict settlement prices
            # TODO: only predict those settlement prices that have not yet been posted to the DB
            ts_pred["price_energy_levies_positive"] = fcast.get_forecast(
                        fcast="naive",
                        column="price_energy_levies_positive",
                        fcast_horizon=self.config_dict["mpc_horizon"],
                        fcast_param=None,
                        fcast_order=None,
                        ts_delivery_current=self.ts_delivery_current,
                        filepath=f"{self.path}/price_history.ft"
                        )
            ts_pred["price_energy_levies_negative"] = fcast.get_forecast(
                        fcast="naive",
                        column="price_energy_levies_negative",
                        fcast_horizon=self.config_dict["mpc_horizon"],
                        fcast_param=None,
                        fcast_order=None,
                        ts_delivery_current=self.ts_delivery_current,
                        filepath=f"{self.path}/price_history.ft"
                        )
            # save list to mpc dataframe for optimization using the predictive controller_real_time method
            ts_pred["timestamp"] = range(self.ts_delivery_current,
                                         self.ts_delivery_current + self.config_dict["mpc_horizon"] * 900,
                                         900)
            self.mpc_table = pd.DataFrame(ts_pred)
            self.mpc_table.set_index("timestamp", inplace=True)

    def controller_model_predictive(self, controller="mpc"):
        """Execute the model predictive controller_real_time for the market participant given the predicted
        generation, consumption, and market prices for a configurable time horizon.

        The controller_real_time will always attempt to maximize its earnings. If no market price optimization is
        desired, a flat market price forecasting should be input.

        :return: None
        """
        if controller == "mpc":
            # Declare the pyomo model
            model = pyo.ConcreteModel()
            # declare decision variables (vectors of same length as MPC horizon)
            # pv power variable

            if self._get_list_plants(plant_type="pv"):
                model.p_pv = pyo.Var(self._get_list_plants(plant_type="pv"),
                                     range(0, self.config_dict["mpc_horizon"]),
                                     domain=pyo.NonNegativeReals)

            # fixedgen power variable
            if self._get_list_plants(plant_type="fixedgen"):
                model.p_fixedgen = pyo.Var(self._get_list_plants(plant_type="fixedgen"),
                                           range(0, self.config_dict["mpc_horizon"]),
                                           domain=pyo.NonNegativeReals)

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
                model.ev_slack = pyo.Var(self._get_list_plants(plant_type="ev"),
                                         range(self.config_dict["mpc_horizon"]),
                                         domain=pyo.NonNegativeReals)
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
                       or (i == self.config_dict["mpc_horizon"] - 1 and self.mpc_table[f"availability_{ev}"].iloc[i] == 1):
                        model.con_soc_ev.add(expr=model.soc_ev[ev, i] >= soc_potential - model.ev_slack[ev, i])
                        dict_soc_ev_min[ev][i] = soc_potential

                    # Set availability and powers depending on if EV is available or not
                    if self.mpc_table[f"availability_{ev}"].iloc[i] == 1:
                        # Set if EV can charge or discharge
                        model.con_p_ev_minus.add(expr=model.p_ev_in[ev, i] <= 1000000 * (1 - model.p_ev_milp[ev, i]))
                        model.con_p_ev_plus.add(expr=model.p_ev_out[ev, i] <= 1000000 * model.p_ev_milp[ev, i])

                        # Set maximum charging and discharging power (upper boundaries)
                        model.p_ev_in[ev, i].setub(self.plant_dict[ev].get("charging_power"))
                        if self.plant_dict[ev].get("v2g"):
                            model.p_ev_out[ev, i].setub(self.plant_dict[ev].get("charging_power"))
                        else:
                            model.p_ev_out[ev, i].setub(0)
                    else:
                        model.p_ev_in[ev, i] = float(0)
                        model.p_ev_out[ev, i] = float(0)

                # Loop from end to beginning and increase the min SoC for the previous timestep to ensure that the EV starts
                #   charging soon enough to reach the final min SoC before the car leaves
                for i in range(self.config_dict["mpc_horizon"]-1, 0, -1):
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

            # fixedgen power, sum of household loads
            p_load = [0] * self.config_dict["mpc_horizon"]
            for hh in self._get_list_plants(plant_type="hh"):
                for i, ts_d in enumerate(range(self.ts_delivery_current,
                                               self.ts_delivery_current + self.config_dict["mpc_horizon"]*900, 900)):
                    p_load[i] += float(self.mpc_table.loc[ts_d, f"power_{hh}"])

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
            for pv in self._get_list_plants(plant_type="pv"):
                for t, t_d in enumerate(range(self.ts_delivery_current,
                                              self.ts_delivery_current + 900*self.config_dict["mpc_horizon"], 900)):
                    model.sum_pv[t] += self.mpc_table.loc[t_d, f"power_{pv}"]
                    if self.plant_dict[pv].get("controllable"):
                        model.con_p_pv.add(expr=model.p_pv[pv, t] <= round(self.mpc_table.loc[t_d, f"power_{pv}"], 1))
                    else:
                        model.con_p_pv.add(expr=model.p_pv[pv, t] == round(self.mpc_table.loc[t_d, f"power_{pv}"], 1))

            # define fixedgen power upper bound from input file
            model.con_p_fixedgen = pyo.ConstraintList()
            model.sum_fixedgen = [0] * self.config_dict["mpc_horizon"]
            for fixedgen in self._get_list_plants(plant_type="fixedgen"):
                for t, t_d in enumerate(range(self.ts_delivery_current,
                                              self.ts_delivery_current + 900*self.config_dict["mpc_horizon"], 900)):
                    model.sum_fixedgen[t] += self.mpc_table.loc[t_d, f"power_{fixedgen}"]
                    if self.plant_dict[fixedgen].get("controllable"):
                        model.con_p_fixedgen.add(expr=model.p_fixedgen[fixedgen, t]
                                                 <= self.mpc_table.loc[t_d, f"power_{fixedgen}"])
                    else:
                        model.con_p_fixedgen.add(expr=model.p_fixedgen[fixedgen, t]
                                                 == self.mpc_table.loc[t_d, f"power_{fixedgen}"])

            # limit battery charging to pv generation
            model.con_batt_charge_grid = pyo.ConstraintList()
            for t in range(0, self.config_dict["mpc_horizon"]):
                expr_left = 0
                expr_right = model.sum_pv[t]
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

            model.con_balance = pyo.ConstraintList()
            for _t in range(self.config_dict["mpc_horizon"]):
                expression_left = p_load[_t]
                for _pv in self._get_list_plants(plant_type="pv"):
                    expression_left += model.p_pv[_pv, _t]
                for _bat in self._get_list_plants(plant_type="bat"):
                    expression_left += model.p_bat_out[_bat, _t] - model.p_bat_in[_bat, _t]
                for _ev in self._get_list_plants(plant_type="ev"):
                    expression_left += model.p_ev_out[_ev, _t] - model.p_ev_in[_ev, _t]
                for _fixedgen in self._get_list_plants(plant_type="fixedgen"):
                    expression_left += model.p_fixedgen[_fixedgen, _t]
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
                        step_obj += 0.0005 * _model.p_grid_out[j] * _model.p_grid_out[j] / 1000 / 1000
                        step_obj += 0.0005 * _model.p_grid_in[j] * _model.p_grid_in[j] / 1000 / 1000
                    # legacy check for electric vehicle constraint violation
                    for item in self._get_list_plants(plant_type="ev"):
                        step_obj += _model.ev_slack[item, j] * 100000000
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

                # Battery
                for bat in self._get_list_plants(plant_type="bat"):
                    dict_mpc_table[f"power_{bat}"][t_d] = model.p_bat_out[bat, i]() - model.p_bat_in[bat, i]()
                    dict_mpc_table[f"soc_{bat}"][t_d] = model.soc_bat[bat, i]()

                # EV
                for ev in self._get_list_plants(plant_type="ev"):
                    dict_mpc_table[f"power_{ev}"][t_d] = model.p_ev_out[ev, i]() - model.p_ev_in[ev, i]()
                    dict_mpc_table[f"soc_{ev}"][t_d] = model.soc_ev[ev, i]()
                    dict_mpc_table[f"soc_min_{ev}"][t_d] = min(dict_soc_ev_min[ev][i], self.plant_dict[ev].get("capacity"))
                    # Temporary check for errors in the electric vehicle charging routine

                    if model.ev_slack[ev, i]() >= 10**-6:
                        # for object name file1.
                        logfile = open(f"{'/'.join(self.path.split('/')[:-2])}/log.txt", "a")
                        logfile.write(f"Warning: User {self.config_dict['id_user']}'s EV #{ev} violated its charging "
                                      f"constraint at {self.t_now} resulting in a slack value of {model.ev_slack[ev, i]()} "
                                      f"in MPC step {i}")
                        logfile.close()
                # Fixed generation
                for fixedgen in self._get_list_plants(plant_type="fixedgen"):
                    dict_mpc_table[f"power_{fixedgen}"][t_d] = model.p_fixedgen[fixedgen, i]()

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
                                               self.ts_delivery_current + self.config_dict["mpc_horizon"]*900, 900)):
                    p_load[i] += float(self.mpc_table.loc[ts_d, f"power_{hh}"])
            dict_mpc_table = self.mpc_table.to_dict()
            dict_mpc_table = self.mpc_table.to_dict()
            for i, t_d in enumerate(range(self.ts_delivery_current,
                                          self.ts_delivery_current + 900 * self.config_dict["mpc_horizon"], 900)):
                # Grid power
                dict_mpc_table[f"power_{self.config_dict['id_meter_grid']}"][t_d] = p_load[i]
            # Save results to file, which will be used as basis for controller_real_time set points and market trading
            self.mpc_table = pd.DataFrame.from_dict(dict_mpc_table)

        ft.write_dataframe(self.mpc_table.reset_index().rename(columns={"index": "timestamp"}),
                           f"{self.path}/controller_mpc.ft")

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
                total_paid = (market_results[db_obj.db_param.QTY_ENERGY_TRADED] * market_results[db_obj.db_param.PRICE_ENERGY_MARKET_
                              + db_obj.lem_config["types_pricing_ex_ante"][0]]).sum()
                if total_energy != 0:
                    dict_price_history.update({"weighted_average_price": total_paid/total_energy/euro_kwh_to_sigma_wh,
                                               "total_energy_traded": total_energy})
                else:
                    dict_price_history.update({"weighted_average_price": self.config_dict["max_bid"],
                                               "total_energy_traded": 0})
            df_price_history.loc[self.ts_delivery_prev] = dict_price_history
        else:
            market_results = db_obj.get_results_market_ex_post(
                ts_delivery_first=self.ts_delivery_prev - 24*3600,
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
                ts_delivery_last=self.ts_delivery_current + self.config_dict["ma_horizon"]*900
                )
            self.update_price_history(db_obj, market_type="ex_ante")
        else:
            self.matched_bids, self.matched_bids_by_timestep = None, None
            self.update_price_history(db_obj,
                                      market_type="ex_post")

    def update_user_preferences(self, db_obj):
        user_info = db_obj.get_info_user(self.config_dict["id_user"])

        self.config_dict["max_bid"] = \
            float(user_info.loc[0, db_obj.db_param.PRICE_ENERGY_BID_MAX])\
            / db_obj.db_param.EURO_TO_SIGMA * 1000
        self.config_dict["min_offer"] = \
            float(user_info.loc[0, db_obj.db_param.PRICE_ENERGY_OFFER_MIN])\
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
            df_target_grid_power[f"power_{self.config_dict['id_meter_grid']}"] =\
                self.matched_bids_by_timestep["net_bids"] * 4

        ft.write_dataframe(df_target_grid_power.reset_index(),
                           f"{self.path}/target_grid_power.ft")

    def market_agent(self, db_obj, clear_positions=False):
        """Calculate and post/update market positions to the double sided market.

        :param db_obj: Database instance, pass the database connection instance to this method
        :param clear_positions: boolean, clear own positions in database if true

        :return:
        """
        # generate list of potential bids from MPC results
        # all grid flows are potential bids

        df_potential_bids = pd.DataFrame(
            self.mpc_table[(self.ts_delivery_current <= self.mpc_table.index)
                           & (self.mpc_table.index <= self.ts_delivery_current
                              + 15*60*self.config_dict["ma_horizon"])]
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

    def retrain_forecasts(self):
        for plant in self.plant_dict:
            if self.plant_dict[plant].get("fcast") == "sarma":
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    param = fcast.train_sarma(filepath=f"{self.path}/raw_data_{plant}.ft",
                                              ts_delivery_prev=self.ts_delivery_prev,
                                              fcast_param_init=self.plant_dict[plant]["fcast_param"],
                                              fcast_order=self.plant_dict[plant]["fcast_order"]
                                              )
                self.plant_dict[plant]["fcast_param"] = param
                with open(f"{self.path}/config_plants.json", "w") as write_file:
                    json.dump(self.plant_dict, write_file)
            else:
                pass

    def _get_list_plants(self, plant_type=None):
        list_plants = []
        for plant in self.config_dict["list_plants"]:
            if self.plant_dict[plant].get("type") == plant_type:
                list_plants.append(plant)
        if plant_type is None:
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
                    time_late = abs(np.random.normal(0, self.config_dict["meter_prob_late_95"]/2, 1)[0])
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
