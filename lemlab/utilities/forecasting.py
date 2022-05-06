__author__ = "sdlumpp"
__credits__ = []
__license__ = ""
__maintainer__ = "sdlumpp"
__email__ = "sebastian.lumpp@tum.de"

import feather as ft
import random
import numpy as np
from scipy.optimize import minimize as sp_minimize
import pandas as pd
import json
import warnings

"""
ForecastManager provides functions for the forecasting of household electric load and production for application
in the local energy market.

Forecasting is divided into two steps. Training models and making the forecast. Training models must be done once at
the beginning of a simulation and may be repeated as often or as rarely as desired.

Applying the model to calculate a forecast must be done at least as often as the MPC is executed.

"""


class ForecastManager:

    def __init__(self, path_prosumer, config_dict, plant_dict, t_override=None, df_weather_fcast=None,
                 df_weather_history=None):
        self.path_prosumer = path_prosumer
        self.config_dict = config_dict
        self.plant_dict = plant_dict
        self.path_prosumer = path_prosumer

        # set current timestamp from system clock or keyword arg
        self.t_now = t_override if t_override else pd.Timestamp.now().timestamp()
        # derive previous and next timestamps
        self.ts_delivery_prev = round(pd.Timestamp(self.t_now, unit="s").floor("15min").timestamp() - 15*60)
        self.ts_delivery_current = self.ts_delivery_prev + 15*60

    def update_forecasts(self):

        self._retrain_forecasts()
        """Return generation prediction for PV plants, consumption predictions for fixedgen loads, as well
              as well as market price predictions for the instance prediction horizon

              Current status: all forecasts are currently perfect predictions determined by looking at future values in the
                              input data

              :return: None
              """
        try:
            self.fcast_table = ft.read_dataframe("fcasts_current.ft").set_index("timestamp")
        except FileNotFoundError:
            ts_init = [[ts, 0] for ts in range(self.ts_delivery_current,
                                               self.ts_delivery_current + self.config_dict["mpc_horizon"] * 900,
                                               900)]
            self.fcast_table = pd.DataFrame(ts_init,
                                            columns=["timestamp", f"power_{self.config_dict['id_meter_grid']}"]
                                            ).set_index("timestamp")

        if self.config_dict["mpc_horizon"] > 0:
            for plant in self.plant_dict:
                last_update = self.plant_dict[plant].get("fcast_last_update")
                period_update = self.plant_dict[plant].get("fcast_period_update")
                if self.ts_delivery_current - last_update >= period_update:

                    if self.plant_dict[plant].get("type") in ["pv", "fixedgen"]:
                        df_temp = self._update_forecast(
                            fcast=self.plant_dict[plant].get("fcast"),
                            fcast_horizon=self.config_dict["mpc_horizon"] + period_update//900,
                            fcast_param=self.plant_dict[plant].get("fcast_param"),
                            fcast_order=self.plant_dict[plant].get("fcast_order"),
                            filepath=f"{self.path_prosumer}/raw_data_{plant}.ft"
                        )
                        self.fcast_table = pd.DataFrame.join(self.fcast_table, df_temp, on="timestamp",
                                                             how="outer", lsuffix=f"duplicate")

                        self.fcast_table.rename(columns={'power': f'power_{plant}'}, inplace=True)

                    elif self.plant_dict[plant].get("type") in ["wind"]:
                        df_temp = self._update_forecast(
                            fcast=self.plant_dict[plant].get("fcast"),
                            fcast_horizon=self.config_dict["mpc_horizon"] + period_update//900,
                            fcast_param=self.plant_dict[plant].get("fcast_param"),
                            fcast_order=self.plant_dict[plant].get("fcast_order"),
                            filepath=f"{self.path_prosumer}".split("prosumer/")[0],
                            column="wind_speed"
                        )
                        self.fcast_table = self.fcast_table.join(df_temp, on="timestamp", how="outer", lsuffix="duplicate")
                        self.fcast_table.rename(columns={'wind_speed': f'wind_speed_{plant}'}, inplace=True)
                        self.fcast_table[f'power_{plant}'] = 0

                    elif self.plant_dict[plant].get("type") == "hh":
                        df_temp = self._update_forecast(
                            fcast=self.plant_dict[plant].get("fcast"),
                            fcast_horizon=self.config_dict["mpc_horizon"] + period_update//900,
                            fcast_param=self.plant_dict[plant].get("fcast_param"),
                            fcast_order=self.plant_dict[plant].get("fcast_order"),
                            filepath=f"{self.path_prosumer}/raw_data_{plant}.ft"
                        )
                        self.fcast_table = self.fcast_table.join(df_temp, on="timestamp",
                                                                 how="outer", lsuffix=f"duplicate")
                        self.fcast_table.rename(columns={'power': f'power_{plant}'}, inplace=True)
                        self.fcast_table.set_index("timestamp", drop=True, inplace=True)

                    elif self.plant_dict[plant].get("type") == "bat":
                        self.fcast_table[f'power_{plant}'] = 0
                        self.fcast_table[f'soc_{plant}'] = 0

                    elif self.plant_dict[plant].get("type") == "ev":
                        self.fcast_table[f'power_{plant}'] = 0
                        self.fcast_table[f'soc_{plant}'] = 0
                        self.fcast_table[f'soc_min_{plant}'] = 0

                        df_temp = self._update_forecast(
                            fcast=self.plant_dict[plant].get("fcast"),
                            fcast_horizon=self.config_dict["mpc_horizon"] + period_update//900,
                            fcast_param=self.plant_dict[plant].get("fcast_param"),
                            fcast_order=self.plant_dict[plant].get("fcast_order"),
                            filepath=f"{self.path_prosumer}/raw_data_{plant}.ft",
                            column="availability"
                            )
                        self.fcast_table = self.fcast_table.join(df_temp, on="timestamp",
                                                                 how="outer", lsuffix=f"duplicate")
                        self.fcast_table.rename(columns={'availability': f'availability_{plant}'}, inplace=True)

                        df_temp = self._update_forecast(
                            fcast=self.plant_dict[plant].get("fcast"),
                            fcast_horizon=self.config_dict["mpc_horizon"] + period_update//900,
                            fcast_param=self.plant_dict[plant].get("fcast_param"),
                            fcast_order=self.plant_dict[plant].get("fcast_order"),
                            filepath=f"{self.path_prosumer}/raw_data_{plant}.ft",
                            column="distance_driven"
                            )
                        self.fcast_table = self.fcast_table.join(df_temp, on="timestamp",
                                                                 how="outer", lsuffix=f"_{plant}")
                        self.fcast_table.rename(columns={'distance_driven': f'distance_driven_{plant}'}, inplace=True)

            if self.config_dict["mpc_price_fcast"] != "flat":
                last_update = self.config_dict["mpc_price_fcast_last_update"]
                period_update = self.config_dict["mpc_price_fcast_period_update"]
                if self.ts_delivery_current - last_update >= period_update:
                    # return all predicted values in one list
                    df_temp = self._update_forecast(
                        fcast=self.config_dict["mpc_price_fcast"],
                        fcast_horizon=self.config_dict["mpc_horizon"] + period_update//900,
                        fcast_param=None,
                        fcast_order=None,
                        filepath=f"{self.path_prosumer}/price_history.ft",
                        column="weighted_average_price",
                    )
                    self.fcast_table = self.fcast_table.join(df_temp, on="timestamp",
                                                             how="outer", lsuffix=f"duplicate")
                    self.fcast_table.rename(columns={'weighted_average_price': f'price'}, inplace=True)
            else:
                self.fcast_table[f'price'] = \
                    (self.config_dict["max_bid"] - self.config_dict["min_offer"]) / 2 * self.config_dict["mpc_horizon"]

            # predict settlement prices
            # TODO: only predict those settlement prices that have not yet been posted to the DB

            df_temp = self._update_forecast(
                        fcast="naive",
                        column="price_energy_levies_positive",
                        fcast_horizon=self.config_dict["mpc_horizon"],
                        fcast_param=None,
                        fcast_order=None,
                        filepath=f"{self.path_prosumer}/price_history.ft"
                        )

            self.fcast_table = self.fcast_table.join(df_temp, on="timestamp", how="outer", lsuffix="duplicate")
            df_temp = self._update_forecast(
                        fcast="naive",
                        column="price_energy_levies_negative",
                        fcast_horizon=self.config_dict["mpc_horizon"],
                        fcast_param=None,
                        fcast_order=None,
                        filepath=f"{self.path_prosumer}/price_history.ft"
                        )
            self.fcast_table = self.fcast_table.join(df_temp, on="timestamp", how="outer", lsuffix="duplicate")

        self.fcast_table.index.name = "timestamp"
        ft.write_dataframe(self.fcast_table.reset_index(), f"{self.path_prosumer}/fcasts_current.ft")

    def _retrain_forecasts(self):
        list_trainable_models = ["sarma", "nn"]

        for plant in self.plant_dict:

            last_retrain = self.plant_dict[plant].get("fcast_last_retrain")
            period_retrain = self.plant_dict[plant].get("fcast_period_retrain")

            if self.ts_delivery_current - last_retrain >= period_retrain:
                if self.plant_dict[plant].get("fcast") == "sarma":
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        param = self._train_sarma(filepath=f"{self.path_prosumer}/raw_data_{plant}.ft",
                                                  fcast_param_init=self.plant_dict[plant]["fcast_param"],
                                                  fcast_order=self.plant_dict[plant]["fcast_order"]
                                                  )
                    self.plant_dict[plant]["fcast_param"] = param
                    with open(f"{self.path_prosumer}/config_plants.json", "w") as write_file:
                        json.dump(self.plant_dict, write_file)
                elif self.plant_dict[plant].get("fcast") == "nn":
                    self._train_neural_net()
                self.plant_dict[plant]["fcast_last_retrain"] = self.ts_delivery_current
            else:
                pass

        with open(f"{self.path_prosumer}/config_plants.json", "w") as write_file:
            json.dump(self.plant_dict, write_file)


    def _train_sarma(self, filepath, fcast_order, fcast_param_init=None):
        print("hey")

    def _train_sarma(self, filepath, fcast_order, fcast_param_init=None):

        """
        Trains a SARMA model of the given order and the set initial parameters and returns the optimized model parameters.

        :param filepath: string, path to the raw data
        :param fcast_order: list, order of SARMA model, see config.YAML for a description
        :param fcast_param_init: list, initial values of the SARMA model parameters

        :return optimal_param: list, new SARMA model parameters after training
        """

        # read historical values and create time series to be utilities from
        df_in = ft.read_dataframe(filepath)
        df_in.set_index("timestamp", inplace=True)
        y = list(df_in[(df_in.index < self.ts_delivery_prev)]["power"]/df_in[(df_in.index < self.ts_delivery_prev)]["power"].max()*2)

        # import model hyper parameters
        order = fcast_order

        # season lengths
        s1 = order[6]
        s2 = order[10]

        # resize input dataset to match training time
        training_time = s2 * order[7] + 3000
        y = y[-training_time:]

        # generate random starting point for optimization if no initial parameters are supplied
        init = fcast_param_init
        if fcast_param_init is None:
            target = 3
            while target > 1 or target < 0.9:
                init = []
                for i in range(sum(order) - s1 - s2):
                    init.append(random.random() * 0.5 - 0.1)
                target = sum(init)

        # execute a gradient search on the model parameters to minimize the RMSE
        result = sp_minimize(self._sarma_objective,
                             x0=init,
                             method="SLSQP",
                             args=(y, order),
                             tol=1e-4)

        # extract optimal fcast parameters
        optimal_param = []
        for val in result.x:
            optimal_param.append(val)
        optimal_param = [round(num, 3) for num in optimal_param]

        return optimal_param

    def _train_neural_net(self):
        print("lol@u")

    @staticmethod
    def _sarma_objective(par, training_data, order=None):
        """
        Calculates the RMSE of the forecast model "par" of order "order" on the training data. Used for training of SARMA
        models.

        :param par: list, SARMA model parameters to be evaluated
        :param training_data: list, training data the model should be evaluated on
        :param order: list, order of the SARMA model, see config.YAML for further explanation

        :return obj: float, RMSE of the SARMA model
        """
        if order is None:
            order = [3, 0, 3, 3, 0, 3, 96, 2, 0, 2, 96 * 7]
        # par contains parameters in order
        # y contains training data for model to be build on
        # order -> [ar, i, ma, s1_ar, s1_i, s1_ma, s1_len, s2_ar, s2_i, s2_ma, s2_len]

        # season lengths
        s1 = order[6]
        s2 = order[10]
        # time series to be fitted
        y = training_data
        # training steps to start and end on
        step_start = s2 * 2
        step_end = len(y)

        # array of residuals as estimated by model
        res_est = [0] * (len(y) + 1)

        # set mean value of data to zero
        y_mean = np.mean(y)
        y = np.array(y) - y_mean

        # squared error, objective
        err = [0] * (len(y) + 1)

        # calculate model residuals for training data
        for t in range(step_start, step_end):
            par_pointer = 0  # pointer used for establishing dynamic order SARIMA equations
            res_est[t] = y[t]  # first term of SARMA -> residual[t] = y[t] - (ar1*y[t-1] + ma1*residual[t-1] ... )
            # AR terms of equation
            for lag in range(1, order[0] + 1):
                res_est[t] -= par[par_pointer] * y[t - lag]
                par_pointer += 1
            # MA terms of equation
            for lag in range(1, order[2] + 1):
                res_est[t] -= par[par_pointer] * res_est[t - lag]
                par_pointer += 1
            # season 1 AR terms of equation
            for lag in range(1, order[3] + 1):
                res_est[t] -= par[par_pointer] * (y[t - lag * s1])
                par_pointer += 1
            # season 2 AR terms of equation
            for lag in range(1, order[5] + 1):  # SAR 2
                res_est[t] -= par[par_pointer] * (y[t - lag * s2])
                par_pointer += 1
            # season 1 MA terms of equation
            for lag in range(1, order[7] + 1):  # SMA 1
                res_est[t] -= par[par_pointer] * (res_est[t - lag * s1])
                par_pointer += 1
            # season 2 MA terms of equation
            for lag in range(1, order[9] + 1):  # SMA 2
                res_est[t] -= par[par_pointer] * (res_est[t - lag * s2])
                par_pointer += 1
            # calculate mean squared error
            err[t] = res_est[t]
            # calculate APE
        # return RMSE as objective value
        return np.sqrt(np.mean(np.square(err)))

    def _update_forecast(self, fcast, fcast_horizon, fcast_order, fcast_param, filepath, column="power"):
        """
        Takes a forecast model fcast and applies it to the data in "column" of "filepath" and returns a forecast starting at
        ts_delivery_current for "fcast_horizon" steps.

        :param fcast: string, type of fcast model to be used e.g. "sarma" or "perfect"
        :param fcast_horizon: int, how many timesteps should the forecast contain?
        :param fcast_order: list, order of the SARMA model, see config.YAML for further explanation
        :param fcast_param: list, fcast model parameters, e.g. SARMA parameters
        :param filepath: string, path to the data
        :param column: string, name of the data column to be forecast

        :return obj: float, RMSE of the SARMA model

        """
        if fcast == "sarma":
            # return sarma forecast on data
            df_y_hat = 0
            # read historical values and create time series to be utilities from
            df_in = ft.read_dataframe(filepath)
            df_in.set_index("timestamp", inplace=True)
            y = list(df_in[(df_in.index <= self.ts_delivery_current - 900)][column]
                     / df_in[(df_in.index <= self.ts_delivery_current - 900)][column].max()*2)

            # import model hyperparameters
            par = fcast_param
            order = fcast_order
            # season lengths
            s1 = order[6]
            s2 = order[10]
            # set mean value of data to zero
            mean_data = np.mean(y)
            for step in range(len(y)):
                y[step] = y[step] - mean_data

            y_hat = y
            y_hat.extend([0] * fcast_horizon)
            y_res_est = [0] * (len(y_hat))
            # calculate model residuals

            for t in range(len(y) - 2 * s2, len(y)):
                y_res_est[t] = y[t]  # first term of SARMA -> residual[t] = y[t] - (ar1*y[t-1] + ma1*residual[t-1] ... )
                par_pointer = 0  # pointer used for establishing dynamic order SARIMA equations
                for lag in range(1, order[0] + 1):  # AR
                    y_res_est[t] -= par[par_pointer] * y[t - lag]
                    par_pointer += 1
                for lag in range(1, order[2] + 1):  # MA
                    y_res_est[t] -= par[par_pointer] * y_res_est[t - lag]
                    par_pointer += 1
                for lag in range(1, order[3] + 1):  # SAR 1
                    y_res_est[t] -= par[par_pointer] * (y[t - lag * s1])
                    par_pointer += 1
                for lag in range(1, order[5] + 1):  # SAR 2
                    y_res_est[t] -= par[par_pointer] * (y[t - lag * s2])
                    par_pointer += 1
                for lag in range(1, order[7] + 1):  # SMA 1
                    y_res_est[t] -= par[par_pointer] * (y_res_est[t - lag * s1])
                    par_pointer += 1
                for lag in range(1, order[9] + 1):  # SMA 2
                    y_res_est[t] -= par[par_pointer] * (y_res_est[t - lag * s2])
                    par_pointer += 1

            for t in range(len(y) - fcast_horizon, len(y)):
                y_hat[t] = y[t]  # first term of SARMA -> y[t] = ar1*y[t-1] + ma1*residual[t-1] ...
                par_pointer = 0  # pointer used for establishing dynamic order SARIMA equations
                for lag in range(1, order[0] + 1):  # AR
                    y_hat[t] += par[par_pointer] * y_hat[t - lag]
                    par_pointer += 1
                for lag in range(1, order[2] + 1):  # MA
                    y_hat[t] += par[par_pointer] * y_res_est[t - lag]
                    par_pointer += 1
                for lag in range(1, order[3] + 1):  # SAR 1
                    y_hat[t] += par[par_pointer] * (y_hat[t - lag * s1])
                    par_pointer += 1
                for lag in range(1, order[5] + 1):  # SAR 2
                    y_hat[t] += par[par_pointer] * (y_hat[t - lag * s2])
                    par_pointer += 1
                for lag in range(1, order[7] + 1):  # SMA 1
                    y_hat[t] += par[par_pointer] * (y_res_est[t - lag * s1])
                    par_pointer += 1
                for lag in range(1, order[9] + 1):  # SMA 2
                    y_hat[t] += par[par_pointer] * (y_res_est[t - lag * s2])
                    par_pointer += 1
            fcast = y_hat[-fcast_horizon:]
            # set mean value of data back to actual mean
            for step in range(len(fcast)):
                fcast[step] += mean_data
                fcast[step] = [self.ts_delivery_current + step*900,
                               fcast[step] * df_in[(df_in.index <= self.ts_delivery_current - 900)][column].max()/2]
            df_fcast = pd.DataFrame(fcast, columns=("timestamp", column)).set_index("timestamp")
            return df_fcast

        elif fcast == "perfect":
            # perfect knowledge of the future
            df_in = ft.read_dataframe(filepath)
            df_in.set_index("timestamp", inplace=True)
            df_y_hat = df_in[(self.ts_delivery_current <= df_in.index)
                            & (df_in.index <= self.ts_delivery_current + 900 * fcast_horizon - 1)][column]
            return df_y_hat

        elif fcast == "naive":
            # naive forecast, today will be the average of the previous 1 days
            # read historical values and create time series to be utilities from
            df_in = ft.read_dataframe(filepath)
            df_in.set_index("timestamp", inplace=True)
            y = list(df_in[(df_in.index <= self.ts_delivery_current - 900)][column])

            y_hat = []
            ts_pt = self.ts_delivery_current
            for step in range(-fcast_horizon, 0, 1):
                val = sum([y[step - i * 96] for i in range(1)])
                y_hat.append([ts_pt, val / 1])
                ts_pt += 900

            df_y_hat = pd.DataFrame(y_hat, columns=("timestamp", column)).set_index("timestamp")
            return df_y_hat

        elif fcast == "naive_average":
            # naive forecast, today will be the average of the previous 7 days
            # read historical values and create time series to be utilities from
            df_in = ft.read_dataframe(filepath)
            df_in.set_index("timestamp", inplace=True)
            y = list(df_in[(df_in.index <= self.ts_delivery_current - 900)][column])
            y_hat = []
            ts_pt = self.ts_delivery_current
            for step in range(-fcast_horizon, 0, 1):
                val = sum([y[step - i * 96] for i in range(7)])
                y_hat.append([ts_pt, val / 7])
                ts_pt += 900

            df_y_hat = pd.DataFrame(y_hat, columns=("timestamp", column)).set_index("timestamp")

            return df_y_hat

        elif fcast == "aggregator":
            # return a zero forecast if plant is aggregated
            df_in = ft.read_dataframe(filepath)
            df_in.set_index("timestamp", inplace=True)
            df_y_hat = df_in[(self.ts_delivery_current <= df_in.index)
                             & (df_in.index <= self.ts_delivery_current + 900 * fcast_horizon - 1)][column]
            df_y_hat[column] = 0
            return df_y_hat

        elif fcast == "smoothed":
            # moving average "perfect filter" forecast
            df_y_hat = 0
            df_in = ft.read_dataframe(filepath)
            df_in.set_index("timestamp", inplace=True)
            raw_pred_temp = list(df_in[(self.ts_delivery_current - 900 * fcast_param <= df_in.index)
                                 & (df_in.index <= self.ts_delivery_current + 900 * fcast_horizon - 1 + 900 * fcast_param)]
                                 [column])
            y_hat = []
            for step in range(0, fcast_horizon):
                temp_val = raw_pred_temp[step + fcast_param]
                for i in range(1, fcast_param + 1):
                    temp_val += raw_pred_temp[step + fcast_param + i]
                    temp_val += raw_pred_temp[step + fcast_param - i]
                temp_val /= 2 * fcast_param + 1
                y_hat.append(temp_val)

            df_y_hat = df_in[(self.ts_delivery_current <= df_in.index)
                             & (df_in.index <= self.ts_delivery_current + 900 * fcast_horizon - 1)][column]

            df_y_hat[column] = y_hat

            return df_y_hat

        elif fcast == "ev_close":
            # "realistic" forecast for electric vehicles. As soon as the vehicle arrives, we know the SOC and for how long
            # the vehicle will be available. Nothing is knows beyond the current charging cycle
            df_in = ft.read_dataframe(filepath)
            df_in.set_index("timestamp", inplace=True)

            y_hat = [list(df_in[(self.ts_delivery_current <= df_in.index)
                                & (df_in.index <= self.ts_delivery_current + 900 * fcast_horizon - 1)][column])]

            val_known = y_hat[0][0]
            for i in range(fcast_horizon):
                if y_hat[0][i] == 0:
                    val_known = 0
                y_hat[0][i] = y_hat[0][i] * val_known

            df_y_hat = df_in[(self.ts_delivery_current <= df_in.index)
                             & (df_in.index <= self.ts_delivery_current + 900 * fcast_horizon - 1)][column]

            df_y_hat[column] = y_hat

            return df_y_hat
