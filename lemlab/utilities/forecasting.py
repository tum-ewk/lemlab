__author__ = "sdlumpp"
__credits__ = []
__license__ = ""
__maintainer__ = "sdlumpp"
__email__ = "sebastian.lumpp@tum.de"

import warnings
import random
import json
import pathlib
import os
from scipy.optimize import minimize as sp_minimize
import feather as ft
import numpy as np
import pandas as pd
import tensorflow as tf

# suppress all tensorflow output
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'  # or any {'0', '1', '2'}

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

        self.fcast_table = None

        # set current timestamp from system clock or keyword arg
        self.t_now = t_override if t_override else pd.Timestamp.now().timestamp()

        # derive previous and next timestamps
        self.ts_delivery_prev = round(pd.Timestamp(self.t_now, unit="s").floor("15min").timestamp() - 15*60)
        self.ts_delivery_current = self.ts_delivery_prev + 15*60

        # organize weather data
        self.df_weather_history = df_weather_history
        self.df_weather_fcast = df_weather_fcast

    def update_forecasts(self):

        """Return generation prediction for PV plants, consumption predictions for fixedgen loads, as well
              as well as market price predictions for the instance prediction horizon

              Current status: all forecasts are currently perfect predictions determined by looking at future values in
              the input data

              :return: None
              """
        # retrain forecast models as required
        self._retrain_forecasts()

        # retrieve most recent forecast results, update and save back to file
        self._retrieve_fcast_table()
        self._update_all_forecasts()
        ft.write_dataframe(self.fcast_table.reset_index(), f"{self.path_prosumer}/fcasts_current.ft")

    # internal functions

    def _retrieve_fcast_table(self):
        if os.path.exists(f"{self.path_prosumer}/fcasts_current.ft"):
            self.fcast_table = ft.read_dataframe(f"{self.path_prosumer}/fcasts_current.ft").set_index("timestamp")
        else:
            ts_init = [[ts, 0] for ts in range(self.ts_delivery_current,
                                               self.ts_delivery_current + self.config_dict["mpc_horizon"] * 900,
                                               900)]
            self.fcast_table = pd.DataFrame(ts_init,
                                            columns=["timestamp", f"power_{self.config_dict['id_meter_grid']}"]
                                            ).set_index("timestamp")

    def _retrain_forecasts(self):
        for plant in self.plant_dict:
            last_retrain = self.plant_dict[plant].get("fcast_last_retrain", 0)
            period_retrain = self.plant_dict[plant].get("fcast_retraining_period", 900)

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
                    self._train_pv_neural_net(path_objective=f"{self.path_prosumer}/raw_data_{plant}.ft",
                                              id_plant=plant)

                self.plant_dict[plant]["fcast_last_retrain"] = self.ts_delivery_current
            else:
                pass

        with open(f"{self.path_prosumer}/config_plants.json", "w") as write_file:
            json.dump(self.plant_dict, write_file)

    def _train_sarma(self, filepath, fcast_order, fcast_param_init=None):

        """
        Trains a SARMA model of the given order and the set initial parameters
        and returns the optimized model parameters.

        :param filepath: string, path to the raw data
        :param fcast_order: list, order of SARMA model, see config.YAML for a description
        :param fcast_param_init: list, initial values of the SARMA model parameters

        :return optimal_param: list, new SARMA model parameters after training
        """

        # read historical values and create time series to be utilities from
        df_in = ft.read_dataframe(filepath)
        df_in.set_index("timestamp", inplace=True)
        y = list(df_in[(df_in.index < self.ts_delivery_prev)]["power"] /
                 df_in[(df_in.index < self.ts_delivery_prev)]["power"].max()*2)

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

    @staticmethod
    def _sarma_objective(par, training_data, order=None):
        """
        Calculates the RMSE of the forecast model "par" of order "order" on the training data.
        Used for training of SARMA models.

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

    def _prepare_data_weather(self,
                              path_objective, input_par,
                              ts_d_first, ts_d_last,
                              train_or_predict="train", real_fcasts=True):

        # get weather history for correct range
        training_data = self.df_weather_history.loc[(slice(ts_d_first, ts_d_last), slice(None))]
        training_data = training_data.drop(training_data.columns.difference(input_par.keys()), axis=1)

        # for solar PV forecasts, create the maximum potential power curve, essentially the clear sky index for this
        # location
        df_raw_data = ft.read_dataframe(f"{path_objective}").set_index("timestamp")
        if "mppc" not in df_raw_data.columns:
            df_raw_data = self._calc_mppc(df_raw_data)
            ft.write_dataframe(df_raw_data.reset_index(), f"{path_objective}")

        # cut raw data to the required length
        df_raw_data = df_raw_data[(df_raw_data.index >= ts_d_first) & (df_raw_data.index <= ts_d_last)]

        if train_or_predict == "train" or real_fcasts is False:
            # get power and MPPC data for this location if we are training
            training_data["power"] = list(df_raw_data["power"])
            training_data["mppc"] = list(df_raw_data["mppc"])
            nn_data_normalized = training_data.copy()

        elif real_fcasts:
            # get weather forecast if we are forecasting
            fcasting_data = self.df_weather_fcast.loc[(slice(self.ts_delivery_current, self.ts_delivery_current),
                                                       slice(self.ts_delivery_current, ts_d_last)), :].copy()
            fcasting_data["mppc"] = list(df_raw_data["mppc"])
            nn_data_normalized = fcasting_data.copy()

        # normalize input data for forecast models
        for key in input_par.keys():
            var_range = input_par[key][1] - input_par[key][0]
            nn_data_normalized[key] = \
                nn_data_normalized[key].sub(input_par[key][0]).div(var_range)

        # if we are training PV models, the forecast power should be normalized to maximum potential power
        if train_or_predict == "train":
            nn_data_normalized = nn_data_normalized[nn_data_normalized["mppc"] >= 0.05]
            nn_data_normalized["power"] = \
                nn_data_normalized["power"].div(nn_data_normalized["mppc"], fill_value=0)
        return nn_data_normalized

    @staticmethod
    def _calc_mppc(df_raw_data):
        df_raw_data["mppc"] = 0
        for ix, row in df_raw_data.iterrows():
            mppc = 0
            t_since_start = ix - df_raw_data.index[0]
            for j in range(1, 20):
                if j * 86400 <= t_since_start:
                    mppc = max(mppc, df_raw_data.loc[ix - j * 86400, "power"])
            df_raw_data.loc[ix, "mppc"] = mppc
        return df_raw_data

    def _train_pv_neural_net(self, path_objective, id_plant):
        """
        Train and save artificial neural network model for weather-based forecasts (pv, wind, heat pump)

        """
        # define input parameters and their ranges for data retrieval and normalization
        input_par = {'temp': [-10 + 273.15, 35 + 273.15],
                     'cloud_cover': [0, 100],
                     'pop': [0, 100],
                     'wind_speed': [0, 30],
                     'wind_dir': [0, 360],
                     'ghi': [0, 1000]}

        # training period of 30 days
        ts_d_first = self.ts_delivery_prev - 86400 * 30
        ts_d_last = self.ts_delivery_prev

        # retrieve training data from the weather and pv power files in normalized form, then shuffle it
        training_data_norm = self._prepare_data_weather(path_objective=path_objective,
                                                        input_par=input_par,
                                                        ts_d_first=ts_d_first,
                                                        ts_d_last=ts_d_last)

        training_data_norm = training_data_norm.sample(frac=1, replace=False)

        # select training and validation data, 20 and 80% of dataset respectively
        validation_data = training_data_norm.tail(n=int(len(training_data_norm) * 0.2)).copy()
        training_data = training_data_norm.head(n=int(len(training_data_norm) * 0.8)).copy()

        x_train = training_data[input_par.keys()].to_numpy()
        x_val = validation_data[input_par.keys()].to_numpy()
        y_train = training_data["power"].to_numpy()
        y_val = validation_data["power"].to_numpy()

        # prepare data for neural network training
        buffer_size = 100
        batch_size = 16

        train_data = tf.data.Dataset.from_tensor_slices((x_train, y_train))
        train_data = train_data.repeat(5)
        train_data = train_data.shuffle(buffer_size).batch(batch_size)

        val_data = tf.data.Dataset.from_tensor_slices((x_val, y_val))
        val_data = val_data.shuffle(buffer_size).batch(batch_size)

        # internal function that slows down learning rate the further training progresses
        def scheduler(epoch, lr):
            if epoch > 10:
                return lr * tf.math.exp(-0.1)
            return lr
        callback_lr = tf.keras.callbacks.LearningRateScheduler(scheduler)
        # define callback for early training stoppage in case no more progress is made for 10 steps
        callback_es = tf.keras.callbacks.EarlyStopping(monitor='val_loss',
                                                       patience=10,
                                                       restore_best_weights=True)

        optimizer = tf.keras.optimizers.Adam(learning_rate=0.001)

        # define neural network layers
        model = tf.keras.models.Sequential([
            tf.keras.layers.Dense(26, activation="relu", input_shape=(len(input_par),)),
            tf.keras.layers.Dense(10, activation="relu"),
            tf.keras.layers.Dense(1),
        ])
        model.compile(optimizer=optimizer, loss='mse')
        model.fit(train_data, epochs=60, verbose=0, validation_data=val_data,
                  callbacks=[callback_lr, callback_es])

        # save neural network to the directory for later retrieval
        path = pathlib.Path(path_objective)
        model.save(path.parent.joinpath(f"fcast_model_{id_plant}.hdf5"))

    def _update_all_forecasts(self):
        if self.config_dict["mpc_horizon"] > 0:
            # get forecasts for physical plants
            for plant in self.plant_dict:
                last_update = self.plant_dict[plant].get("fcast_last_update", 0)
                period_update = self.plant_dict[plant].get("fcast_update_period", 900)
                if self.ts_delivery_current - last_update >= period_update:
                    if self.plant_dict[plant].get("type") in ["pv", "fixedgen"]:
                        df_temp = self._update_forecast(id_plant=plant)
                        df_temp.rename(columns={'power': f'power_{plant}'}, inplace=True)
                        self.fcast_table = self.fcast_table.join(df_temp, how="outer", lsuffix=f"duplicate")

                    elif self.plant_dict[plant].get("type") in ["wind"]:
                        df_temp = self._update_forecast(id_plant=plant, column="wind_speed")
                        df_temp.rename(columns={'wind_speed': f'wind_speed_{plant}'}, inplace=True)

                        self.fcast_table = self.fcast_table.join(df_temp, how="outer", lsuffix="duplicate")
                        self.fcast_table[f'power_{plant}'] = 0
                    elif self.plant_dict[plant].get("type") == "hh":
                        df_temp = self._update_forecast(id_plant=plant)
                        df_temp.rename(columns={'power': f'power_{plant}'}, inplace=True)
                        self.fcast_table = self.fcast_table.join(df_temp, how="outer", lsuffix=f"duplicate")

                    elif self.plant_dict[plant].get("type") == "bat":
                        self.fcast_table[f'power_{plant}'] = 0
                        self.fcast_table[f'soc_{plant}'] = 0

                    elif self.plant_dict[plant].get("type") == "ev":
                        df_temp = self._update_forecast(id_plant=plant, column="availability")
                        df_temp.rename(columns={'availability': f'availability_{plant}'}, inplace=True)
                        self.fcast_table = self.fcast_table.join(df_temp, how="outer", lsuffix=f"duplicate")

                        df_temp = self._update_forecast(id_plant=plant, column="distance_driven")
                        df_temp.rename(columns={'distance_driven': f'distance_driven_{plant}'}, inplace=True)
                        self.fcast_table = self.fcast_table.join(df_temp, how="outer", lsuffix=f"duplicate")

                        self.fcast_table[f'power_{plant}'] = 0
                        self.fcast_table[f'soc_{plant}'] = 0
                        self.fcast_table[f'soc_min_{plant}'] = 0

                    self.plant_dict[plant]["fcast_last_update"] = self.ts_delivery_current
                    with open(f"{self.path_prosumer}/config_plants.json", "w") as write_file:
                        json.dump(self.plant_dict, write_file)

            # get forecasts for lem prices
            if self.config_dict["mpc_price_fcast"] != "flat":
                last_update = self.config_dict.get("mpc_price_fcast_last_update", 0)
                period_update = self.config_dict.get("mpc_price_fcast_update_period")

                if self.ts_delivery_current - last_update >= period_update:
                    # return all predicted values in one list
                    df_temp = self._update_forecast(
                        fcast=self.config_dict["mpc_price_fcast"],
                        fcast_horizon=self.config_dict["mpc_horizon"] + period_update//900,
                        filepath=f"{self.path_prosumer}/price_history.ft",
                        column="weighted_average_price",
                    )
                    df_temp.rename(columns={'weighted_average_price': f'price'}, inplace=True)
                    self.fcast_table = self.fcast_table.join(df_temp, how="outer", lsuffix=f"duplicate")

                    self.config_dict["mpc_price_fcast_last_update"] = self.ts_delivery_current
                    with open(f"{self.path_prosumer}/config_account.json", "w") as write_file:
                        json.dump(self.config_dict, write_file)
            else:
                self.fcast_table[f'price'] = \
                    (self.config_dict["max_bid"] - self.config_dict["min_offer"]) / 2 * self.config_dict["mpc_horizon"]

            # predict settlement prices
            # TODO: only predict those settlement prices that have not yet been posted to the DB
            # add perfect predictions

            df_temp = self._update_forecast(
                        fcast="naive",
                        column="price_energy_levies_positive",
                        fcast_horizon=self.config_dict["mpc_horizon"],
                        filepath=f"{self.path_prosumer}/price_history.ft"
                        )

            self.fcast_table = self.fcast_table.join(df_temp, how="outer", lsuffix="duplicate")
            df_temp = self._update_forecast(
                        fcast="naive",
                        column="price_energy_levies_negative",
                        fcast_horizon=self.config_dict["mpc_horizon"],
                        filepath=f"{self.path_prosumer}/price_history.ft"
                        )
            self.fcast_table = self.fcast_table.join(df_temp, how="outer", lsuffix="duplicate")

            self.fcast_table.drop(list(self.fcast_table.filter(regex='duplicate')), axis=1, inplace=True)
            self.fcast_table = self.fcast_table[self.fcast_table.index >= self.ts_delivery_current]

    def _update_forecast(self,
                         id_plant=None,
                         fcast=None,
                         fcast_horizon=None,
                         filepath=None,
                         column="power"):
        """
        Takes a forecast model fcast and applies it to the data in "column" of "filepath" and returns a forecast
        starting at ts_delivery_current for "fcast_horizon" steps.

        :param fcast: string, type of fcast model to be used e.g. "sarma" or "perfect"
        :param fcast_horizon: int, how many timesteps should the forecast contain?
        :param filepath: string, path to the data
        :param column: string, name of the data column to be forecast

        :return obj: float, RMSE of the SARMA model

        """
        if fcast is None:
            fcast = self.plant_dict[id_plant].get("fcast")
        if fcast_horizon is None:
            fcast_horizon = self.config_dict["mpc_horizon"] + \
                            self.plant_dict[id_plant].get("fcast_update_period", 900) // 900
        if id_plant is not None:
            fcast_param = self.plant_dict[id_plant].get("fcast_param")
            fcast_order = self.plant_dict[id_plant].get("fcast_order")
        if filepath is None:
            filepath = f"{self.path_prosumer}/raw_data_{id_plant}.ft"

        if fcast == "sarma":
            # return sarma forecast on data

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
                             & (df_in.index <= self.ts_delivery_current + 900 * fcast_horizon)][column].to_frame()
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
            df_in = ft.read_dataframe(filepath)
            df_in.set_index("timestamp", inplace=True)
            raw_pred_temp = list(df_in[(self.ts_delivery_current - 900 * fcast_param <= df_in.index)
                                 & (df_in.index <= self.ts_delivery_current
                                    + 900 * fcast_horizon - 1 + 900 * fcast_param)]
                                 [column])
            y_hat = []

            ts_pt = self.ts_delivery_current

            for step in range(0, fcast_horizon):

                temp_val = raw_pred_temp[step + fcast_param]
                for i in range(1, fcast_param + 1):
                    temp_val += raw_pred_temp[step + fcast_param + i]
                    temp_val += raw_pred_temp[step + fcast_param - i]
                temp_val /= 2 * fcast_param + 1

                y_hat.append([ts_pt, temp_val])
                ts_pt += 900

            df_y_hat = pd.DataFrame(y_hat, columns=("timestamp", column)).set_index("timestamp")

            return df_y_hat

        elif fcast == "ev_close":
            # "realistic" forecast for electric vehicles. As soon as the vehicle arrives, we know the SOC and for
            # how long the vehicle will be available. Nothing is knows beyond the current charging cycle
            df_in = ft.read_dataframe(filepath)
            df_in.set_index("timestamp", inplace=True)
            df_in = df_in[(self.ts_delivery_current <= df_in.index)
                          & (df_in.index <= self.ts_delivery_current + 900 * fcast_horizon - 1)]
            df_y_hat = df_in
            state_vehicle = 1
            for ix, row in df_in.iterrows():
                if row["availability"] == 0:
                    state_vehicle = 0
                df_y_hat.loc[ix, column] = row[column] * state_vehicle
            return df_y_hat

        elif fcast == "nn":
            # load saved neural network model
            path = pathlib.Path(filepath)
            nn_model = tf.keras.models.load_model(path.parent.joinpath(f"fcast_model_{id_plant}.hdf5"))
            # set forecasting timeframe
            ts_d_start = self.ts_delivery_current
            ts_d_end = self.ts_delivery_current + 900 * (1 + self.config_dict["mpc_horizon"]
                                                         + self.plant_dict[id_plant].get("fcast_update_period") // 900)

            # define input parameters and their ranges for data retrieval and normalization
            input_par = {'temp': [-10 + 273.15, 35 + 273.15],
                         'cloud_cover': [0, 100],
                         'pop': [0, 100],
                         'wind_speed': [0, 30],
                         'wind_dir': [0, 360],
                         'ghi': [0, 1000]}

            # retrieve external data weather and pv power files in normalized form
            forecasting_data_norm = self._prepare_data_weather(
                path_objective=path,
                input_par=input_par,
                ts_d_first=ts_d_start,
                ts_d_last=ts_d_end,
                train_or_predict="predict",
                real_fcasts=True)

            # apply neural network, multiply normalized forecast by MPPC and set any stray negative forecasts to 0
            x_fcast = forecasting_data_norm[input_par.keys()].to_numpy()
            forecasting_data_norm["power_fcast"] = nn_model.predict(x_fcast, verbose=0)
            forecasting_data_norm["power_fcast"] = forecasting_data_norm["power_fcast"] * forecasting_data_norm["mppc"]
            forecasting_data_norm.loc[(forecasting_data_norm["power_fcast"] <= 0), "power_fcast"] = 0

            # return forecast result
            df_y_hat = forecasting_data_norm.reset_index()
            df_y_hat.drop(df_y_hat.columns.difference(['ts_delivery_fcast', 'power_fcast']), axis=1, inplace=True)
            df_y_hat.rename(columns={"ts_delivery_fcast": "timestamp", "power_fcast": "power"}, inplace=True)
            return df_y_hat.set_index("timestamp")
