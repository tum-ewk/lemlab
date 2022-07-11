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
import feather as ft
import numpy as np
import pandas as pd
from bisect import bisect_left

# suppress tensorflow warnings because there are always some drivers for some graphics card missing.
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'  # or any {'0', '1', '2', '3'}


class ForecastManager:

    """
    ForecastManager provides functions for the forecasting of prosumer time series.

    Forecasting is divided into two steps: model retraining and forecast update.

    Retraining is the step in which historical data are analysed and model weights reassigned.
    Forecast update is the application of the trained model to the most recent input data to generate a new forecast.

    The most recent forecast for each plant is saved to prosumer/fcasts_current.ft for the model predictive controller
    and market agent to make use of.

    Retraining and update frequencies are specified in the plant configuration files.

    Public methods:

        __init__ :         Self explanatory

        update_forecasts:  Retrains and updates all plant forecasts as well as price forecasts for the Prosumer class.
                           Results are saved to the fcast_table DataFrame.

    """

    def __init__(self, prosumer_obj):
        """Create instance of ForecastManager.

        :param prosumer_obj: the Prosumer object that owns this instance of ForecastManager

        """
        self.path_prosumer = prosumer_obj.path
        self.config_dict = prosumer_obj.config_dict
        self.plant_dict = prosumer_obj.plant_dict

        self.fcast_table = None
        for plant in self.plant_dict:
            if self.plant_dict[plant].get("fcast", None) in ["nn"]:
                import tensorflow as tf
                self.tf = tf
            elif self.plant_dict[plant].get("fcast", None) in ["sarma"]:
                from scipy.optimize import minimize as sp_minimize
                self.sp_minimize = sp_minimize

        # set current timestamp from system clock or keyword arg
        self.t_now = prosumer_obj.t_now

        # derive previous and next timestamps
        self.ts_delivery_prev = prosumer_obj.ts_delivery_prev
        self.ts_delivery_current = prosumer_obj.ts_delivery_current

        # organize weather data
        self.df_weather_history = prosumer_obj.df_weather_history
        self.df_weather_fcast = prosumer_obj.df_weather_fcast

    def update_forecasts(self):

        """Public function that calls retraining and updating functions for all plants and prices required by the parent
           Prosumer instance.

           :return: None
              """
        # retrain forecast models as required
        self._retrain_forecasts()

        # retrieve most recent forecast results, update and save back to file
        self._retrieve_fcast_table()
        self._update_all_forecasts()
        ft.write_dataframe(self.fcast_table.reset_index(), f"{self.path_prosumer}/fcasts_current.ft")

    # internal functions

    def _retrain_forecasts(self):
        # iterate through all plants owned by the Prosumer instance
        for plant in self.plant_dict:
            # check when the models were last retrained and how often they should be retrained
            last_retrain = self.plant_dict[plant].get("fcast_last_retrain", 0)
            period_retrain = self.plant_dict[plant].get("fcast_retraining_period", 900)

            if self.ts_delivery_current - last_retrain >= period_retrain:

                # currently only sarma and PV neural networks can be retrained
                if self.plant_dict[plant].get("fcast") == "sarma":
                    # sarma algorithm has issues with objective value overflow, suppress these warnings as the function
                    # works just fine
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        self._train_sarma(id_plant=plant, column="power")
                        # set plant as retrained if it was actually done
                        self.plant_dict[plant]["fcast_last_retrain"] = self.ts_delivery_current

                elif self.plant_dict[plant].get("fcast") == "nn":
                    # the only implementer neural network is the PV neural network
                    # model is automatically saved to file
                    self._train_pv_neural_net(path_objective=f"{self.path_prosumer}/raw_data_{plant}.ft",
                                              id_plant=plant)
                    # set plant as retrained if it was actually done
                    self.plant_dict[plant]["fcast_last_retrain"] = self.ts_delivery_current

        # save retraining timestamps to file
        with open(f"{self.path_prosumer}/config_plants.json", "w") as write_file:
            json.dump(self.plant_dict, write_file)

    def _update_all_forecasts(self):
        # update forecasts for all plants operated by the owning Prosumer instance
        if self.config_dict["mpc_horizon"] > 0:
            # get forecasts for physical plants

            for plant in self.plant_dict:
                # check last update time
                last_update = self.plant_dict[plant].get("fcast_last_update", 0)
                period_update = self.plant_dict[plant].get("fcast_update_period", 900)

                # if time for forecasts to be updated again, do it!
                if self.ts_delivery_current - last_update >= period_update:
                    if self.plant_dict[plant].get("type") in ["pv", "fixedgen"]:
                        # retrieve forecast for pv and fixed_gen plants, scale PU forecast by plant power
                        df_temp = self.__update_single_forecast(id_plant=plant)
                        df_temp["power"] *= self.plant_dict[plant].get("power")
                        # rename column and merge into forecast table
                        df_temp.rename(columns={'power': f'power_{plant}'}, inplace=True)
                        self.fcast_table = self.fcast_table.join(df_temp, how="outer", lsuffix=f"duplicate")

                    elif self.plant_dict[plant].get("type") in ["wind"]:
                        # retrieve forecast for wind plants
                        df_temp = self.__update_single_forecast(id_plant=plant, column="wind_speed")
                        df_temp.rename(columns={'wind_speed': f'wind_speed_{plant}'}, inplace=True)

                        # translate wind speed into power generation according to plant spec file
                        with open(f"{self.path_prosumer}/spec_{plant}.json") as read_file:
                            spec_file = json.load(read_file)

                        lookup_ws = spec_file["wind_speed_m/s"]
                        lookup_power = spec_file["power_pu"]

                        list_ws = list(df_temp[f"wind_speed_{plant}"])
                        list_power = []
                        for ws in list_ws:
                            list_power.append(
                                self._lookup(ws, lookup_ws, lookup_power) * self.plant_dict[plant]["power"])

                        df_temp[f"power_{plant}"] = list_power

                        # merge into forecast table
                        self.fcast_table = self.fcast_table.join(df_temp, how="outer", lsuffix="duplicate")

                    elif self.plant_dict[plant].get("type") == "hh":
                        # retrieve hh forecast and merge into forecast table
                        df_temp = self.__update_single_forecast(id_plant=plant)
                        df_temp.rename(columns={'power': f'power_{plant}'}, inplace=True)
                        self.fcast_table = self.fcast_table.join(df_temp, how="outer", lsuffix=f"duplicate")

                    elif self.plant_dict[plant].get("type") == "bat":
                        # Battery is not forecast.
                        # We create the columns for power and soc to be set by the controller later
                        self.fcast_table[f'power_{plant}'] = 0
                        self.fcast_table[f'soc_{plant}'] = 0

                    elif self.plant_dict[plant].get("type") == "ev":
                        # retrieve electric vehicle forecasts for availability AND distance driven
                        df_temp = self.__update_single_forecast(id_plant=plant, column="availability")
                        df_temp.rename(columns={'availability': f'availability_{plant}'}, inplace=True)
                        self.fcast_table = self.fcast_table.join(df_temp, how="outer", lsuffix=f"duplicate")

                        df_temp = self.__update_single_forecast(id_plant=plant, column="distance_driven")
                        df_temp.rename(columns={'distance_driven': f'distance_driven_{plant}'}, inplace=True)
                        self.fcast_table = self.fcast_table.join(df_temp, how="outer", lsuffix=f"duplicate")
                        # empty columns created for EV analogously to battery
                        self.fcast_table[f'power_{plant}'] = 0
                        self.fcast_table[f'soc_{plant}'] = 0
                        self.fcast_table[f'soc_min_{plant}'] = 0

                    elif self.plant_dict[plant].get("type") == "hp":
                        # retrieve heat pump temperature forecasts
                        weather_fcast = "weather_perfect" if self.plant_dict[plant].get("fcast") == "perfect" else "weather_fcast"
                        df_temp = self.__update_single_forecast(id_plant=plant,
                                                                fcast=weather_fcast, column="temp")

                        df_temp.rename(columns={'temp': f'temp_{plant}'}, inplace=True)
                        self.fcast_table = self.fcast_table.join(df_temp, how="outer", lsuffix=f"duplicate")

                        # retrieve heat load forecasts
                        df_temp = self.__update_single_forecast(id_plant=plant, column="heat")
                        df_temp.rename(columns={'heat': f'heat_{plant}'}, inplace=True)
                        self.fcast_table = self.fcast_table.join(df_temp, how="outer", lsuffix=f"duplicate")

                        # empty columns created for HP analogously to battery
                        self.fcast_table[f'power_{plant}'] = 0
                        self.fcast_table[f'soc_{plant}'] = 0
                        self.fcast_table[f'cop_{plant}'] = 0

                    # if plant was updated, save this to the spec file
                    self.plant_dict[plant]["fcast_last_update"] = self.ts_delivery_current
                    with open(f"{self.path_prosumer}/config_plants.json", "w") as write_file:
                        json.dump(self.plant_dict, write_file)

            # These forecasts are handled separately from those for plants
            # LEM prices are forecast either naively (same as yesterday)
            if self.config_dict["mpc_price_fcast"] == "naive":
                last_update = self.config_dict.get("mpc_price_fcast_last_update", 0)
                period_update = self.config_dict.get("mpc_price_fcast_update_period")

                if self.ts_delivery_current - last_update >= period_update:
                    # return all predicted values in one list
                    df_temp = self.__update_single_forecast(
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
            # or flat (market price is always exactly the average between the market floor and ceiling)
            else:
                self.fcast_table[f'price'] = (self.config_dict["max_bid"] + self.config_dict["min_offer"]) / 2

            # Levies prices always perform a naive forecast. Perfect forecasts require the retailer/network operator
            # to post settlement prices on the market in advance

            df_temp = self.__update_single_forecast(
                        fcast="naive",
                        column="price_energy_levies_positive",
                        fcast_horizon=self.config_dict["mpc_horizon"],
                        filepath=f"{self.path_prosumer}/price_history.ft"
                        )

            self.fcast_table = self.fcast_table.join(df_temp, how="outer", lsuffix="duplicate")
            df_temp = self.__update_single_forecast(
                        fcast="naive",
                        column="price_energy_levies_negative",
                        fcast_horizon=self.config_dict["mpc_horizon"],
                        filepath=f"{self.path_prosumer}/price_history.ft"
                        )
            self.fcast_table = self.fcast_table.join(df_temp, how="outer", lsuffix="duplicate")

            # drop duplicates from the table and then throw away old data
            self.fcast_table.drop(list(self.fcast_table.filter(regex='duplicate')), axis=1, inplace=True)
            self.fcast_table = self.fcast_table[self.fcast_table.index >= self.ts_delivery_current]

    # very internal functions

    def __update_single_forecast(self,
                                 id_plant=None,
                                 fcast=None,
                                 fcast_horizon=None,
                                 filepath=None,
                                 column="power"):
        """
        Takes the forecast model "fcast" for plant "id_plant" and applies it to the data in "column" of "filepath" and returns a forecast
        starting at ts_delivery_current for "fcast_horizon" steps.

        id_plant is optional as price forecasts don't require a plant to be attached.

        :param id_plant: string, id of plant to be forecast
        :param fcast: string, type of fcast model to be used e.g. "sarma" or "perfect"
        :param fcast_horizon: int, how many timesteps should the forecast contain?
        :param filepath: string, path to the data
        :param column: string, name of the data column to be forecast

        :return obj: float, RMSE of the SARMA model

        """

        if id_plant is not None:
            fcast_param = self.plant_dict[id_plant].get("fcast_param")
            fcast_order = self.plant_dict[id_plant].get("fcast_order")

            if fcast_horizon is None:
                fcast_horizon = self.config_dict["mpc_horizon"] + \
                                self.plant_dict[id_plant].get("fcast_update_period", 900) // 900

        if fcast is None:
            fcast = self.plant_dict[id_plant].get("fcast")

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
            # neural network for pv plant
            # load saved neural network model
            path = pathlib.Path(filepath)
            nn_model = self.tf.keras.models.load_model(path.parent.joinpath(f"fcast_model_{id_plant}.hdf5"))
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

        elif fcast == "weather_perfect":
            # perfect forecast on a weather parameter "column"
            df_wind_perfect = \
                pd.DataFrame(
                    self.df_weather_history.loc[
                        slice(self.ts_delivery_current, self.ts_delivery_current + 900 * fcast_horizon),
                        slice(None)]
                    [column].droplevel('ts_delivery_fcast'))
            df_wind_perfect.index.name = "timestamp"
            return df_wind_perfect

        elif fcast == "weather_fcast":
            # actual forecast on a weather parameter "column"
            df_wind_fcast = \
                pd.DataFrame(
                    self.df_weather_fcast.loc[
                        (slice(self.ts_delivery_current, self.ts_delivery_current),
                         slice(self.ts_delivery_current, self.ts_delivery_current + 900 * fcast_horizon)),
                        :]
                    [column].droplevel('ts_delivery_current'))
            df_wind_fcast.index.name = "timestamp"
            return df_wind_fcast

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

    def _train_sarma(self, id_plant, column):
        """
        Trains a SARMA model of the given order and the set initial parameters
        and returns the optimized model parameters.

        :param id_plant: string, id of the plant model to be trained
        :param column
        :return optimal_param: list, new SARMA model parameters after training
        """

        # read historical values and create time series to be utilities from
        df_in = ft.read_dataframe(f"{self.path_prosumer}/raw_data_{id_plant}.ft")
        df_in.set_index("timestamp", inplace=True)

        y = list(df_in[(df_in.index < self.ts_delivery_prev)][column] /
                 df_in[(df_in.index < self.ts_delivery_prev)][column].max()*2)

        # import model hyper parameters
        order = self.plant_dict[id_plant]["fcast_order"]

        # season lengths
        s1 = order[6]
        s2 = order[10]

        # resize input dataset to match training time
        training_time = s2 * order[7] + 3000
        y = y[-training_time:]

        # generate random starting point for optimization if no initial parameters are supplied
        init = self.plant_dict[id_plant]["fcast_param"]

        if init is None:
            target = 3
            while target > 1 or target < 0.9:
                init = []
                for i in range(sum(order) - s1 - s2):
                    init.append(random.random() * 0.5 - 0.1)
                target = sum(init)

        # execute a gradient search on the model parameters to minimize the RMSE

        result = self.sp_minimize(self._sarma_objective,
                                  x0=init,
                                  method="SLSQP",
                                  args=(y, order),
                                  tol=1e-4)

        # extract optimal fcast parameters
        optimal_param = []
        for val in result.x:
            optimal_param.append(val)
        optimal_param = [round(num, 3) for num in optimal_param]

        self.plant_dict[id_plant]["fcast_param"] = optimal_param
        # save forecast parameters to file
        with open(f"{self.path_prosumer}/config_plants.json", "w") as write_file:
            json.dump(self.plant_dict, write_file)

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

        train_data = self.tf.data.Dataset.from_tensor_slices((x_train, y_train))
        train_data = train_data.repeat(5)
        train_data = train_data.shuffle(buffer_size).batch(batch_size)

        val_data = self.tf.data.Dataset.from_tensor_slices((x_val, y_val))

        val_data = val_data.shuffle(buffer_size).batch(batch_size)

        # internal function that slows down learning rate the further training progresses
        def scheduler(epoch, lr):
            if epoch > 10:
                return lr * self.tf.math.exp(-0.1)
            return lr

        callback_lr = self.tf.keras.callbacks.LearningRateScheduler(scheduler)
        # define callback for early training stoppage in case no more progress is made for 10 steps
        callback_es = self.tf.keras.callbacks.EarlyStopping(monitor='val_loss',
                                    patience=10,
                                    restore_best_weights=True)

        optimizer = self.tf.keras.optimizers.Adam(learning_rate=0.001)

        # define neural network layers
        model = self.tf.keras.models.Sequential([
            self.tf.keras.layers.Dense(26, activation="relu", input_shape=(len(input_par),)),
            self.tf.keras.layers.Dense(10, activation="relu"),
            self.tf.keras.layers.Dense(1),
        ])
        model.compile(optimizer=optimizer, loss='mse')
        model.fit(train_data, epochs=60, verbose=0, validation_data=val_data,
                  callbacks=[callback_lr, callback_es])

        # save neural network to the directory for later retrieval
        path = pathlib.Path(path_objective)
        model.save(path.parent.joinpath(f"fcast_model_{id_plant}.hdf5"))

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
