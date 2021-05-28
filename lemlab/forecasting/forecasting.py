__author__ = "sdlumpp"
__credits__ = []
__license__ = ""
__maintainer__ = "sdlumpp"
__email__ = "sebastian.lumpp@tum.de"

import feather
import random
import numpy as np
from scipy.optimize import minimize as sp_minimize

"""
forecasting provides functions for the forecasting/prediction of household electric load and production for application
in the local energy market.

Forecasting is divided into two steps. Training models and making the forecast. Training models must be done once at
the beginning of a simulation and may be repeated as often or as rarely as desired.

Applying the model to calculate a forecast must be done at least as often as the MPC is executed.

"""


def train_sarma(filepath, ts_delivery_prev, fcast_order, fcast_param_init=None):
    """
    Trains a SARMA model of the given order and the set initial parameters and returns the optimized model parameters.

    :param filepath: string, path to the raw data
    :param ts_delivery_prev: integer, unix timestamp of the last timestep allowed to the used for training
    :param fcast_order: list, order of SARMA model, see config.YAML for a description
    :param fcast_param_init: list, initial values of the SARMA model parameters

    :return optimal_param: list, new SARMA model parameters after training
    """

    # read historical values and create time series to be forecasting from
    df_in = feather.read_dataframe(filepath)
    df_in.set_index("timestamp", inplace=True)
    y = list(df_in[(df_in.index < ts_delivery_prev)]["power"]/df_in[(df_in.index < ts_delivery_prev)]["power"].max()*2)

    # import model hyperparameters
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
    result = sp_minimize(_sarma_objective,
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


def _sarma_objective(par, training_data, order=[3, 0, 3, 3, 0, 3, 96, 2, 0, 2, 96 * 7]):
    """
    Calculates the RMSE of the forecast model "par" of order "order" on the training data. Used for training of SARMA
    models.

    :param par: list, SARMA model parameters to be evaluated
    :param training_data: list, training data the model should be evaluated on
    :param order: list, order of the SARMA model, see config.YAML for further explanation

    :return obj: float, RMSE of the SARMA model
    """
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


def get_forecast(fcast, fcast_horizon, fcast_order, fcast_param, ts_delivery_current, filepath, column="power"):
    """
    Takes a forecast model fcast and applies it to the data in "column" of "filepath" and returns a forecast starting at
    ts_delivery_current for "fcast_horizon" steps.

    :param fcast: string, type of fcast model to be used e.g. "sarma" or "perfect"
    :param fcast_horizon: int, how many timesteps should the forecast contain?
    :param fcast_order: list, order of the SARMA model, see config.YAML for further explanation
    :param fcast_param: list, fcast model parameters, e.g. SARMA parameters
    :param ts_delivery_current: integer, unix timestamp of the first timestep to be forecast
    :param filepath: string, path to the data
    :param column: string, name of the data column to be forecast

    :return obj: float, RMSE of the SARMA model

    """
    if fcast == "sarma":
        # return sarma forecast on data

        # read historical values and create time series to be forecasting from
        df_in = feather.read_dataframe(filepath)
        df_in.set_index("timestamp", inplace=True)
        y = list(df_in[(df_in.index <= ts_delivery_current - 900)][column]
                 / df_in[(df_in.index <= ts_delivery_current - 900)][column].max()*2)

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
            fcast[step] *= df_in[(df_in.index <= ts_delivery_current - 900)][column].max()/2
        return fcast

    elif fcast == "perfect":
        # perfect knowledge of the future
        df_in = feather.read_dataframe(filepath)
        df_in.set_index("timestamp", inplace=True)
        y_hat = list(df_in[(ts_delivery_current <= df_in.index)
                     & (df_in.index <= ts_delivery_current + 900 * fcast_horizon - 1)]
                     [column])
        return y_hat

    elif fcast == "naive":
        # naive forecast, today will be the same as yesterday

        # read historical values and create time series to be forecasting from
        df_in = feather.read_dataframe(filepath)
        df_in.set_index("timestamp", inplace=True)
        y_hat = list(df_in[(df_in.index <= ts_delivery_current - 900)][column])[-fcast_horizon:]
        return y_hat

    elif fcast == "naive_average":
        # naive forecast, today will be the average of the previous 7 days

        # read historical values and create time series to be forecasting from
        df_in = feather.read_dataframe(filepath)
        df_in.set_index("timestamp", inplace=True)
        y = list(df_in[(df_in.index <= ts_delivery_current - 900)][column])
        y_hat = []
        for step in range(-fcast_horizon, 0, 1):
            val = sum([y[step - i * 96] for i in range(7)])
            y_hat.append(val / 7)
        return y_hat

    elif fcast == "aggregator":
        # return a zero forecast if plant is aggregated
        y_hat = [0]*fcast_horizon
        return y_hat

    elif fcast == "smoothed":
        # moving average "perfect filter" forecast

        df_in = feather.read_dataframe(filepath)
        df_in.set_index("timestamp", inplace=True)
        raw_pred_temp = list(df_in[(ts_delivery_current - 900 * fcast_param <= df_in.index)
                             & (df_in.index <= ts_delivery_current + 900 * fcast_horizon - 1 + 900 * fcast_param)]
                             [column])
        y_hat = []
        for step in range(0, fcast_horizon):
            temp_val = raw_pred_temp[step + fcast_param]
            for i in range(1, fcast_param + 1):
                temp_val += raw_pred_temp[step + fcast_param + i]
                temp_val += raw_pred_temp[step + fcast_param - i]
            temp_val /= 2 * fcast_param + 1
            y_hat.append(temp_val)
        return y_hat

    elif fcast == "ev_perfect":
        # perfect forecast for electric vehicles
        df_in = feather.read_dataframe(filepath)
        df_in.set_index("timestamp", inplace=True)
        y_hat = [list(df_in[(ts_delivery_current <= df_in.index)
                            & (df_in.index <= ts_delivery_current + 900 * fcast_horizon - 1)]["availability"]),
                 list(df_in[(ts_delivery_current <= df_in.index)
                            & (df_in.index <= ts_delivery_current + 900 * fcast_horizon - 1)]["distance_driven"])]
        return y_hat

    elif fcast == "ev_close":
        # "realistic" forecast for electric vehicles. As soon as the vehicle arrives, we know the SOC and for how long
        # the vehicle will be available. Nothing is knows beyond the current charging cycle
        df_in = feather.read_dataframe(filepath)
        df_in.set_index("timestamp", inplace=True)
        y_hat = [list(df_in[(ts_delivery_current <= df_in.index)
                            & (df_in.index <= ts_delivery_current + 900 * fcast_horizon - 1)]["availability"]),
                 list(df_in[(ts_delivery_current <= df_in.index)
                            & (df_in.index <= ts_delivery_current + 900 * fcast_horizon - 1)]["distance_driven"])]
        val_known = y_hat[0][0]
        for i in range(fcast_horizon):
            if y_hat[0][i] == 0:
                val_known = 0
            y_hat[0][i] = y_hat[0][i] * val_known
            y_hat[1][i] = y_hat[1][i] * val_known
        return y_hat
