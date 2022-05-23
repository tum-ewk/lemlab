__author__ = "sdlumpp"
__credits__ = []
__license__ = ""
__maintainer__ = "sdlumpp"
__email__ = "sebastian.lumpp@tum.de"

import json
import numpy as np
import pandas as pd
from random import random
import lemlab.utilities.forecasting as fcast
import warnings


class Aggregator:
    """Aggregator defines objects and functions to simulate an aggregator operating in a local energy market.

       The main tasks of the Aggregator in the local energy market are:
                        - utilities and purchasing energy for the fixed household loads of their customers

        Public methods:

        __init__ :              Create an Aggregator instance from a configuration folder created
                                using the ScenarioExecutor class.

        pre_clearing_activity: Executes all activities required before the market is cleared:
                                1) forecast aggregated loads
                                2) post buy and sell positions on the LEM

        post_clearing_activity: Executes all activities required after market is cleared:
                                1) none
    """
    def __init__(self, path, t_override=None):
        """

        Create an Aggregator instance from a configuration folder created using the ScenarioExecutor class.

        :param path: path to prosumer configuration directory
        :param t_override: pandas Timestamp, if supplied, this parameter forces the MP to use the supplied
                            timestamp, otherwise the current time is used.

        :return None:
        """
        self.path = path
        # load configuration from file
        with open(f"{path}/config_account.json", "r") as read_file:
            self.config_dict = json.load(read_file)
        # set current timestamp from system clock or keyword arg
        self.t_now = t_override if t_override else pd.Timestamp.now().timestamp()
        # derive previous and next timestamps
        self.ts_delivery_prev = round(pd.Timestamp(self.t_now, unit="s").floor("15min").timestamp() - 15*60)
        self.ts_delivery_current = self.ts_delivery_prev + 15*60
        # initialize instance attributes
        self.mpc_table = None
        self.matched_bids = None
        self.matched_bids_by_timestep = None

    def pre_clearing_activity(self, db_obj, flag_retrain_forecasts, clear_positions=False):
        """
        Executes all activities required before the market is cleared.

        :param db_obj: DatabaseConnection instance, provides database connection
        :param flag_retrain_forecasts: bool, if True, forecasts models are retrained
        :param clear_positions: bool, if True, clear all open positions from ex-ante markets before posting
                                      new positions

        :return None:
        """
        self.update_user_preferences(db_obj)
        # then, retrain forecasts if necessary
        if flag_retrain_forecasts:
            self.retrain_forecasts()
        self.get_predictions()
        self.get_market_results(db_obj=db_obj)
        self.market_agent(db_obj=db_obj, clear_positions=clear_positions)

    def post_clearing_activity(self, db_obj):
        """
        Executes all activities required after the market is cleared.

        :param db_obj: DatabaseConnection instance, provides database connection

        :return None:
        """
        pass

    def update_user_preferences(self, db_obj):
        """
        Updates all user preferences from the database, in case changes to preferences are triggered by the user via
        the web interface.

        :param db_obj: DatabaseConnection instance, provides database connection

        :return None:
        """
        # get contents of user info table
        user_info = db_obj.get_info_user(self.config_dict["id_user"])

        # update the config dict using the user info table
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
        self.config_dict["preference_quality"] = \
            user_info.loc[0, db_obj.db_param.PREFERENCE_QUALITY]
        self.config_dict["id_market_agent"] = \
            user_info.loc[0, db_obj.db_param.ID_MARKET_AGENT]
        self.config_dict["premium_preference_quality"] = \
            float(user_info.loc[0, db_obj.db_param.PREMIUM_PREFERENCE_QUALITY])
        # save the new config dict
        with open(f"{self.path}/config_account.json", "w") as write_file:
            json.dump(self.config_dict, write_file)

    def get_market_results(self, db_obj):
        """
        Retrieve ex-ante market results for the market participant

        :param db_obj: DatabaseConnection instance, provides database connection

        :return None:
        """
        self.matched_bids, self.matched_bids_by_timestep = db_obj.get_results_market_ex_ante(
            id_user=self.config_dict['id_market_agent'],
            ts_delivery_first=self.ts_delivery_prev,
            ts_delivery_last=self.ts_delivery_current + self.config_dict["ma_horizon"]*900
            )

    def market_agent(self, db_obj, clear_positions=False):
        """
        Calculate and post/update market positions to the double sided market.

        :param db_obj: Database instance, pass the database connection instance to this method
        :param clear_positions: bool, if True, clear all open positions from ex-ante markets before posting
                                      new positions

        :return None:
        """
        # generate list of potential bids from MPC results
        # all grid flows are potential bids
        df_potential_bids = pd.DataFrame(
            self.mpc_table[(self.ts_delivery_current <= self.mpc_table.index)
                           & (self.mpc_table.index <= self.ts_delivery_current
                              + 15*60*self.config_dict["ma_horizon"])]
            [f"pred_load"])

        df_potential_bids.rename(columns={f"pred_load": "net_bids"}, inplace=True)

        df_potential_bids["net_bids"] = df_potential_bids["net_bids"] / 4 - self.matched_bids_by_timestep["net_bids"]

        dict_pot_bids = df_potential_bids.to_dict()
        index_pot_bids = sorted(list(dict_pot_bids["net_bids"]))
        # dictionary that holds the positions before posting to db
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

        # conversion factor from euros per kwh to sigma (fixed point db currency) per wh
        euro_kwh_to_sigma_wh = db_obj.db_param.EURO_TO_SIGMA / 1000
        # initialize params for linear bidding strategy
        linear_delta = 0
        linear_gradient = (self.config_dict["max_bid"] - self.config_dict["min_offer"]) / (
                           self.config_dict["ma_horizon"] - 1)
        # loop through all potential bidding periods and generate positions
        # ignore periods that cannot be bid for anymore
        for ts_d in [ts_d_pot for ts_d_pot in index_pot_bids if ts_d_pot > self.ts_delivery_current]:
            # determine energy to be traded
            energy_position = round(dict_pot_bids["net_bids"][ts_d])
            # minimum trading qty
            post_position = True if abs(energy_position) >= 10 else False

            # set quality preference
            if energy_position < 0:
                quality = self.config_dict["preference_quality"]
                premium = self.config_dict["premium_preference_quality"]
            else:
                quality = "na"
                premium = 0
            # ZI strategy chooses random prices in range
            if self.config_dict["ma_strategy"] == "zi":
                # determine energy price,
                price = self.config_dict["min_offer"] \
                        + random() * (self.config_dict["max_bid"] - self.config_dict["min_offer"])
                price = round(price, 4)
                price *= euro_kwh_to_sigma_wh
            # linear trading strategy determines price based on how far into the future ts_d lies
            else:
                if energy_position < 0:
                    price = round(self.config_dict["max_bid"] - linear_delta, 6)
                    price *= euro_kwh_to_sigma_wh
                else:
                    price = round(self.config_dict["min_offer"] + linear_delta, 6)
                    price *= euro_kwh_to_sigma_wh
            # save position if it is to be posted
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
                dict_positions[db_obj.db_param.TS_DELIVERY].append(ts_d)
            # increment linear algorithm's param
            linear_delta += linear_gradient

        # clear old open positions if required
        if clear_positions:
            db_obj.clear_positions(id_user=self.config_dict['id_market_agent'])

        # post positions if there are any
        if len(dict_positions[db_obj.db_param.ID_USER]) > 0:
            df_bids = pd.DataFrame(dict_positions)
            db_obj.post_positions(df_bids,
                                  t_override=self.t_now)

    def get_predictions(self):
        """
        Performs utilities on aggregated load profile according the forecast algorithms specified in the user config.

        :return None:
        """
        # get forecasts using utilities.py
        agg_pred = fcast.get_forecast(fcast=self.config_dict["fcast"],
                                      fcast_horizon=self.config_dict["ma_horizon"],
                                      fcast_order=self.config_dict["fcast_sarma_order"],
                                      fcast_param=self.config_dict["fcast_param"],
                                      ts_delivery_current=self.ts_delivery_current,
                                      filepath=f"{self.path}/raw_data_aggregated_loads.ft"
                                      )
        # return all predicted values in one list
        list_ts = list(range(self.ts_delivery_prev,
                             self.ts_delivery_prev + 15*60*self.config_dict["ma_horizon"], 15*60))
        data_predicted = np.transpose([list_ts, agg_pred])

        self.mpc_table = pd.DataFrame(data_predicted,
                                      columns=["ts_delivery", "pred_load"]).set_index("ts_delivery")

    def retrain_forecasts(self):
        """
        Retrains forecast models

        :return None:
        """
        # currently only the sarma model needs to be retrained
        if self.config_dict["fcast"] == "sarma":
            # catch warnings, as the primitive gradient optimization for the sarma training
            # often causes overflow warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                new_param = fcast.train_sarma(filepath=f"{self.path}/raw_data_aggregated_loads.ft",
                                              ts_delivery_prev=self.ts_delivery_prev,
                                              fcast_order=self.config_dict["fcast_sarma_order"],
                                              fcast_param_init=self.config_dict["fcast_param"]
                                              )
            self.config_dict["fcast_param"] = new_param
            # save new parameters to the config file
            with open(f"{self.path}/config_account.json", "w") as write_file:
                json.dump(self.config_dict, write_file)
        else:
            # placeholder for future forecasts to be retrained
            pass
