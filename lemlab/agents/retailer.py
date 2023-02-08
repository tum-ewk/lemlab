__author__ = "sdlumpp"
__credits__ = []
__license__ = ""
__maintainer__ = "sdlumpp"
__email__ = "sebastian.lumpp@tum.de"

import pandas as pd
import json


class Retailer:
    """Retailer defines objects and functions to simulate an electricity retailer participating in
       a local energy market.

       The main tasks of the Retailer in the local energy market are:
                        - purchasing surplus energy and covering excess demand to ensure security of supply in the
                           local energy market

        Public methods:

        __init__ :              Create an Retailer instance from a configuration folder created
                                using the ScenarioExecutor class.

        pre_clearing_activity: Executes all activities required before the market is cleared:
                                1) post buy and sell positions

        post_clearing_activity: Executes all activities required after market is cleared:
                                1) none
    """

    def __init__(self, path, t_override=None):
        """
        Create an instance of the Retailer class from a configuration folder created using the ScenarioExecutor class.

        :param path:        String, path to the configuration directory of the retailer to be initialized
        :param t_override:  Integer, unix timestamp, specifies the current simulation time. If None, current time used

        :return None:
        """
        # set current timestamp from system clock or keyword arg
        self.t_now = t_override if t_override else pd.Timestamp.now().timestamp()
        # derive previous and next timestamps
        self.ts_delivery_prev = round(pd.Timestamp(self.t_now, unit="s").floor("15min").timestamp() - 15*60)
        self.ts_delivery_current = self.ts_delivery_prev + 15*60

        self.path = path
        with open(f"{self.path}/config_account.json", "r") as read_file:
            self.config_dict = json.load(read_file)

    def pre_clearing_activity(self, db_obj, clear_positions=False):
        """
        Executes all activities required before the market is cleared.

        :param db_obj: DatabaseConnection instance, provides database connection
        :param clear_positions: bool, if True, clear all open positions from ex-ante markets before posting
                                      new positions

        :return None:
        """
        if db_obj.lem_config["types_clearing_ex_ante"]:
            self.market_agent(db_obj=db_obj, clear_positions=clear_positions)

    def post_clearing_activity(self, db_obj):
        """
        Executes all activities required after the market is cleared.

        :param db_obj: DatabaseConnection instance, provides database connection

        :return None:
        """
        pass

    def market_agent(self, db_obj, clear_positions=False):
        """
        Calculates and posts/updates market positions (price-energy pairs) to the local energy market.

        :param db_obj: DatabaseConnection instance, provides database connection
        :param clear_positions: bool, if True, clear all open positions from ex-ante markets before posting
                                      new positions

        :return None:
        """

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
        for type_position in ["bid", "offer"]:
            if type_position == "bid":
                price = self.config_dict["price_buy"] * euro_kwh_to_sigma_wh
            else:
                price = self.config_dict["price_sell"] * euro_kwh_to_sigma_wh
            dict_positions[db_obj.db_param.ID_USER].append(self.config_dict["id_market_agent"])
            dict_positions[db_obj.db_param.QTY_ENERGY].append(
                self.config_dict["qty_energy_bid"] if type_position == "bid" else self.config_dict["qty_energy_offer"])
            dict_positions[db_obj.db_param.TYPE_POSITION].append(type_position)
            dict_positions[db_obj.db_param.NUMBER_POSITION].append(0)
            dict_positions[db_obj.db_param.STATUS_POSITION].append(0)
            dict_positions[db_obj.db_param.PRICE_ENERGY].append(price)
            dict_positions[db_obj.db_param.QUALITY_ENERGY].append(self.config_dict["quality"])
            dict_positions[db_obj.db_param.PREMIUM_PREFERENCE_QUALITY].append(0)
            dict_positions[db_obj.db_param.T_SUBMISSION].append(self.t_now)
            dict_positions[db_obj.db_param.TS_DELIVERY].append(self.ts_delivery_current + 15 * 60)

        if clear_positions:
            db_obj.clear_positions(id_user=self.config_dict['id_market_agent'])

        if len(dict_positions[db_obj.db_param.ID_USER]) > 0:
            df_bids = pd.DataFrame(dict_positions)
            db_obj.post_positions(df_bids,
                                  t_override=self.t_now)
