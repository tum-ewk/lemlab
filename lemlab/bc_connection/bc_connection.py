import lemlab.bc_connection.bc_param as bc_param
import time
import json
import os
import re
from web3 import Web3, HTTPProvider
import pandas as pd
from pathlib import Path
from tqdm import tqdm


class BlockchainConnection:
    """Blockchain connection provides all connection methods required by lemlab.
       In order to remain database-agnostic no other module may connect to the blockchain directly."""

    def __init__(self, bc_dict):
        try:
            web3_instance = Web3(HTTPProvider("http://" + bc_dict.get("host") + ":"
                                              + bc_dict.get("port"),
                                              request_kwargs={'timeout': bc_dict.get("timeout")}))
            # getting abi, bytecode, address via json file created by truffle
            json_path = os.path.join(str(Path(__file__).parent.parent.parent), 'Truffle', 'build', 'contracts',
                                     bc_dict.get("contract_name") + '.json')
            with open(json_path) as json_file:
                data = json.load(json_file)
            contract_bytecode = data['bytecode']
            contract_address = data['networks'][str(bc_dict.get("network_id"))]['address']
            contract_abi = json.dumps(data['abi'])
            # create contract instance with the coinbase and the contract function
            self.platform = web3_instance.eth.contract(address=contract_address,
                                                       abi=contract_abi,
                                                       bytecode=contract_bytecode)
            self.web3_eth = web3_instance.eth
            self.coinbase = web3_instance.eth.coinbase
            self.functions = self.platform.functions
        except Exception as e:
            print(e)
            assert False

    ###################################################
    # Functions for the info_user table
    # Market participants only

    def get_info_user(self, user_id=""):
        user_info_list = self.functions.get_user_infos().call()
        user_info_df = pd.DataFrame(user_info_list, columns=bc_param.info_user_column_names)
        if user_id != "":
            user_info_df = user_info_df[user_info_df["id_user"] == user_id]
            if user_info_df.empty:
                print("User is not stored.")
        return user_info_df

    def get_list_all_users(self, ts_delivery_active=None, return_list=False):
        """
        Returns a list of all the users, either in list format or in dataframe format
        Parameters
        ----------
        ts_delivery_active: optional argument to filter out users by the time they delivered energy
        return_list: bool: if the returned value is a list of lists or a dataframe

        Returns: Either list of lists or dataframe of all the users registered in the blockchain
        --------

        """
        user_info_list = self.functions.get_user_infos().call()
        user_info_df = pd.DataFrame(user_info_list, columns=bc_param.info_user_column_names)
        if ts_delivery_active is not None:
            final_list = []
            for _, user in user_info_df.iterrows():
                if user[bc_param.TS_DELIVERY_FIRST] <= ts_delivery_active <= user[bc_param.TS_DELIVERY_LAST]:
                    final_list.append(dict(user))
            user_info_list = final_list
            user_info_df = pd.DataFrame(final_list, columns=bc_param.info_user_column_names)
        if return_list:
            return user_info_list
        else:
            return user_info_df

    # Admins only

    def register_user(self, df_user):
        tx_hash = self.functions.push_user_info(tuple(df_user.values.tolist()[0])).transact({'from': self.coinbase})
        return tx_hash

    def delete_user(self, df_user, del_meters=True):
        # deletes a user, additionally, it also deletes all the meters corresponding to that user
        tx_hash = self.functions.delete_user(tuple(df_user.values.tolist()[0]), del_meters).transact(
            {'from': self.coinbase})
        return tx_hash

    def edit_user(self, df_user):
        """
        Function to edit the info of a user. For that, we assume that the user_id stays the same, in which case
        we delete it from the blockchain and then re_uploaded it to the last position of the array of users.
        Parameters
        ----------
        df_user: single dataframe with the info of a user

        Returns
        -------
        The tx_hash of the user registered

        """
        tx_hash = self.delete_user(df_user, del_meters=False)
        self.wait_for_transact(tx_hash)
        return self.register_user(df_user)

    ###################################################
    # Functions for the meter registration table
    # Market participants only

    def get_info_meter(self, meter_id=""):
        meter_info_list = self.functions.get_id_meters().call()
        meter_info_df = pd.DataFrame(meter_info_list, columns=bc_param.info_meter_column_names)
        if meter_id != "":
            meter_info_df = meter_info_df[meter_info_df["id_meter"] == meter_id]
            if meter_info_df.empty:
                print("User is not stored.")
        return meter_info_df

    def get_list_main_meters(self, ts_delivery_active=None, return_list=False):
        """
        Function to get the list of the main meters only. It first retrieves all the meters already filtered by time
        and then uses regex to filter out the main meters, which are the ones with grid in the name
        Parameters
        ----------
        ts_delivery_active: to get only the meters that are at an exact time active
        return_list: if the return parameter is a list or a dataframe

        Returns
        -------
        A list or dataframe of all the main meters
        """

        all_meters_df = self.get_list_all_meters(ts_delivery_active, return_list=False)
        search_pattern = re.compile(r'grid')  # we use regex to search for the meters with grid in their type
        main_meters_list = []
        for _, meter in all_meters_df.iterrows():
            if search_pattern.search(str(meter[bc_param.TYPE_METER]).lower()) is not None:
                main_meters_list.append(dict(meter))

        if return_list:
            return main_meters_list
        else:
            return pd.DataFrame(main_meters_list, columns=bc_param.info_meter_column_names)

    def get_list_all_meters(self, ts_delivery_active=None, return_list=False):
        """
        Function to retrieve all the main meters, and filter them out by time if needed
        Parameters
        ----------
        ts_delivery_active: int: if the meter was active at that time, optional parameter to filter out
        return_list: if the data to be returned is a dataframe (default) or a list

        Returns
        -------

        """
        meter_info_list = self.functions.get_id_meters().call()
        meter_info_df = pd.DataFrame(meter_info_list, columns=bc_param.info_meter_column_names)
        if ts_delivery_active is not None:
            final_meter_list = []
            for _, meter in meter_info_df.iterrows():
                if meter[bc_param.TS_DELIVERY_FIRST] <= ts_delivery_active <= meter[bc_param.TS_DELIVERY_LAST]:
                    final_meter_list.append(dict(meter))

            meter_info_list = final_meter_list
            meter_info_df = pd.DataFrame(final_meter_list, columns=bc_param.info_meter_column_names)
        if return_list:
            return meter_info_list
        else:
            return meter_info_df

    # Admins only

    def register_meter(self, df_meter):
        tx_hash = self.functions.push_id_meters(tuple(df_meter.values.tolist()[0])).transact({'from': self.coinbase})
        return tx_hash

    def delete_meter(self, df_meter):
        tx_hash = self.functions.delete_meter(tuple(df_meter.values.tolist()[0])).transact({'from': self.coinbase})
        return tx_hash

    def edit_meter(self, df_meter):
        """
        Function to edit a meter, for that, we assume that the id_meter will stay the same, and that only the other
        information was changed. The meter will be deleted and then added as the last one of the qeue
        Parameters
        ----------
        df_meter: dataframe of the new meter to push

        Returns
        -------

        """
        tx_hash = self.delete_meter(df_meter)
        self.wait_for_transact(tx_hash)
        return self.register_meter(df_meter)

    ### Meter readings functions
    def log_meter_readings_cumulative(self, df_readings_meter):

        return df_readings_meter

    #################################################
    # Functions the meter_delta_readings
    #################################################
    def push_meter_readings_delta(self, df_meter_delta):
        tx_hash = self.functions.push_meter_reading_delta(tuple(df_meter_delta.values)).transact({'from': self.coinbase})
        return tx_hash
    def get_meter_readings_delta(self):
        meter_readings=self.functions.get_meter_reading_delta.call()
    # Functions for mapping the meters to the users and so
    # not possible yet, needs the id market agent
    """def get_mapping_to_user(self):
        returns a mapping from id_meter and id_market_agent to each user
    """

    """def get_map_to_main_meter(self):

        info_meter = self.get_list_all_meters(returnList=True)
        info_users = self.get_list_all_users(returnList=True)

        map_grid_meter_to_self = dict([(i, a) for i, a in zip(info_meter["id_meter"], info_meter["id_meter"])])

        map_user_to_meter = dict([(i, a) for i, a in zip(info_meter["id_user"], info_meter["id_meter"])])

        info_user = self._query_data_free(f"SELECT {self.db_param.ID_USER}, {self.db_param.ID_MARKET_AGENT}"
                                          f" FROM {self.db_param.NAME_TABLE_INFO_USER}")

        map_ma_to_user = dict([(i, a) for i, a in zip(info_user["id_market_agent"], info_user["id_user"])])

        map_everything_to_main_meter = {}
        for id_ma in map_ma_to_user:
            map_everything_to_main_meter[id_ma] = map_user_to_meter.get(map_ma_to_user[id_ma], "0000000000")
        map_everything_to_main_meter = {**map_everything_to_main_meter, **map_grid_meter_to_self, **map_user_to_meter}
        return map_everything_to_main_meter"""

    ###################################################
    # Functions for the market bid submission table
    # Market participants only

    def get_open_positions(self, isOffer=True, returnBoth=False, temp=True, user_id="", returnList=False):
        """

        Parameters
        ----------
        isOffer:bool: wether to return an offer or a Bid
        returnBoth:bool: to return both the offers and the bids
        temp: if the temporal data or the permanent data are to be retrieved
        user_id: if an specific user_id positions are to be retrieved
        returnList: if the positions are to be retrieved as a List or Dataframe(default)

        Returns
        -------
        A list or Dataframe containing all the offers/bids
        """

        if isOffer:
            position_list = self.functions.getOffers(temp).call()
        else:
            position_list = self.functions.getBids(temp).call()

        if returnBoth:
            position_list = self.functions.getOffers(temp).call() + \
                            self.functions.getBids(temp).call()

        if returnList:
            return position_list
        position_df = pd.DataFrame(position_list, columns=bc_param.positions_market_ex_ante_column_names)
        if user_id != "":
            position_df = position_df[position_df["id_user"] == user_id]
            if position_df.empty:
                print("User has no open positions on the market.")
        return position_df

    def push_position(self, df_position, temp=True, permament=False):
        """
        Function that gets a pd.Series and registers it in the blockchain
        Parameters
        ----------
        df_position: pd.Series with all the info to register a postition
        temp: bool: wether is a temporal position or not
        permament: bool: wether is a permanent position or not

        Returns
        -------
        tx_hash: the transaction hash of the operation
        """
        if df_position["type_position"] == "offer":
            tx_hash = self.functions.pushOfferOrBid(tuple(df_position.values),
                                                    True, temp, permament).transact({'from': self.coinbase})
        elif df_position["type_position"] == "bid":
            tx_hash = self.functions.pushOfferOrBid(tuple(df_position.values),
                                                    False, temp, permament).transact({'from': self.coinbase})
        else:
            print("Position type is not valid")
            return
        return tx_hash

    def push_all_positions(self, df_positions, temp=True, permanent=False):
        for _, row in tqdm(df_positions.iterrows(), total=df_positions.shape[0]):
            tx_hash = self.push_position(row, temp=temp, permament=permanent)

        return tx_hash

    ###################################################
    # Functions for the market clearing of data
    # Market participants only
    # clears temporal data from the blockchain
    # temporal data are temp bids and offers and market results
    def clear_temp_data(self):
        try:
            tx_hash = self.functions.clearTempData().transact({'from': self.coinbase})
            self.wait_for_transact(tx_hash)
        except:
            # exceptions happens when the cost of deletion is too big. then we have to delete chunk by chunk
            limit_to_remove = 500
            while len(self.get_open_positions(isOffer=True, temp=True, returnList=True)) > 0 or \
                    len(self.get_open_positions(isOffer=False, temp=True, returnList=True)) > 0 or \
                    len(self.functions.getTempMarketResults().call()) > 0 or \
                    len(self.functions.getMarketResultsTotal().call()) > 0:
                try:
                    tx_hash = self.functions.clearTempData_gas_limit(limit_to_remove).transact({'from': self.coinbase})
                    self.wait_for_transact(tx_hash)
                except:
                    limit_to_remove -= 50

    # clears permanent data from the blockchain
    # permanent data are permanent bids and offers and all the info from the users and meters
    def clear_permanent_data(self):
        try:
            tx_hash = self.functions.clearPermanentData().transact({'from': self.coinbase})
            self.wait_for_transact(tx_hash)
        except:
            # exceptions happens when the cost of deletion is too big. then we have to delete chunk by chunk
            # 500 entries are to be removed, in the future, to be replaced by some gas estimation function
            limit_to_remove = 500
            while len(self.get_open_positions(isOffer=True, temp=False, returnList=True)) > 0 or \
                    len(self.get_open_positions(isOffer=False, temp=False, returnList=True)) > 0 or \
                    len(self.functions.get_user_infos().call()) or \
                    len(self.functions.get_id_meters().call()):
                try:
                    tx_hash = self.functions.clearPermanentData_gas_limit(limit_to_remove).transact(
                        {'from': self.coinbase})
                    self.wait_for_transact(tx_hash)
                except:
                    limit_to_remove -= 50

    ###################################################
    # Utility functions
    def wait_for_transact(self, tx_hash):
        tx_receipt = self.web3_eth.waitForTransactionReceipt(tx_hash)
        return tx_receipt

    # function that gets temporary/permanent offers or bids from the blockchain.

    # given the transaction hash, return the log of the function
    def get_log(self, tx_hash):
        tx_receipt = self.wait_for_transact(tx_hash)
        log_to_process = tx_receipt['logs'][0]
        processed_log = self.platform.events.logString().processLog(log_to_process)
        log = processed_log['args']['arg']
        return log


if __name__ == "__main__":
    block_dict = {"host": "localhost",
                  "port": "8540",
                  "timeout": 600,
                  "network_id": 8995,
                  "contract_name": "ClearingExAnte"}
    from lemlab.platform import lem

    bc_lem_conn = BlockchainConnection(block_dict)

    bc_lem_conn.clear_temp_data()
    bc_lem_conn.clear_permanent_data()
    assert bc_lem_conn.get_open_positions().empty and \
           bc_lem_conn.get_list_all_meters().empty and \
           bc_lem_conn.get_list_all_users().empty

    print("All the info is empty afer clearing")

    print(bc_lem_conn.get_info_user(user_id="9022DLBF"))

    N = 10
    info_users = lem.create_user_ids(N)
    info_meters = lem.create_user_ids(N)
    info_market_agents = lem.create_user_ids(N)
    for i in range(N):
        new_user = pd.DataFrame(
            data=[[info_users[i], 1000, 0, 10000, 100, 'green', 10, 'zi', 0, info_market_agents[i], 0, 0]],
            columns=bc_param.info_user_column_names)
        tx0 = bc_lem_conn.register_user(df_user=new_user)
        # bc_lem_conn.wait_for_transact(tx)

        new_meter_df = pd.DataFrame(
            data=[[info_meters[i], "3214KOPL", "0", "virtual grid meter", 'aggregator', 'green', 0, 0, 'test']],
            columns=bc_param.info_meter_column_names)
        tx = bc_lem_conn.register_meter(df_meter=new_meter_df)
        # bc_lem_conn.wait_for_transact(tx)

    bc_lem_conn.wait_for_transact(tx)
    # print("Meter:", bc_lem_conn.get_info_meter(meter_id="6543MZUG"))
    print("Users now:", len(bc_lem_conn.get_list_all_users(return_list=True)))
    tx_hash = bc_lem_conn.delete_user(new_user)
    bc_lem_conn.wait_for_transact(tx_hash)
    print("Users after", len(bc_lem_conn.get_list_all_users(return_list=True)))

    edited_user = pd.DataFrame(
        data=[[info_users[3], 30, 0, 1, 100, 'local', 10, 'zi', 0, info_market_agents[3], 2, 5]],
        columns=bc_param.info_user_column_names)
    tx_hash = bc_lem_conn.edit_user(edited_user)
    bc_lem_conn.wait_for_transact(tx_hash)

    all_meters_df = bc_lem_conn.get_list_main_meters()
    all_users_df = bc_lem_conn.get_list_all_users()

    print("All users", all_users_df.head(n=10))

    print("Main meters", all_meters_df.head(n=10))

    edited_meter = pd.DataFrame(
        data=[[info_meters[2], "3134ASDE", "0", "virtual grid meter", 'aggregator', 'local', 0, 0, 'no_test']],
        columns=bc_param.info_meter_column_names)
    tx_hash = bc_lem_conn.edit_meter(edited_meter)
    bc_lem_conn.wait_for_transact(tx_hash)

    print("Meters edited", bc_lem_conn.get_list_main_meters())

    new_position = pd.Series(data=["6533MZUG", 100, 10, 1, 10, "bid", 0, 0, round(time.time()), 123412],
                             index=bc_param.positions_market_ex_ante_column_names)
    tx = bc_lem_conn.push_position(df_position=new_position)
    bc_lem_conn.wait_for_transact(tx)

    print("Open positions", bc_lem_conn.get_open_positions())
