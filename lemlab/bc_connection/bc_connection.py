import lemlab.bc_connection.bc_param as bc_param
import time
import json
import os
from web3 import Web3, HTTPProvider
import pandas as pd
from pathlib import Path


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

    # Admins only

    def register_user(self, df_user):
        tx_hash = self.functions.push_user_info(tuple(df_user.values.tolist()[0])).transact({'from': self.coinbase})
        return tx_hash

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

    # Admins only

    def register_meter(self, df_meter):
        tx_hash = self.functions.push_id_meters(tuple(df_meter.values.tolist()[0])).transact({'from': self.coinbase})
        return tx_hash

    ###################################################
    # Functions for the market bid submission table
    # Market participants only

    def get_open_positions(self, bids_or_offers="bids", temp=True, user_id=""):
        if bids_or_offers == "bids":
            position_list = self.functions.getBids(temp)
        elif bids_or_offers == "offers":
            position_list = self.functions.getOffers(temp)
        else:
            position_list = self.functions.getBids(temp) + self.functions.getOffers(temp)
        position_df = pd.DataFrame(position_list, columns=bc_param.positions_market_ex_ante_column_names)
        if user_id != "":
            position_df = position_df[position_df["id_user"] == user_id]
            if position_df.empty:
                print("User has no open positions on the market.")
        return position_df

    def push_position(self, df_position, temp=True, permament=False):
        if df_position["type_position"].values == "offer":
            tx_hash = self.functions.pushOfferOrBid(tuple(df_position.values),
                                                    True, temp, permament).transact({'from': self.coinbase})
        elif df_position["type_position"].values == "bid":
            tx_hash = self.functions.pushOfferOrBid(tuple(df_position.values)[0],
                                                    False, temp, permament).transact({'from': self.coinbase})
        else:
            print("Position type is not valid")
            return
        return tx_hash


if __name__ == "__main__":
    block_dict = {"host": "localhost",
                  "port": "8540",
                  "timeout": 600,
                  "network_id": 8995,
                  "contract_name": "Platform"}

    bc_lem_conn = BlockchainConnection(block_dict)

    print(bc_lem_conn.get_info_user(user_id="7434RSBU"))

    new_user = pd.DataFrame(data=[["TESTUSER", 1000, 0, 10000, 100, 'green', 10, 'zi', 0, 0, 0]],
                            columns=bc_param.info_user_column_names)
    bc_lem_conn.register_user(df_user=new_user)

    print(bc_lem_conn.get_info_user(user_id="TESTUSER"))

    new_meter_df = pd.DataFrame(data=[["TESTMETER", "TESTUSER", 1, "0", 'aggregator', 'local', 0, 0, 'test']],
                                columns=bc_param.info_meter_column_names)
    bc_lem_conn.register_meter(df_meter=new_meter_df)

    print(bc_lem_conn.get_info_meter(meter_id="TESTMETER"))

    new_position = pd.DataFrame(data=[["TESTUSER", 100, 10, 1, 10, "bid", 0, 0, round(time.time()), 123412]],
                                columns=bc_param.positions_market_ex_ante_column_names)
    # bc_lem_conn.push_position(df_position=new_position)

    # print(bc_lem_conn.get_open_positions())

