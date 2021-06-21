import os
from web3 import Web3, HTTPProvider
import pandas as pd
from pathlib import Path

Platform_contract = None  # instance of the contract on the blockchain
coinbase = None  # address of the coinbase
functions = None  # functions of the contract
contract_address = None  # address of the contract
web3_instance = None  # instance of the connection to the blockchain


# given the transaction hash, this function returns the log of the function
def getLog(tx_hash):
    tx_receipt = web3_instance.eth.waitForTransactionReceipt(tx_hash)
    log_to_process = tx_receipt['logs'][0]
    processed_log = Platform_contract.events.logString().processLog(log_to_process)
    log = processed_log['args']['arg']
    return log


# converts list of lists to a dataframe, given the columns of the new dataframe
def convertListToPdDataFrame(_list_of_lists, cols):
    df = pd.DataFrame(_list_of_lists, columns=cols)
    return df


# set ups the blockchain, initializes the global variable
def setUpBlockchain(host="localhost", port="8540", contract_name="Platform", timeout=2000, network_id='8995',
                    project_dir=str(Path(__file__).parent.parent.parent)):
    global contract_address, Platform_contract, coinbase, functions, web3_instance
    try:
        web3_instance = Web3(
            HTTPProvider("http://" + host + ":" + port, request_kwargs={'timeout': timeout}))  # seconds
        # getting abi, bytecode, address via json file created by truffle
        json_path = os.path.join(project_dir, 'Truffle', 'build', 'contracts', contract_name + '.json')
        import json
        with open(json_path) as json_file:
            data = json.load(json_file)
        bytecode = data['bytecode']
        network_ids = list(data['networks'].keys())
        contract_address = data['networks'][network_id]['address']
        abi = json.dumps(data['abi'])
        # create contract instance with the coinbase and the contract function
        Platform_contract = web3_instance.eth.contract(address=contract_address, abi=abi, bytecode=bytecode)
        coinbase = web3_instance.eth.coinbase
        functions = Platform_contract.functions
    except Exception as e:
        print(e)
        assert False


# clears all temporary data on the blockchain
def clearTempData():
    try:
        tx_hash = functions.clearTempData().transact({'from': coinbase})
        web3_instance.eth.waitForTransactionReceipt(tx_hash)
    except:
        # exceptions happens when the cost of deletion is too big. then we have to delete chunk by chunk
        limit_to_remove = 500
        while len(getOffers_or_Bids(isOffer=True, temp=True)) > 0 or len(
                getOffers_or_Bids(isOffer=False, temp=True)) > 0 or len(
            functions.getTempMarketResults().call()) > 0 or len(functions.getMarketResultsTotal().call()) > 0:
            try:
                tx_hash = functions.clearTempData_gas_limit(limit_to_remove).transact({'from': coinbase})
                web3_instance.eth.waitForTransactionReceipt(tx_hash)
            except:
                limit_to_remove -= 50


# clears all permanent data on the blockchain
def clearPermanentData():
    try:
        tx_hash = functions.clearPermanentData().transact({'from': coinbase})
        web3_instance.eth.waitForTransactionReceipt(tx_hash)
    except:
        # exceptions happens when the cost of deletion is too big. then we have to delete chunk by chunk
        limit_to_remove = 500
        while len(getOffers_or_Bids(isOffer=True, temp=False)) > 0 or len(
                getOffers_or_Bids(isOffer=False, temp=False)) > 0 or len(functions.get_user_infos().call()) or len(
            functions.get_id_meters().call()):
            try:
                tx_hash = functions.clearPermanentData_gas_limit(limit_to_remove).transact({'from': coinbase})
                web3_instance.eth.waitForTransactionReceipt(tx_hash)
            except:
                limit_to_remove -= 50


# function that gets temporary/permanent offers or bids from the blockchain.
def getOffers_or_Bids(isOffer=True, temp=True):
    if isOffer:
        return functions.getOffers(temp).call()
    else:
        return functions.getBids(temp).call()
