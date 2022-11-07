import pandas as pd
import numpy as np
import random
import time
import string


def insert_random_positions(db_obj, config, positions, n_positions, t_d_range, ids_user):
    positions.loc[:, db_obj.db_param.ID_USER] = random.choices(ids_user, k=n_positions)
    positions.loc[:, db_obj.db_param.T_SUBMISSION] = [round(time.time())] * n_positions
    positions.loc[:, db_obj.db_param.QTY_ENERGY] = random.choices(range(1, 1000, 1), k=n_positions)
    positions.loc[:, db_obj.db_param.TYPE_POSITION] = random.choices(list(config['lem']['types_position'].values()),
                                                                     k=n_positions)
    positions.loc[:, db_obj.db_param.QUALITY_ENERGY] = random.choices(list(config['lem']['types_quality'].values()),
                                                                      k=n_positions)
    positions.loc[:, db_obj.db_param.TS_DELIVERY] = random.choices(t_d_range, k=n_positions)
    positions.loc[:, db_obj.db_param.NUMBER_POSITION] = int(0)
    positions.loc[:, db_obj.db_param.STATUS_POSITION] = int(0)
    positions.loc[positions[db_obj.db_param.TYPE_POSITION] == 'offer', db_obj.db_param.PRICE_ENERGY] = [
        int(x * db_obj.db_param.EURO_TO_SIGMA / 1000)
        for x in random.choices(np.arange(config['retailer']['price_buy'],
                                          config['retailer']['price_sell'], 0.0001),
                                k=len(positions.loc[positions[db_obj.db_param.TYPE_POSITION] == 'offer', :]))]
    positions.loc[positions[db_obj.db_param.TYPE_POSITION] == 'offer',
                  db_obj.db_param.PREMIUM_PREFERENCE_QUALITY] = int(0)
    positions.loc[positions[db_obj.db_param.TYPE_POSITION] == 'bid', db_obj.db_param.PRICE_ENERGY] = [
        int(x * db_obj.db_param.EURO_TO_SIGMA / 1000)
        for x in random.choices(np.arange(config['retailer']['price_buy'],
                                          config['retailer']['price_sell'], 0.0001),
                                k=len(positions.loc[positions[db_obj.db_param.TYPE_POSITION] == 'bid', :]))]
    positions.loc[positions[db_obj.db_param.TYPE_POSITION] == 'bid',
                  db_obj.db_param.PREMIUM_PREFERENCE_QUALITY] = random.choices(range(0, 50, 1), k=len(
        positions.loc[positions[db_obj.db_param.TYPE_POSITION] == 'bid', :]))

    return positions


def create_random_positions(db_obj, config, ids_user, n_positions=None, verbose=False):
    if n_positions is None:
        n_positions = 100
    t_start = round(time.time()) - (
            round(time.time()) % config['lem']['interval_clearing']) + config['lem']['interval_clearing']
    t_end = t_start + config['lem']['horizon_clearing']
    # Range of time steps
    t_d_range = np.arange(t_start, t_end, config['lem']['interval_clearing'])
    # Create bid df
    positions = pd.DataFrame(columns=db_obj.get_table_columns(db_obj.db_param.NAME_TABLE_POSITIONS_MARKET_EX_ANTE))
    positions = insert_random_positions(db_obj, config, positions, n_positions, t_d_range, ids_user)

    # Drop duplicates
    positions = positions.drop_duplicates(
        subset=[db_obj.db_param.ID_USER, db_obj.db_param.NUMBER_POSITION, db_obj.db_param.TYPE_POSITION,
                db_obj.db_param.TS_DELIVERY])
    if verbose:
        print(pd.Timestamp.now(), 'Positions successfully written to DB')

    return positions


# Create random user ids
def create_user_ids(num=30):
    user_id_list = list()
    for i in range(num):
        # Create random user id in the form of 1234ABDS
        user_id_int = np.random.randint(1000, 10000)
        user_id_str = ''.join(random.sample(string.ascii_uppercase, 4))
        user_id_random = str(user_id_int) + user_id_str
        # Append user id to list
        user_id_list.append(user_id_random)

    return user_id_list
