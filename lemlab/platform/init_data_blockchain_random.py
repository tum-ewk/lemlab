import sys
import os
from lemlab.db_connection import db_connection, db_param
from lemlab.platform import lem
from pathlib import Path
import pandas as pd
import time
import yaml
# from tqdm import tqdm
from lemlab.bc_connection.bc_connection import BlockchainConnection

from current_scenario_file import scenario_file_path

project_dir = str(Path(__file__).parent.parent.parent)
sys.path.append(project_dir)


# the function had to be manually copied to avoid circular imports
def _convert_qualities_to_int(db_obj, positions, dict_types):
    dict_types_inverted = {v: k for k, v in dict_types.items()}
    positions[db_obj.db_param.QUALITY_ENERGY] = [dict_types_inverted[i] for i in
                                                 positions[db_obj.db_param.QUALITY_ENERGY]]
    return positions



