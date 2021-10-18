"""
Use this file to define the current yaml file to be used in all simulations and tests
All the files will import this file, so be careful with the file and the import
"""
import os
from pathlib import Path

scenario_file = "sim_test_config.yaml"

scenario_file_path = os.path.join(Path(__file__).parent, "code_examples", scenario_file)
