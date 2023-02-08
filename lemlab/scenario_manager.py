__author__ = "sdlumpp"
__credits__ = ["TUM-Doepfert"]
__license__ = ""
__maintainer__ = "TUM-Doepfert"
__email__ = "markus.doepfert@tum.de"

import time
import shutil
import os
import string
import json
from random import shuffle, choice, random, randint, choices
import numpy as np
from ruamel.yaml import YAML
from typing import Tuple, Any, Union
import pandas as pd
import feather as ft
import math
from scipy import optimize


class Scenario:
    """
    A class used to generate or edit a scenario.
    ...

    Attributes
    ----------
    scenario_name : str
        name of the scenario
    path_scenario : str
        path to the scenario
    path_input_data : str
        path input data is stored
    config : dict
        config file that contains all required settings to set up a new or edit an existing scenario
    yaml : function
        allows reading and writing yaml files without changing their original format

    Public Methods
    -------
    __init__() -> None
        initializer, requires no params
    new_scenario(path_specification: str = None, scenario_name: str = None) -> None
        creates a new scenario based on the provided config file, takes the path of config file and the name of the
        new scenario as params
    load_scenario(path_scenario: str) -> None
        loads an existing scenario, takes path of scenario directory as param
    edit_scenario(path_new_config: str, name_new_scenario: str) -> None
        edits an existing scenario based on a changed config file, takes the path of the edited config file and the
        name of the new scenario as params
    delete_prosumer(id_prosumer: str) -> None
        deletes a prosumer, takes the id of the prosumer that is to be deleted
    """

    def __init__(self):
        """initializer

        Args:

        Returns:

        """

        # initialize name of the scenario
        self.scenario_name = None
        # initialize paths of the scenario and the input data
        self.path_scenario = None
        self.path_input_data = None
        # initialize config file
        self.config = None
        # initialize yaml functionality
        self.yaml = YAML()

    def new_scenario(self, path_specification: str = None, scenario_name: str = None) -> None:
        """creates a new scenario based on the specified config file under the given scenario name

        Args:
            path_specification: path of config file in relation to main directory
            scenario_name: name under which the new scenario is stored

        Returns:
            None

        Structure:
            __general_setup()
                get all paths and folders set up for the scenario
            __create_lem() (optional)
                set up local energy market platform
            __create_retailer() (optional)
                create retailer that balances the system
            __create_prosumers()
                creates the initial setup for the creation of prosumers and hands it over to __create_prosumer()
                to create the individual prosumers
            __create_aggregator() (optional)
                creates aggregator for prosumers
        """
        print("*** Creating new scenario... ***")

        self.__general_setup(path_specification, scenario_name)

        # Only execute when LEM is active
        if self.config["simulation"]["lem_active"]:
            self.__create_lem()
            self.__create_retailer()

        if self.config["simulation"]["agents_active"]:
            self.__create_prosumers()
            if self.config["aggregator"]["active"]:
                self.__create_aggregator()

        print(f"*** {scenario_name} successfully generated. ***\n")

    def load_scenario(self, path_scenario: str) -> None:
        """loads an existing scenario that is in the path_scenario directory

        Args:
            path_scenario: path of the scenario

        Returns:
            None

        """

        with open(f"{path_scenario}/config.yaml") as config_file:
            self.config = self.yaml.load(config_file)
        self.scenario_name = path_scenario.split("/")[-1]
        self.path_input_data = self.config["simulation"]["path_input_data"]
        self.path_scenario = path_scenario

    def edit_scenario(self, path_new_config: str, name_new_scenario: str) -> None:
        """creates a new scenario based on an existing using the edited config file

        Args:
            path_new_config: path of the edited config file (needs to be in the existing scenario the new scenario
                             will be based on)
            name_new_scenario: name of the new scenario that will be created

        Returns:
            None

        """
        print(f"*** Creating edited scenario '{name_new_scenario}'... ***")

        # General setup of new scenario
        self.__general_setup(path_specification=path_new_config, scenario_name=name_new_scenario)

        # Load old config files
        base_path_old = path_new_config.rpartition("/")[0]
        with open(f"{base_path_old}/config.yaml") as config_file:
            config_old = self.yaml.load(config_file)

        # Lists that results in different actions for the categories
        list_no_action = ["simulation", "db_connections"]  # categories that require no action
        list_new = ["lem", "retailer", "aggregator"]  # categories that require new files
        list_individual = ["prosumer", "producer"]  # categories that require individual edits

        # Loop through the old config file and compare it to the new one to find the differences
        print("Changed settings:")
        for category in self.config:
            if self.config[category] != config_old[category]:
                self.__print_changed_settings(category, config_old)
                if category in list_no_action:
                    pass  # Do nothing
                elif category in list_new:
                    self.__edit_new(category)
                elif category in list_individual:
                    self.__edit_individual(category, config_old, base_path_old)
                else:
                    raise Warning(f"Category '{category}' is not listed. Please ensure that config file is in "
                                  f"the standard format.")
            else:
                if category in list_new + ["prosumer"]:
                    self.__copy_folder(category, base_path_old)
                    # pass

    def delete_prosumer(self, id_prosumer: str) -> None:
        """deletes the prosumer with the provided id_prosumer from the scenario

        Args:
            id_prosumer: ID of the prosumer

        Returns:
            None

        """

        # Delete the prosumer's directory
        shutil.rmtree(path=f"{self.path_scenario}/prosumer/{id_prosumer}", ignore_errors=True)

        # Delete the prosumer from the aggregator's list (if applicable)
        if self.config["aggregator"]["active"]:
            self.__deaggregate_prosumers(list_prosumers=[id_prosumer])

    def __general_setup(self, path_specification: str = None, scenario_name: str = None) -> None:
        """creates the necessary paths and folders to store all files of the scenario

        Args:
            path_specification: path of the config file for the scenario
            scenario_name: name of the scenario

        Returns:
            None

        """

        # Set paths
        path_specification = "./config.yaml" if path_specification is None else path_specification
        self.scenario_name = scenario_name if scenario_name is not None else "new_scenario_1"

        # Load configuration file
        with open(f"{path_specification}") as config_file:
            self.config = self.yaml.load(config_file)

        # Set scenario and input paths
        self.path_scenario = f"{self.config['simulation']['path_scenarios']}/{self.scenario_name}"
        self.path_input_data = f"{self.config['simulation']['path_input_data']}"

        # Create scenario directory tree (if scenario exists, delete and recreate)
        self.__create_folders([(f"{self.config['simulation']['path_scenarios']}", True),
                               (f"{self.path_scenario}", True),
                               (f"{self.path_scenario}/lem", True),
                               (f"{self.path_scenario}/retailer", True),
                               (f"{self.path_scenario}/prosumer", True),
                               (f"{self.path_scenario}/aggregator", True),
                               (f"{self.path_scenario}/weather", True)])

        # Copy config file to scenario directory
        with open(f"{self.path_scenario}/config.yaml", 'w') as file:
            self.yaml.dump(self.config, file)

        # Copy weather file as feather files to scenario directory
        df_weather = pd.read_csv(f"{self.path_input_data}/weather/weather.csv")
        df_weather.to_feather(f"{self.path_scenario}/weather/weather.ft")

    def __create_folders(self, list_paths: list) -> None:
        """creates new or replaces existing folders specified in the list of paths

        Args:
            list_paths: list of tuples that contains the respective path of the folder as well as a flag that, if set
                        to true, causes all files within the folder to be deleted

        Returns:
            None

        """

        # Loop through all paths and create the folders
        for path, delete_flag in list_paths:
            if os.path.isdir(path):
                try:
                    # Delete folder contents if true
                    if delete_flag:
                        self.__del_file_contents(path)
                except FileNotFoundError:
                    pass
            else:
                os.mkdir(path)
        time.sleep(0.1)

    def __create_lem(self) -> None:
        """creates the files for the lem platform

        Args:

        Returns:
            None

        """

        # Check if platform needs to be set up
        if not self.config["simulation"]["lem_active"]:
            return

        # Read balancing price time series from input data directory if set to "file"
        if self.config["lem"]["bal_energy_pricing_mechanism"] == "file":
            try:
                df_balancing_prices = pd.read_csv(f"{self.path_input_data}/lem/balancing_prices/"
                                                  f"{self.config['lem']['path_bal_prices']}").set_index("timestamp")
            except FileNotFoundError:
                raise FileNotFoundError("Balancing price file does not exist. Exiting setup.")

            # Save balancing price time series to lem specification directory
            ft.write_dataframe(df_balancing_prices.reset_index(),
                               f"{self.path_scenario}/lem/balancing_prices.ft")

        # Read levy price time series from input data directory if set to "file"
        if self.config["lem"]["levy_pricing_mechanism"] == "file":
            try:
                df_levy_prices = pd.read_csv(f"{self.path_input_data}/lem/levy_prices/"
                                             f"{self.config['lem']['path_levy_prices']}").set_index("timestamp")
            except FileNotFoundError:
                raise FileNotFoundError("Levy price file does not exist. Exiting setup.")

            # Save levy price time series to lem specification directory
            ft.write_dataframe(df_levy_prices.reset_index(),
                               f"{self.path_scenario}/lem/levy_prices.ft")

        for type_pricing in self.config["lem"]["types_pricing_ex_post"]:
            str_type_pricing = self.config["lem"]["types_pricing_ex_post"][type_pricing]
            try:
                shutil.copyfile(f"{self.path_input_data}/lem/ex_post_pricing/{str_type_pricing}.json",
                                f"{self.path_scenario}/lem/{str_type_pricing}.json")
            except FileNotFoundError:
                raise FileNotFoundError(f"Ex-post pricing file {str_type_pricing}.json does note exist. Exiting setup.")

        # Save config file to platform specification directory
        with open(f"{self.path_scenario}/lem/config_account.json", "w+") as write_file:
            json.dump(self.config["lem"], write_file)

    def __create_retailer(self) -> None:
        """creates the files for the retailer

        Args:

        Returns:
            None

        """

        # Check if file needs to be created
        if not self.config["simulation"]["lem_active"]:  # exit function if lem is inactive
            return
        self.config["retailer"]["id_market_agent"] = self.config["retailer"]["id_user"]
        # Save config file to retailer specification directory
        with open(f"{self.path_scenario}/retailer/config_account.json", "w+") as write_file:
            json.dump(self.config["retailer"], write_file)

    def __create_prosumers(self) -> None:
        """creates the general setup for the prosumers to pass the information on to create the individual prosumers

        Args:

        Returns:
            None

        """

        # Check if prosumers need to be created
        if not self.config["simulation"]["agents_active"]:  # exit function if agents are inactive
            return

        # Gather all information about the prosumers household consumption and their devices (e.g. PV or batteries)
        distr_hh, prosumers_devices = self.__create_prosumers_hh_devices()

        # Configure standard settings for all prosumers
        prosumer_info = {
            "mpc_horizon": self.config["prosumer"]["mpc_horizon"],
            "mpc_price_fcast": self.config["prosumer"]["mpc_price_fcast"],
            "mpc_price_fcast_retraining_period": self.config["prosumer"]["mpc_price_fcast_retraining_period"],
            "mpc_price_fcast_update_period": self.config["prosumer"]["mpc_price_fcast_update_period"],
            "solver": self.config["prosumer"]["general_solver"],
            "meter_prob_late": self.config["prosumer"]["meter_prob_late"],
            "meter_prob_late_95": self.config["prosumer"]["meter_prob_late_95"],
            "meter_prob_missing": self.config["prosumer"]["meter_prob_missing"],
        }

        # Configure individual settings for one prosumer at a time and hand them over to __create_prosumer()
        for idx in range(self.config["prosumer"]["general_number_of"]):
            list_plant_specs = []
            # Specify household loads
            self.__gen_hh(list_plant_specs, distr_hh)

            # Configure electrical devices if present
            for key in prosumers_devices:
                if prosumers_devices[key][idx]:
                    for _ in range(prosumers_devices[key][idx]):
                        self.__gen_plants(key=key, list_plant_specs=list_plant_specs)

            # Update individual prosumer info in dict
            prosumer_info.update({
                "list_plant_specs": list_plant_specs,
                "controller_strategy": self.config["prosumer"]["controller_strategy"],
                "ma_horizon": choice(self.config["prosumer"]["ma_horizon"]),
                "ma_strategy": choice(self.config["prosumer"]["ma_strategy"]),
                "ma_bid_max": self.config["retailer"]["price_sell"],
                "ma_offer_min": self.config["retailer"]["price_buy"],
                "ma_preference_quality": choice(self.config["prosumer"]["ma_preference_quality"]),
                "ma_premium_preference_quality": choice(self.config["prosumer"]["ma_premium_preference_quality"]),
            })

            # Create individual prosumer
            self.__create_prosumer(account=prosumer_info)

        # Check if large-scale producers are to be included and create them as prosumers without consumption
        producers_plants = ["pv", "wind", "fixedgen"]
        for plant in producers_plants:
            if self.config["producer"][f"{plant}_active"]:
                list_plant_specs = []
                self.__gen_plants(key=plant, list_plant_specs=list_plant_specs, participant_type="producer")

                # Update individual prosumer info in dict
                prosumer_info.update({
                    "list_plant_specs": list_plant_specs,
                    "ma_horizon": choice(self.config["prosumer"]["ma_horizon"]),
                    "ma_strategy": choice(self.config["prosumer"]["ma_strategy"]),
                    "ma_bid_max": self.config["retailer"]["price_sell"],
                    "ma_offer_min": self.config["retailer"]["price_buy"],
                    "ma_preference_quality": choice(self.config["prosumer"]["ma_preference_quality"]),
                    "ma_premium_preference_quality": choice(self.config["prosumer"]["ma_premium_preference_quality"]),
                })

                # Create individual prosumer (in this case a producer --> no consumption)
                self.__create_prosumer(account=prosumer_info)

    def __create_prosumers_hh_devices(self) -> Tuple[list, dict]:
        """creates the configurations for the household demand as well as all types of devices

        Comment: The configuration of the household demands is only required when the household are sized using a
                 distribution.
                 Currently, each household cannot have more than one of each type of device.

        Args:

        Returns:
            distr_hh: list that contains the number of household, which lie in between a specific demand intervall
            prosumers_devices: dict that contains a list for each type of device, which specifies if a household has (1)
            or does not (0) have a specific type of device.

        """

        # Generate a list where each element represents the number of prosumers with the total consumption specified in
        #   self.config["prosumer"]["hh_load_intervals"] if "hh_load_type" is set to distribution. If total consumption
        #   is specified as uniform across all households, no action is taken
        if self.config["prosumer"]["hh_sizing"] == "distribution":
            distr_hh = self.__gen_distr_list(num=self.config["prosumer"]["general_number_of"],
                                             distr=self.config["prosumer"]["hh_sizing_distribution"])
        else:
            distr_hh = None

        # Setup dict that contains information about all prosumers' electrical devices
        prosumers_devices = {"pv": [0] * self.config["prosumer"]["general_number_of"],  # PV
                             "bat": [0] * self.config["prosumer"]["general_number_of"],  # Battery
                             "ev": [0] * self.config["prosumer"]["general_number_of"],  # Electric vehicle
                             "hp": [0] * self.config["prosumer"]["general_number_of"],  # Heat pump
                             "chp": [0] * self.config["prosumer"]["general_number_of"],  # CHP
                             "wind": [0] * self.config["prosumer"]["general_number_of"],  # Wind turbine
                             "fixedgen": [0] * self.config["prosumer"]["general_number_of"]}  # Fixed generation

        # Create households based on the specifications in scenario_config for each device type
        # Photovoltaics
        prosumers_devices["pv"] = self.__gen_rand_bool_list(length=self.config["prosumer"]["general_number_of"],
                                                            share_1s=self.config["prosumer"]["pv_fraction"])

        # Batteries (either dependent or independent of PV systems)
        if self.config["prosumer"]["bat_fraction_dependent_on_pv"]:
            prosumers_devices["bat"] = self.__gen_dep_bool_list(list_bool=prosumers_devices["pv"],
                                                                share_1s=self.config["prosumer"]["bat_fraction"])
        else:
            prosumers_devices["bat"] = self.__gen_rand_bool_list(length=self.config["prosumer"]["general_number_of"],
                                                                 share_1s=self.config["prosumer"]["bat_fraction"])

        # Electric vehicles
        prosumers_devices["ev"] = self.__gen_rand_bool_list(length=self.config["prosumer"]["general_number_of"],
                                                            share_1s=self.config["prosumer"]["ev_fraction"])

        # Heat pumps
        prosumers_devices["hp"] = self.__gen_rand_bool_list(length=self.config["prosumer"]["general_number_of"],
                                                            share_1s=self.config["prosumer"]["hp_fraction"])

        # CHP
        prosumers_devices["chp"] = self.__gen_rand_bool_list(length=self.config["prosumer"]["general_number_of"],
                                                             share_1s=self.config["prosumer"]["chp_fraction"])

        # Wind turbines
        prosumers_devices["wind"] = self.__gen_rand_bool_list(length=self.config["prosumer"]["general_number_of"],
                                                              share_1s=self.config["prosumer"]["wind_fraction"])

        # Fixed generation
        prosumers_devices["fixedgen"] = self.__gen_rand_bool_list(length=self.config["prosumer"]["general_number_of"],
                                                                  share_1s=self.config["prosumer"]["fixedgen_fraction"])

        return distr_hh, prosumers_devices

    def __gen_hh(self, list_plant_specs: list, distr_hh: list = None) -> None:
        """generates the specifications the household and appends them to the list with all plant specifications

        Args:
            list_plant_specs: list that contains a dictionary for each plant type with the required information
            distr_hh: list that contains the number of households that lie within a specific consumption interval.
                      Note: Only used when "hh_sizing" in the config file is set to "distribution", otherwise it is
                            set to None

        Returns:
            None

        """

        # Dict containing all specifications of the household
        dict_hh = {"type": "hh",
                   "activated": True,
                   "has_submeter": self.config["prosumer"]["hh_has_submeter"],
                   "fcast": self.config["prosumer"]["hh_fcast"],
                   "fcast_order": [],
                   "fcast_param": [],
                   "fcast_retraining_period": self.config["prosumer"]["hh_fcast_retraining_period"],
                   "fcast_update_period": self.config["prosumer"]["hh_fcast_update_period"],
                   }

        # Change the forecast parameters based on the method
        if dict_hh["fcast"] == "sarma":
            dict_hh["fcast_order"] = self.config["prosumer"]["hh_fcast_sarma_order"]
            num_param = sum(dict_hh["fcast_order"]) - dict_hh["fcast_order"][6] - dict_hh["fcast_order"][10]
            dict_hh["fcast_param"] = [1 / num_param] * num_param
        elif dict_hh["fcast"] == "smoothed":
            dict_hh["fcast_param"] = 9

        # Add the corresponding preliminary consumption to dict that is used for the dimensioning of pv, bat and hp
        if self.config["prosumer"]["hh_sizing"] == "uniform":
            # Set the annual consumption to the defined value in the config file
            dict_hh["annual_consumption"] = self.config["prosumer"]["hh_sizing_uniform"]
        elif self.config["prosumer"]["hh_sizing"] == "distribution":
            # Set the annual consumption to one of the values specified in distr_hh
            # Creates the interval from which the average values of neighboring elements is chosen. The lower and upper
            # bound are added to the beginning and the end since these values do not have to be specifically written in
            # the config file and therefore need to be set here
            interval = [0] + self.config["prosumer"]["hh_sizing_distribution_intervals"] + \
                       [self.config["prosumer"]["hh_sizing_distribution_intervals"][-1]]
            # Choose a random consumption range that is still available in distr_hh
            idx = choice([idx for idx, elem in enumerate(distr_hh) if elem != 0])
            # Update distr_hh to keep track of which household consumptions are still to be created
            distr_hh[idx] -= 1
            # Preliminary consumption is the average of the lower and upper bound of the chosen range
            dict_hh["annual_consumption"] = (interval[idx + 1] + interval[idx]) / 2
        else:
            raise Warning(f"The chosen parameter ({self.config['prosumer']['hh_sizing']}) for 'hh_sizing' is not "
                          f"valid. Please select one of the options listed in the config file.")

        list_plant_specs.append(dict_hh)

    def __gen_plants(self, key: str, list_plant_specs: list, participant_type: str = "prosumer") -> None:
        """calls the respective function based on the provided key to generate the plant's specifications

        Args:
            key: string that specifies the type of plant that is to be created
            list_plant_specs: list that contains a dictionary for each plant type with the required specifications

        Returns:
            None

        """

        if key == "pv":
            self.__gen_pv(list_plant_specs, participant_type)
        elif key == "bat":
            self.__gen_bat(list_plant_specs)
        elif key == "ev":
            self.__gen_ev(list_plant_specs)
        elif key == "hp":
            self.__gen_hp(list_plant_specs)
        elif key == "chp":
            self.__gen_chp(list_plant_specs)
        elif key == "wind":
            self.__gen_wind(list_plant_specs, participant_type)
        elif key == "fixedgen":
            self.__gen_fixedgen(list_plant_specs, participant_type)
        else:
            raise Warning(f"Key {key} has no no associated function.")

    def __gen_pv(self, list_plant_specs: list, participant_type: str) -> None:
        """generates the PV specifications according to the config file and the household parameters and appends them to
        the list with all plant specifications

        Args:
            list_plant_specs: list that contains a dictionary for each plant type with the required information
            participant_type: string that specifies the type of participant (e.g. producer or prosumer)

        Returns:
            None

        """

        # Dict containing all specifications of the PV system
        dict_pv = {"type": "pv",
                   "activated": True,
                   "has_submeter": True,
                   }

        # Check for the type of participant
        if participant_type == "prosumer":
            # Calculate PV power considering the randomization factor pv_size_deviation and update dict_pv
            hh_consumption = next(item["annual_consumption"] for item in list_plant_specs if item["type"] == "hh")
            pv_power = hh_consumption * self.config["prosumer"]["pv_sizing_power"]
            pv_power = round(pv_power * (1 - self.config["prosumer"]["pv_sizing_power_deviation"] +
                                         2 * self.config["prosumer"]["pv_sizing_power_deviation"] * random()), 1)
        elif participant_type == "producer":
            # Read peak power from config file
            pv_power = self.config[participant_type]["pv_power"]
            # Set information about submeters to False as only one meter exists
            dict_pv["has_submeter"] = False
        else:
            raise Warning(f"The value of participant_type '{participant_type}' does not exist.")

        # Update the dict and append the PV specifications to the list of plant specs
        dict_pv.update({"power": round(pv_power),
                        "controllable": self.config[participant_type]["pv_controllable"],
                        "fcast": self.config[participant_type]["pv_fcast"],
                        "fcast_order": [],
                        "fcast_param": 9,
                        "fcast_retraining_period": self.config["prosumer"]["pv_fcast_retraining_period"],
                        "fcast_update_period": self.config["prosumer"]["pv_fcast_update_period"],
                        "quality": self.config[participant_type]["pv_quality"],
                        })

        list_plant_specs.append(dict_pv)

    def __gen_bat(self, list_plant_specs: list) -> None:
        """generates the battery specifications according to the config file and the PV/household specifications and
        appends them to the list with all plant specifications

        Args:
            list_plant_specs: list that contains a dictionary for each plant type with the required information

        Returns:
            None

        """

        # Dict containing all specifications of the battery
        dict_bat = {"type": "bat",
                    "activated": True,
                    "has_submeter": True,
                    }

        # Check if prosumer has PV system otherwise size according to household consumption
        pv_power = next((item["power"] for item in list_plant_specs if item["type"] == "pv"), None)
        if pv_power:
            dict_bat["power"] = round(self.config["prosumer"]["bat_sizing_power"] * pv_power)
        else:
            hh_consumption = next(item["annual_consumption"] for item in list_plant_specs if item["type"] == "hh")
            dict_bat["power"] = round(self.config["prosumer"]["bat_sizing_power"] * hh_consumption)

        # Update the dict and append the battery specifications to the list of plant specs
        dict_bat.update({"capacity": round(dict_bat["power"] * self.config["prosumer"]["bat_sizing_capacity"]),
                         "efficiency": self.config["prosumer"]["bat_efficiency"],
                         "charge_from_grid": self.config["prosumer"]["bat_charge_from_grid"],
                         "quality": self.config["prosumer"]["bat_quality"],
                         })

        list_plant_specs.append(dict_bat)

    def __gen_ev(self, list_plant_specs: list) -> None:
        """generates the electrice vehicle's specifications according to the config file and appends them to the list
        with all plant specifications

        Args:
            list_plant_specs: list that contains a dictionary for each plant type with the required information

        Returns:
            None

        """

        # Generate dict with all specifications and append to the list
        dict_ev = {"type": "ev",
                   "activated": True,
                   "has_submeter": True,
                   "efficiency": self.config["prosumer"]["ev_efficiency"],
                   "v2g": self.config["prosumer"]["ev_v2g"],
                   "charging_power": choice(self.config["prosumer"]["ev_charging_power"]),
                   "capacity": choice(self.config["prosumer"]["ev_capacity"]),
                   "consumption": choice(self.config["prosumer"]["ev_consumption"]),
                   "fcast": self.config["prosumer"]["ev_fcast"],
                   "fcast_order": [],
                   "fcast_param": [],
                   "fcast_retraining_period": self.config["prosumer"]["ev_fcast_retraining_period"],
                   "fcast_update_period": self.config["prosumer"]["ev_fcast_update_period"],
                   "quality": self.config["prosumer"]["ev_quality"],
                   }

        list_plant_specs.append(dict_ev)

    def __gen_hp(self, list_plant_specs: list) -> None:
        """generates the heat pump's specifications according to the config file and appends them to the list
        with all plant specifications

        Args:
            list_plant_specs: list that contains a dictionary for each plant type with the required information

        Returns:
            None

        """

        # Generate dict with all specifications and append to the list
        dict_hp = {"type": "hp",
                   "activated": True,
                   "has_submeter": True,
                   "power_th": self.config["prosumer"]["hp_sizing_power"],
                   "hp_type": choice(self.config["prosumer"]["hp_type"]),
                   "temperature": self.config["prosumer"]["hp_temperature"],
                   "capacity": choice(self.config["prosumer"]["hp_capacity"]),
                   "efficiency": self.config["prosumer"]["hp_tes_efficiency"],
                   "fcast": self.config["prosumer"]["hp_fcast"],
                   "fcast_order": [],
                   "fcast_param": [],
                   "fcast_retraining_period": self.config["prosumer"]["hp_fcast_retraining_period"],
                   "fcast_update_period": self.config["prosumer"]["hp_fcast_update_period"],
                   }

        list_plant_specs.append(dict_hp)

    def __gen_chp(self, list_plant_specs: list) -> None:
        """generates the chp's specifications according to the config file and appends them to the list
        with all plant specifications

        Args:
            list_plant_specs: list that contains a dictionary for each plant type with the required information

        Returns:
            None

        """
        # Generate dict with all specifications and append to the list
        dict_chp = {"type": "chp",
                    "activated": True,
                    "has_submeter": True,
                    "power_th": self.config["prosumer"]["chp_sizing_power"],
                    "heat_elec_ratio": self.config["prosumer"]["chp_heat_elec_ratio"],
                    "efficiency": self.config["prosumer"]["chp_efficiency"],
                    "capacity": choice(self.config["prosumer"]["chp_capacity"]),
                    "tes_efficiency": self.config["prosumer"]["chp_tes_efficiency"],
                    "fcast": self.config["prosumer"]["chp_fcast"],
                    "fcast_order": [],
                    "fcast_param": [],
                    "fcast_retraining_period": self.config["prosumer"]["chp_fcast_retraining_period"],
                    "fcast_update_period": self.config["prosumer"]["chp_fcast_update_period"],
                    }

        list_plant_specs.append(dict_chp)

    def __gen_wind(self, list_plant_specs: list, participant_type: str) -> None:
        """generates the wind specifications according to the config file and the household parameters and appends them
        to the list with all plant specifications

        Args:
            list_plant_specs: list that contains a dictionary for each plant type with the required information
            participant_type: string that specifies the type of participant (e.g. producer or prosumer)

        Returns:
            None

        """

        # Dict containing all specifications of the wind power system
        dict_wind = {"type": "wind",
                     "activated": True,
                     "has_submeter": True,
                     }

        # Check for the type of participant
        if participant_type == "prosumer":
            # Calculate wind power considering the randomization factor wind_sizing_power_deviation
            hh_consumption = next(item["annual_consumption"] for item in list_plant_specs if item["type"] == "hh")
            wind_power = hh_consumption * self.config["prosumer"]["wind_sizing_power"]
            wind_power = round(wind_power * (1 - self.config["prosumer"]["wind_sizing_power_deviation"] +
                                             2 * self.config["prosumer"]["wind_sizing_power_deviation"] * random()), 1)
        elif participant_type == "producer":
            # Read power rating form config file
            wind_power = self.config[participant_type]["wind_power"]
            # Set information about submeters to False as only one meter exists
            dict_wind["has_submeter"] = False
        else:
            raise Warning(f"The value of participant_type '{participant_type}' does not exist.")

        # Update the dict and append the wind specifications to the list of plant specs
        dict_wind.update({"power": wind_power,
                          "controllable": self.config[participant_type]["wind_controllable"],
                          "fcast": self.config[participant_type]["wind_fcast"],
                          "fcast_order": [],
                          "fcast_param": 9,
                          "fcast_retraining_period": self.config["prosumer"]["wind_fcast_retraining_period"],
                          "fcast_update_period": self.config["prosumer"]["wind_fcast_update_period"],
                          "quality": self.config[participant_type]["wind_quality"],
                          })

        list_plant_specs.append(dict_wind)

    def __gen_fixedgen(self, list_plant_specs: list, participant_type: str) -> None:
        """generates the fixed generation's specifications according to the config file and appends them to the list
        with all plant specifications

        Args:
            list_plant_specs: list that contains a dictionary for each plant type with the required information
            participant_type: string that specifies the type of participant (e.g. producer or prosumer)

        Returns:
            None

        """

        # Dict containing all specifications of the wind power system
        dict_fixedgen = {"type": "fixedgen",
                         "activated": True,
                         "has_submeter": True,
                         }

        # If participant type is produce always set "has_submeter" to False
        if participant_type == "producer":
            dict_fixedgen["has_submeter"] = False

        # Update the dict and append the fixedgen specifications to the list of plant specs
        dict_fixedgen.update({"power": self.config[participant_type]["fixedgen_power"],
                              "controllable": self.config[participant_type]["fixedgen_controllable"],
                              "fcast": "perfect",
                              "fcast_order": [],
                              "fcast_param": [],
                              "quality": self.config[participant_type]["fixedgen_quality"],
                              })

        # Generate dict with all specifications and append to the list
        list_plant_specs.append(dict_fixedgen)

    def __create_prosumer(self, account: dict) -> None:
        """creates the directory and files for the individual prosumer

        Args:
            account: dict that contains all information about the respective prosumer that is required to generate them

        Returns:
            None

        """

        # Generate list of random plant IDs, which are assigned to the prosumer's various plants (incl. the household)
        id_len = 10
        list_plant_ids = [self.__gen_rand_id(id_len) for _ in range(len(account["list_plant_specs"]))]

        # Create user ID based on the number of folders in the destination directory "prosumer"
        num_prosumers = len(os.listdir(f"{self.path_scenario}/prosumer/"))
        id_user = str(num_prosumers + 1).zfill(id_len)

        # Add new information to account dict
        account.update({"id_user": id_user,
                        "id_meter_grid": self.__gen_rand_id(id_len),
                        "list_plants": list_plant_ids,
                        "id_market_agent": id_user})

        # Create folder for prosumer based on ID
        os.mkdir(f"{self.path_scenario}/prosumer/{account['id_user']}")

        # Generate all files for the prosumer's devices and register meters in DB
        plant_dict = {}
        for i, plant in enumerate(account["list_plant_specs"]):
            plant_dict[list_plant_ids[i]] = plant

        # Initialize prosumer's main meter
        self.__init_meter(id_user=account['id_user'], id_meter=account.get('id_meter_grid'),
                          init_reading_positive=randint(1, 10000), init_reading_negative=randint(1, 10000))
        final_plant_dict = plant_dict
        # Initialize all other meters and create device-dependent files
        for plant_id in list_plant_ids:
            self.__init_meter(id_user=account['id_user'], id_meter=plant_id,
                              init_reading_positive=randint(1, 10000), init_reading_negative=randint(1, 10000))

            # Check which types are present and create the corresponding files
            final_plant_dict[plant_id] = self.__create_plant_files(plant_type=plant_dict[plant_id].get("type"),
                                                                   account=account, plant_id=plant_id,
                                                                   plant_dict=plant_dict)

        # Write final config files to the directory
        # Contains the general account information
        account.pop("list_plant_specs", None)  # deleted as information is found in plant_dict/config_plants.json
        with open(f"{self.path_scenario}/prosumer/{account['id_user']}/config_account.json", "w") \
                as write_file:
            json.dump(account, write_file)

        # Contains the plant-specific information
        with open(f"{self.path_scenario}/prosumer/{account['id_user']}/config_plants.json", "w") \
                as write_file:
            json.dump(plant_dict, write_file)

    def __init_meter(self, id_meter, id_user, init_reading_positive=0, init_reading_negative=0) -> None:
        """initialize a meter file with initial positive and negative meter readings

        Args:
            id_meter: ID of the meter that is to be initialized
            id_user: ID of the prosumer that the meter belongs to
            init_reading_positive: initial positive meter reading
            init_reading_negative: initial negative meter reading

        Returns:
            None

        """

        # Write json file with meter readings to prosumer directory
        with open(f"{self.path_scenario}/prosumer/{id_user}/meter_{id_meter}.json",
                  "w+") as write_file:
            json.dump([init_reading_positive, init_reading_negative], write_file)

    def __create_plant_files(self, plant_type: str, **kwargs) -> None:
        """calls the corresponding function specified in plant_type

        Args:
            plant_type: str that contains information about the type of plant that is to be created
            kwargs: set of arguments that are required to generate the plant files

        Returns:
            None

        """
        if plant_type == "hh":
            plant_config = self.__create_hh_files(**kwargs)
        elif plant_type == "pv":
            plant_config = self.__create_pv_files(**kwargs)
        elif plant_type == "bat":
            plant_config = self.__create_bat_files(**kwargs)
        elif plant_type == "ev":
            plant_config = self.__create_ev_files(**kwargs)
        elif plant_type == "hp":
            plant_config = self.__create_hp_files(**kwargs)
        elif plant_type == "chp":
            plant_config = self.__create_chp_files(**kwargs)
        elif plant_type == "wind":
            plant_config = self.__create_wind_files(**kwargs)
        elif plant_type == "fixedgen":
            plant_config = self.__create_fixedgen_files(**kwargs)
        else:
            raise Warning("Key was found with no associated function.")
        return plant_config

    def __create_hh_files(self, **kwargs) -> None:
        """creates the household files of the respective prosumer

        Args:
            kwargs: set of arguments that contains the account and plant-ID information

        Returns:
            None

        """

        # Read necessary keyword arguments
        account = kwargs["account"]
        plant_id = kwargs["plant_id"]

        # Read in all household consumption files and their respective annual consumptions
        filenames_hh = os.listdir(f'{self.path_input_data}/prosumers/hh/')
        consumptions_hh = [int(x[3:].partition("_")[0]) for x in filenames_hh]

        # Set the minimum and maximum annual household consumption value that is allowed
        #   If the sizing is done using "distribution" there are three different cases:
        #       1. The consumption lies below the smallest value in the list of "hh_sizing_distribution_intervals"
        #       2. The consumption lies in between two values in the list of "hh_sizing_distribution_intervals"
        #       3. The consumption lies above the highest value in the list of "hh_sizing_distribution_intervals"
        if self.config["prosumer"]["hh_sizing"] == "distribution":
            # 1.: Set the minimum value to 0 and the maximum value to the first value in
            #     "hh_sizing_distribution_intervals"
            if account["list_plant_specs"][0]["annual_consumption"] \
                    < self.config["prosumer"]["hh_sizing_distribution_intervals"][0]:
                interval_min = 0
                interval_max = self.config["prosumer"]["hh_sizing_distribution_intervals"][0]
            # 2.: Set the minimum value to the closest lower value and the maximum value to the closest higher value in
            #     "hh_sizing_distribution_intervals"
            elif self.config["prosumer"]["hh_sizing_distribution_intervals"][0] \
                    < account["list_plant_specs"][0]["annual_consumption"] \
                    < self.config["prosumer"]["hh_sizing_distribution_intervals"][-1]:
                interval_min = [x for x in self.config["prosumer"]["hh_sizing_distribution_intervals"]
                                if x < account["list_plant_specs"][0]["annual_consumption"]][-1]
                interval_max = [x for x in self.config["prosumer"]["hh_sizing_distribution_intervals"]
                                if x >= account["list_plant_specs"][0]["annual_consumption"]][0]
            # 3.: Set the minimum value to the last value of "hh_sizing_distribution_intervals" and the maximum value
            #     to inf as there is no upper bound
            elif account["list_plant_specs"][0]["annual_consumption"] \
                    >= self.config["prosumer"]["hh_sizing_distribution_intervals"][-1]:
                interval_min = self.config["prosumer"]["hh_sizing_distribution_intervals"][-1]
                interval_max = float("inf")
            else:
                raise Warning("Something went wrong in the assignment of the household load intervals. Please check"
                              "__create_hh_files().")
        #   If the sizing is set to "uniform" then the minimum value lies 500 kWh below and the maximum value 500 kWh
        #       above the preliminary annual consumption
        else:  # hh_load_type == "uniform"
            interval_min = max(0, account["list_plant_specs"][0]["annual_consumption"] - 500)
            interval_max = account["list_plant_specs"][0]["annual_consumption"] + 500

        # Choose random consumption pattern that lies between interval_min and interval_max
        try:
            idx = choice([idx for idx, x in enumerate(consumptions_hh) if interval_min < x <= interval_max])
        except IndexError:
            raise LookupError("No files match the household consumption in the destination folder. Please add a file "
                              "that matches the annual consumption set in the config file.")

        # Read respective household time series from input data directory
        filename_hh = f"{self.path_input_data}/prosumers/hh/{filenames_hh[idx]}"
        df_hh = pd.read_csv(filename_hh, usecols=["timestamp", "power"]).set_index("timestamp") * (-1)

        # Update consumption information to actual consumption
        account["list_plant_specs"][0]["annual_consumption"] = consumptions_hh[idx]

        # Save household time series to prosumer specifications directory
        ft.write_dataframe(df_hh.reset_index(),
                           f"{self.path_scenario}/prosumer/{account['id_user']}"
                           f"/raw_data_{plant_id}.ft")

        return account["list_plant_specs"][0]

    def __create_pv_files(self, **kwargs) -> None:
        """creates the PV files of the respective prosumer

        Args:
            kwargs: set of arguments that contains the account and plant-ID information

        Returns:
            None

        """

        # Read necessary keyword arguments
        account = kwargs["account"]
        plant_id = kwargs["plant_id"]

        # Read random normalized PV time series from input data directory
        list_pv = os.listdir(f'{self.path_input_data}/prosumers/pv/')
        filename_pv = f"{self.path_input_data}/prosumers/pv/{choice(list_pv)}"
        df_pv = pd.read_csv(filename_pv).set_index("timestamp")

        # Save scaled PV time series to prosumer specifications directory
        ft.write_dataframe(df_pv.reset_index(),
                           f"{self.path_scenario}/prosumer/{account['id_user']}"
                           f"/raw_data_{plant_id}.ft")
        ix = account["list_plants"].index(plant_id)
        plant_config = account["list_plant_specs"][ix]

        return plant_config

    def __create_bat_files(self, **kwargs) -> None:
        """creates the battery files of the respective prosumer

        Args:
            kwargs: set of arguments that contains the account and plant-ID information

        Returns:
            None

        """

        # Read necessary keyword arguments
        account = kwargs["account"]
        plant_id = kwargs["plant_id"]
        plant_dict = kwargs["plant_dict"]

        # Calculate absolute initial battery SoC in Wh
        soc_init = round(plant_dict[plant_id].get('capacity') * self.config["prosumer"]["bat_soc_init"])

        # Write SoC to prosumer specifications directory
        with open(f"{self.path_scenario}/prosumer/{account['id_user']}/soc_{plant_id}.json", "w") \
                as write_file:
            json.dump(soc_init, write_file)

        ix = account["list_plants"].index(plant_id)
        plant_config = account["list_plant_specs"][ix]

        return plant_config

    def __create_ev_files(self, **kwargs) -> None:
        """creates the electric vehicle files of the respective prosumer

        Args:
            kwargs: set of arguments that contains the account and plant-ID information

        Returns:
            None

        """

        # Read necessary keyword arguments
        account = kwargs["account"]
        plant_id = kwargs["plant_id"]
        plant_dict = kwargs["plant_dict"]

        # Calculate initial electric vehicle SoC in kWh
        soc_init = plant_dict[plant_id].get("capacity") * self.config["prosumer"]["ev_soc_init"]

        # Write SoC to prosumer specifications directory
        with open(f"{self.path_scenario}/prosumer/{account['id_user']}/soc_{plant_id}.json", "w") \
                as write_file:
            json.dump(soc_init, write_file)

        # Read random EV time series containing info about availability and driven distances from input data directory
        df_ev = pd.read_csv(f"{self.path_input_data}/prosumers/ev/"
                            f"{choice(os.listdir(f'{self.path_input_data}/prosumers/ev/'))}"
                            ).set_index("timestamp")

        # Write EV time series to prosumer specifications directory
        ft.write_dataframe(df_ev.reset_index(),
                           f"{self.path_scenario}/prosumer/{account['id_user']}"
                           f"/raw_data_{plant_id}.ft")
        return plant_dict[plant_id]

    def __create_chp_files(self, **kwargs) -> None:
        """creates the heat pump files of the respective prosumer

        Args:
            kwargs: set of arguments that contains the account and plant-ID information

        Returns:
            None

        """

        # Read necessary keyword arguments
        account = kwargs["account"]
        plant_id = kwargs["plant_id"]

        # Find out the annual consumption to identify the correct household heat demand time series
        hh_plant = next(plant for plant in account["list_plant_specs"] if plant["type"] == "hh")
        annual_consumption = hh_plant["annual_consumption"]

        # Read respective household time series from input data directory
        filename_hh = next(household for household in os.listdir(f'{self.path_input_data}/prosumers/hh/')
                           if str(annual_consumption) in household)
        filename_hh = f"{self.path_input_data}/prosumers/hh/{filename_hh}"

        # Read in respective heat demand column
        column = "heat"
        df_chp = pd.read_csv(filename_hh, usecols=["timestamp", column]).set_index("timestamp")*(-1)

        # Write heat demand time series to prosumer specifications directory
        ft.write_dataframe(df_chp.reset_index(),
                           f"{self.path_scenario}/prosumer/{account['id_user']}"
                           f"/raw_data_{plant_id}.ft")

        # Update power and storage capacity information based on peak heat demand
        max_heat = max(abs(df_chp["heat"]))
        chp_power_th = math.ceil(max_heat / 10 ** (len(str(max_heat)) - 1)) * 10 ** (len(str(max_heat)) - 1)  # in W

        # read hp electric power from spec dict
        ix = account["list_plants"].index(plant_id)
        chp_plant = account["list_plant_specs"][ix]
        # set thermal power and storage capacity
        chp_plant["capacity"] *= chp_power_th   # multiplication with the sizing factor set in __gen_chp()
        chp_plant["power_th"] *= chp_power_th   # multiplication with the sizing factor set in __gen_chp()

        # set and save initial SoC of thermal storage
        soc_init = chp_plant["capacity"] * self.config["prosumer"]["chp_soc_init"]
        # Write SoC to prosumer specifications directory
        with open(f"{self.path_scenario}/prosumer/{account['id_user']}/soc_{plant_id}.json", "w") \
                as write_file:
            json.dump(soc_init, write_file)

        return chp_plant

    def __create_hp_files(self, **kwargs) -> None:
        """creates the heat pump files of the respective prosumer

        Args:
            kwargs: set of arguments that contains the account and plant-ID information

        Returns:
            None

        """

        # Read necessary keyword arguments
        account = kwargs["account"]
        plant_id = kwargs["plant_id"]

        # Find out the annual consumption to identify the correct household heat demand time series
        hh_plant = next(plant for plant in account["list_plant_specs"] if plant["type"] == "hh")
        annual_consumption = hh_plant["annual_consumption"]

        # Read respective household time series from input data directory
        filename_hh = next(household for household in os.listdir(f'{self.path_input_data}/prosumers/hh/')
                           if str(annual_consumption) in household)
        filename_hh = f"{self.path_input_data}/prosumers/hh/{filename_hh}"

        # Read in respective heat demand column
        column = "heat"
        df_hp = pd.read_csv(filename_hh, usecols=["timestamp", column]).set_index("timestamp")*(-1)

        # Write heat demand time series to prosumer specifications directory
        ft.write_dataframe(df_hp.reset_index(),
                           f"{self.path_scenario}/prosumer/{account['id_user']}"
                           f"/raw_data_{plant_id}.ft")

        # Update power and storage capacity information based on peak heat demand
        max_heat = max(abs(df_hp["heat"]))
        hp_power_th = math.ceil(max_heat / 10 ** (len(str(max_heat)) - 1)) * 10 ** (len(str(max_heat)) - 1)  # unit in W

        # read hp electric power from spec dict
        ix = account["list_plants"].index(plant_id)
        hp_plant = account["list_plant_specs"][ix]

        # set thermal power and storage capacity
        hp_plant["capacity"] *= hp_power_th
        hp_plant['power_th'] *= hp_power_th

        # set and save initial SoC of thermal storage
        soc_init = hp_plant["capacity"] * self.config["prosumer"]["hp_soc_init"]
        # Write SoC to prosumer specifications directory
        with open(f"{self.path_scenario}/prosumer/{account['id_user']}/soc_{plant_id}.json", "w") \
                as write_file:
            json.dump(soc_init, write_file)

        # Read random heat pump file that fits the heat pump type from input data directory and copy to
        # prosumer specifications directory
        hp_type = hp_plant["hp_type"]
        hp_t_out = self.config["prosumer"]["hp_temperature"]
        hp_dataset = pd.read_csv(f'{self.path_input_data}/prosumers/hp/hp_database.csv')
        hp_param_generic = hp_dataset.loc[(hp_dataset['Model'] == 'Generic') & (hp_dataset['Type'] == hp_type) &
                                          (hp_dataset['Subtype'] == 'On-Off')]  # generic fitting parameter
        df_hp_param = self.__get_hp_parameters(model='Generic', group_id=int(hp_param_generic["Group"].values),
                                               t_in=-7, t_out=hp_t_out, p_th=hp_power_th)  # specific fitting parameter

        df_hp_param.to_json(f"{self.path_input_data}/prosumers/hp/hp_{hp_type[0:5]}.json", orient="records")

        shutil.move(f"{self.path_input_data}/prosumers/hp/hp_{hp_type[0:5]}.json",
                    f"{self.path_scenario}/prosumer/{account['id_user']}/spec_{plant_id}.json")

        return hp_plant

    def __get_hp_parameters(self, model: str, group_id: int = 0, t_in: int = 0, t_out: int = 0, p_th: int = 0,) \
            -> pd.DataFrame:
        """
            Loads the content of the database for a specific heat pump model
            and returns a pandas ``DataFrame`` containing the heat pump parameters.
            Parameters
            ----------
            model : str
                Name of the heat pump model or "Generic".
            group_id : numeric, default 0
                only for model "Generic": Group ID for subtype of heat pump. [1-6].
            t_in : numeric, default 0
                only for model "Generic": Input temperature :math:`T` at primary side of the heat pump. [°C]
            t_out : numeric, default 0
                only for model "Generic": Output temperature :math:`T` at secondary side of the heat pump. [°C]
            p_th : numeric, default 0
                only for model "Generic": Thermal output power at setpoint t_in, t_out (and for
                water/water, brine/water heat pumps t_amb = -7°C). [W]
            Returns
            -------
            parameters : pd.DataFrame
                Data frame containing the model parameters.
            """
        df = pd.read_csv(f'{self.path_input_data}/prosumers/hp/hp_database.csv', delimiter=',')
        df = df.loc[df['Model'] == model]
        parameters = pd.DataFrame()
        parameters['Manufacturer'] = (df['Manufacturer'].values.tolist())
        parameters['Model'] = (df['Model'].values.tolist())
        try:
            parameters['MAPE_COP'] = df['MAPE_COP'].values.tolist()
            parameters['MAPE_P_el'] = df['MAPE_P_el'].values.tolist()
            parameters['MAPE_P_th'] = df['MAPE_P_th'].values.tolist()
        except:
            pass
        parameters['P_th_h_ref [W]'] = (df['P_th_h_ref [W]'].values.tolist())
        parameters['P_el_h_ref [W]'] = (df['P_el_h_ref [W]'].values.tolist())
        parameters['COP_ref'] = (df['COP_ref'].values.tolist())
        parameters['Group'] = (df['Group'].values.tolist())
        parameters['p1_P_th [1/°C]'] = (df['p1_P_th [1/°C]'].values.tolist())
        parameters['p2_P_th [1/°C]'] = (df['p2_P_th [1/°C]'].values.tolist())
        parameters['p3_P_th [-]'] = (df['p3_P_th [-]'].values.tolist())
        parameters['p4_P_th [1/°C]'] = (df['p4_P_th [1/°C]'].values.tolist())
        parameters['p1_P_el_h [1/°C]'] = (df['p1_P_el_h [1/°C]'].values.tolist())
        parameters['p2_P_el_h [1/°C]'] = (df['p2_P_el_h [1/°C]'].values.tolist())
        parameters['p3_P_el_h [-]'] = (df['p3_P_el_h [-]'].values.tolist())
        parameters['p4_P_el_h [1/°C]'] = (df['p4_P_el_h [1/°C]'].values.tolist())
        parameters['p1_COP [-]'] = (df['p1_COP [-]'].values.tolist())
        parameters['p2_COP [-]'] = (df['p2_COP [-]'].values.tolist())
        parameters['p3_COP [-]'] = (df['p3_COP [-]'].values.tolist())
        parameters['p4_COP [-]'] = (df['p4_COP [-]'].values.tolist())
        try:
            parameters['P_th_c_ref [W]'] = (df['P_th_c_ref [W]'].values.tolist())
            parameters['P_el_c_ref [W]'] = (df['P_el_c_ref [W]'].values.tolist())
            parameters['p1_Pdc [1/°C]'] = (df['p1_Pdc [1/°C]'].values.tolist())
            parameters['p2_Pdc [1/°C]'] = (df['p2_Pdc [1/°C]'].values.tolist())
            parameters['p3_Pdc [-]'] = (df['p3_Pdc [-]'].values.tolist())
            parameters['p4_Pdc [1/°C]'] = (df['p4_Pdc [1/°C]'].values.tolist())
            parameters['p1_P_el_c [1/°C]'] = (df['p1_P_el_c [1/°C]'].values.tolist())
            parameters['p2_P_el_c [1/°C]'] = (df['p2_P_el_c [1/°C]'].values.tolist())
            parameters['p3_P_el_c [-]'] = (df['p3_P_el_c [-]'].values.tolist())
            parameters['p4_P_el_c [1/°C]'] = (df['p4_P_el_c [1/°C]'].values.tolist())
            parameters['p1_EER [-]'] = (df['p1_EER [-]'].values.tolist())
            parameters['p2_EER [-]'] = (df['p2_EER [-]'].values.tolist())
            parameters['p3_EER [-]'] = (df['p3_EER [-]'].values.tolist())
            parameters['p4_EER [-]'] = (df['p4_EER [-]'].values.tolist())
        except:
            pass

        if model == 'Generic':
            parameters = parameters.iloc[group_id - 1:group_id]

            def simulate(t_in_primary: Union[float, np.ndarray], t_in_secondary: Union[float, np.ndarray], parameters,
                         t_amb: Union[float, np.ndarray], mode: int = 1,
                         p_th_min: Union[float, np.ndarray] = 0) -> dict:
                """
                Performs the simulation of the heat pump model.
                Parameters
                ----------
                t_in_primary : numeric or iterable (e.g. pd.Series)
                    Input temperature on primry side :math:`T` (air, brine, water). [°C]
                t_in_secondary : numeric or iterable (e.g. pd.Series)
                    Input temperature on secondary side :math:`T` from heating storage or system. [°C]
                parameters : pd.DataFrame
                    Data frame containing the heat pump parameters from hplib.getParameters().
                t_amb : numeric or iterable (e.g. pd.Series)
                    Ambient temperature :math:'T' of the air. [°C]
                mode : int
                    for heating: 1, for cooling: 2
                p_th_min : Minimum thermal power output [W]. Inverter heat pumps increase electrical Power input.
                At maximum electrical input, an electrical heating rod turns on.
                Returns
                -------
                df : pd.DataFrame
                    with the following columns
                    T_in = Input temperature :math:`T` at primary side of the heat pump. [°C]
                    T_out = Output temperature :math:`T` at secondary side of the heat pump. [°C]
                    T_amb = Ambient / Outdoor temperature :math:`T`. [°C]
                    COP = Coefficient of Performance.
                    EER = Energy Efficiency Ratio.
                    P_el = Electrical input Power. [W]
                    P_th = Thermal output power. [W]
                    m_dot = Mass flow at secondary side of the heat pump. [kg/s]
                """

                delta_t = 5  # Inlet temperature is supposed to be heated up by 5 K
                cp = 4200  # J/(kg*K), specific heat capacity of water
                group_id = parameters['Group'].array[0]
                p1_p_el_h = parameters['p1_P_el_h [1/°C]'].array[0]
                p2_p_el_h = parameters['p2_P_el_h [1/°C]'].array[0]
                p3_p_el_h = parameters['p3_P_el_h [-]'].array[0]
                p4_p_el_h = parameters['p4_P_el_h [1/°C]'].array[0]
                p1_cop = parameters['p1_COP [-]'].array[0]
                p2_cop = parameters['p2_COP [-]'].array[0]
                p3_cop = parameters['p3_COP [-]'].array[0]
                p4_cop = parameters['p4_COP [-]'].array[0]
                p_el_ref = parameters['P_el_h_ref [W]'].array[0]
                p_th_ref = parameters['P_th_h_ref [W]'].array[0]
                try:
                    p1_eer = parameters['p1_EER [-]'].array[0]
                    p2_eer = parameters['p2_EER [-]'].array[0]
                    p3_eer = parameters['p3_EER [-]'].array[0]
                    p4_eer = parameters['p4_EER [-]'].array[0]
                    p1_p_el_c = parameters['p1_P_el_c [1/°C]'].array[0]
                    p2_p_el_c = parameters['p2_P_el_c [1/°C]'].array[0]
                    p3_p_el_c = parameters['p3_P_el_c [-]'].array[0]
                    p4_p_el_c = parameters['p4_P_el_c [1/°C]'].array[0]
                    p_el_col_ref = parameters['P_el_c_ref [W]'].array[0]
                except:
                    p1_eer = np.nan
                    p2_eer = np.nan
                    p3_eer = np.nan
                    p4_eer = np.nan
                    p1_p_el_c = np.nan
                    p2_p_el_c = np.nan
                    p3_p_el_c = np.nan
                    p4_p_el_c = np.nan
                    p_el_col_ref = np.nan

                if mode == 2 and group_id > 1:
                    raise ValueError('Cooling is only possible with heat pumps of group id = 1.')

                t_in = t_in_primary  # info value for dataframe
                if mode == 1:
                    t_out = t_in_secondary + delta_t  # Inlet temperature is supposed to be heated up by 5 K
                    eer = 0
                if mode == 2:  # Inlet temperature is supposed to be cooled down by 5 K
                    t_out = t_in_secondary - delta_t
                    cop = 0
                # for subtype = air/water heat pump
                if group_id in (1, 4):
                    t_amb = t_in
                t_ambient = t_amb
                # for regulated heat pumps
                if group_id in (1, 2, 3):
                    if mode == 1:
                        cop = p1_cop * t_in + p2_cop * t_out + p3_cop + p4_cop * t_amb
                        p_el = p_el_ref * (p1_p_el_h * t_in
                                           + p2_p_el_h * t_out
                                           + p3_p_el_h
                                           + p4_p_el_h * t_amb)
                        if group_id == 1:
                            if isinstance(t_in, np.ndarray):
                                t_in = np.full_like(t_in, -7)
                            else:
                                t_in = -7
                            t_amb = t_in

                        elif group_id == 2:
                            if isinstance(t_amb, np.ndarray):
                                t_amb = np.full_like(t_amb, -7)
                            else:
                                t_amb = -7
                        p_el_25 = 0.25 * p_el_ref * (p1_p_el_h * t_in
                                                     + p2_p_el_h * t_out
                                                     + p3_p_el_h
                                                     + p4_p_el_h * t_amb)
                        if isinstance(p_el, np.ndarray):
                            p_el = np.where(p_el < p_el_25, p_el_25, p_el)
                        elif p_el < p_el_25:
                            p_el = p_el_25

                        p_th = p_el * cop

                        if isinstance(cop, np.ndarray):
                            # turn on heating rod and compressor
                            p_el = np.where((cop > 1) & (p_th < p_th_min) & (p_el_ref < p_th_min / cop),
                                            p_el_ref + p_th_ref, p_el)
                            p_th = np.where((cop > 1) & (p_th < p_th_min) & (p_el_ref < p_th_min / cop),
                                            p_el_ref * cop + p_th_ref,
                                            p_th)
                            # increase electrical power for compressor
                            p_el = np.where((cop > 1) & (p_th < p_th_min) & (p_el_ref > p_th_min / cop), p_th_min / cop,
                                            p_el)
                            p_th = np.where((cop > 1) & (p_th < p_th_min) & (p_el_ref > p_th_min / cop), p_th_min, p_th)
                            # only turn on heating rod
                            p_el = np.where(cop <= 1, p_th_ref, p_el)
                            p_th = np.where(cop <= 1, p_th_ref, p_th)
                            cop = p_th / p_el
                        else:
                            if cop <= 1:
                                cop = 1
                                p_el = p_th_ref
                                p_th = p_th_ref
                            elif p_th < p_th_min:
                                if p_el_ref > p_th_min / cop:
                                    p_el = p_th_min / cop
                                    p_th = p_th_min
                                else:
                                    p_el = p_el_ref + p_th_ref
                                    p_th = p_el_ref * cop + p_th_ref
                                    cop = p_th / p_el

                    if mode == 2:
                        eer = (p1_eer * t_in + p2_eer * t_out + p3_eer + p4_eer * t_amb)
                        if isinstance(t_in, np.ndarray):
                            t_in = np.where(t_in < 25, 25, t_in)
                        elif t_in < 25:
                            t_in = 25
                        t_amb = t_in
                        p_el = (p1_p_el_c * t_in + p2_p_el_c * t_out + p3_p_el_c + p4_p_el_c * t_amb) * p_el_col_ref
                        if isinstance(p_el, np.ndarray):
                            eer = np.where(p_el < 0, 0, eer)
                            p_el = np.where(p_el < 0, 0, p_el)
                        elif p_el < 0:
                            eer = 0
                            p_el = 0
                        p_th = -(eer * p_el)
                        if isinstance(eer, np.ndarray):
                            p_el = np.where(eer <= 1, 0, p_el)
                            p_th = np.where(eer <= 1, 0, p_th)
                            eer = np.where(eer <= 1, 0, eer)
                        elif eer < 1:
                            eer = 0
                            p_el = 0
                            p_th = 0

                # for subtype = On-Off
                elif group_id in (4, 5, 6):
                    p_el = (p1_p_el_h * t_in
                            + p2_p_el_h * t_out
                            + p3_p_el_h
                            + p4_p_el_h * t_amb) * p_el_ref

                    cop = p1_cop * t_in + p2_cop * t_out + p3_cop + p4_cop * t_amb

                    p_th = p_el * cop

                    if isinstance(cop, np.ndarray):
                        p_el = np.where((cop > 1) & (p_th < p_th_min), p_el + p_th_ref, p_el)
                        p_th = np.where((cop > 1) & (p_th < p_th_min), p_th + p_th_ref, p_th)
                        p_el = np.where(cop <= 1, p_th_ref, p_el)
                        p_th = np.where(cop <= 1, p_th_ref, p_th)
                        cop = p_th / p_el

                    else:
                        if cop <= 1:
                            cop = 1
                            p_el = p_th_ref
                            p_th = p_th_ref
                        elif p_th < p_th_min:
                            p_th = p_th + p_th_ref
                            p_el = p_el + p_th_ref
                            cop = p_th / p_el

                # massflow
                m_dot = abs(p_th / (delta_t * cp))

                # round
                result = pd.DataFrame()

                result['T_in'] = [t_in_primary]
                result['T_out'] = [t_out]
                result['T_amb'] = [t_ambient]
                result['COP'] = [cop]
                result['EER'] = [eer]
                result['P_el'] = [p_el]
                result['P_th'] = [p_th]
                result['m_dot'] = [m_dot]
                return result

            def get_parameters_fit(model: str, group_id: int = 0, p_th: int = 0) -> pd.DataFrame:
                """
                Helper function for leastsquare fit of thermal output power at reference set point.
                Parameters
                ----------
                model : str
                    Name of the heat pump model.
                group_id : numeric, default 0
                    Group ID for a parameter set which represents an average heat pump of its group.
                p_th : numeric, default 0
                    Thermal output power. [W]
                Returns
                -------
                parameters : pd.DataFrame
                    Data frame containing the model parameters.
                """
                df = pd.read_csv(f'{self.path_input_data}/prosumers/hp/hp_database.csv', delimiter=',')
                df = df.loc[df['Model'] == model]
                parameters = pd.DataFrame()

                parameters['Model'] = (df['Model'].values.tolist())
                parameters['P_th_h_ref [W]'] = (df['P_th_h_ref [W]'].values.tolist())
                parameters['P_el_h_ref [W]'] = (df['P_el_h_ref [W]'].values.tolist())
                parameters['COP_ref'] = (df['COP_ref'].values.tolist())
                parameters['Group'] = (df['Group'].values.tolist())
                parameters['p1_P_th [1/°C]'] = (df['p1_P_th [1/°C]'].values.tolist())
                parameters['p2_P_th [1/°C]'] = (df['p2_P_th [1/°C]'].values.tolist())
                parameters['p3_P_th [-]'] = (df['p3_P_th [-]'].values.tolist())
                parameters['p4_P_th [1/°C]'] = (df['p4_P_th [1/°C]'].values.tolist())
                parameters['p1_P_el_h [1/°C]'] = (df['p1_P_el_h [1/°C]'].values.tolist())
                parameters['p2_P_el_h [1/°C]'] = (df['p2_P_el_h [1/°C]'].values.tolist())
                parameters['p3_P_el_h [-]'] = (df['p3_P_el_h [-]'].values.tolist())
                parameters['p4_P_el_h [1/°C]'] = (df['p4_P_el_h [1/°C]'].values.tolist())
                parameters['p1_COP [-]'] = (df['p1_COP [-]'].values.tolist())
                parameters['p2_COP [-]'] = (df['p2_COP [-]'].values.tolist())
                parameters['p3_COP [-]'] = (df['p3_COP [-]'].values.tolist())
                parameters['p4_COP [-]'] = (df['p4_COP [-]'].values.tolist())

                if model == 'Generic':
                    parameters = parameters.iloc[group_id - 1:group_id]
                    parameters.loc[:, 'P_th_h_ref [W]'] = p_th
                    t_in_hp = [-7, 0, 10]  # air/water, brine/water, water/water
                    t_out_fix = 52
                    t_amb_fix = -7
                    p1_cop = parameters['p1_COP [-]'].array[0]
                    p2_cop = parameters['p2_COP [-]'].array[0]
                    p3_cop = parameters['p3_COP [-]'].array[0]
                    p4_cop = parameters['p4_COP [-]'].array[0]
                    if group_id == 1 or group_id == 4:
                        t_in_fix = t_in_hp[0]
                    if group_id == 2 or group_id == 5:
                        t_in_fix = t_in_hp[1]
                    if group_id == 3 or group_id == 6:
                        t_in_fix = t_in_hp[2]
                    cop_ref = p1_cop * t_in_fix + p2_cop * t_out_fix + p3_cop + p4_cop * t_amb_fix
                    p_el_ref = p_th / cop_ref
                    parameters.loc[:, 'P_el_h_ref [W]'] = p_el_ref
                    parameters.loc[:, 'COP_ref'] = cop_ref
                return parameters

            def fit_func_p_th_ref(p_th: int, t_in: int, t_out: int, group_id: int, p_th_set_point: int) -> int:
                """
                Helper function to determine difference between given and calculated
                thermal output power in [W].
                Parameters
                ----------
                p_th : numeric
                    Thermal output power. [W]
                t_in : numeric
                    Input temperature :math:`T` at primary side of the heat pump. [°C]
                t_out : numeric
                    Output temperature :math:`T` at secondary side of the heat pump. [°C]
                group_id : numeric
                    Group ID for a parameter set which represents an average heat pump of its group.
                p_th_set_point : numeric
                    Thermal output power. [W]
                Returns
                -------
                p_th_diff : numeric
                    Thermal output power. [W]
                """
                if group_id == 1 or group_id == 4:
                    t_amb = t_in
                else:
                    t_amb = -7
                parameters = get_parameters_fit(model='Generic', group_id=group_id, p_th=p_th)
                df = simulate(t_in, t_out - 5, parameters, t_amb)
                p_th_calc = df.P_th.values[0]
                p_th_diff = p_th_calc - p_th_set_point
                return p_th_diff

            def fit_p_th_ref(t_in: int, t_out: int, group_id: int, p_th_set_point: int) -> Any:
                """
                Determine the thermal output power in [W] at reference conditions (T_in = [-7, 0, 10] ,
                T_out=52, T_amb=-7) for a given set point for a generic heat pump, using a least-square method.
                Parameters
                ----------
                t_in : numeric
                    Input temperature :math:`T` at primary side of the heat pump. [°C]
                t_out : numeric
                    Output temperature :math:`T` at secondary side of the heat pump. [°C]
                group_id : numeric
                    Group ID for a parameter set which represents an average heat pump of its group.
                p_th_set_point : numeric
                    Thermal output power. [W]
                Returns
                -------
                p_th : Any
                    Thermal output power. [W]
                """
                P_0 = [1000]  # starting values
                a = (t_in, t_out, group_id, p_th_set_point)
                p_th, _ = optimize.leastsq(fit_func_p_th_ref, P_0, args=a)
                return p_th

            p_th_ref = fit_p_th_ref(t_in, t_out, group_id, p_th)  # may be simplified
            parameters.loc[:, 'P_th_h_ref [W]'] = p_th_ref
            t_in_hp = [-7, 0, 10]  # air/water, brine/water, water/water
            t_out_fix = 52
            t_amb_fix = -7
            p1_cop = parameters['p1_COP [-]'].array[0]
            p2_cop = parameters['p2_COP [-]'].array[0]
            p3_cop = parameters['p3_COP [-]'].array[0]
            p4_cop = parameters['p4_COP [-]'].array[0]
            if (p1_cop * t_in + p2_cop * t_out + p3_cop + p4_cop * t_amb_fix) <= 1.0:
                raise ValueError('COP too low! Increase t_in or decrease t_out.')
            if group_id == 1 or group_id == 4:
                t_in_fix = t_in_hp[0]
            if group_id == 2 or group_id == 5:
                t_in_fix = t_in_hp[1]
            if group_id == 3 or group_id == 6:
                t_in_fix = t_in_hp[2]
            cop_ref = p1_cop * t_in_fix + p2_cop * t_out_fix + p3_cop + p4_cop * t_amb_fix
            p_el_ref = p_th_ref / cop_ref
            parameters.loc[:, 'P_el_h_ref [W]'] = p_el_ref
            parameters.loc[:, 'COP_ref'] = cop_ref
            if group_id == 1:
                try:
                    p1_eer = parameters['p1_EER [-]'].array[0]
                    p2_eer = parameters['p2_EER [-]'].array[0]
                    p3_eer = parameters['p3_EER [-]'].array[0]
                    p4_eer = parameters['p4_EER [-]'].array[0]
                    eer_ref = p1_eer * 35 + p2_eer * 7 + p3_eer + p4_eer * 35
                    parameters.loc[:, 'P_th_c_ref [W]'] = p_el_ref * 0.6852 * eer_ref
                    parameters['P_el_c_ref [W]'] = \
                        p_el_ref * 0.6852  # average value from real Heatpumps (P_el35/7 to P_el-7/52)
                    parameters.loc[:, 'EER_ref'] = eer_ref
                except:
                    pass
        return parameters

    def __create_wind_files(self, **kwargs) -> None:
        """creates the wind files of the respective prosumer

        Args:
            kwargs: set of arguments that contains the account and plant-ID information

        Returns:
            None

        """

        # Read necessary keyword arguments
        account = kwargs["account"]
        plant_id = kwargs["plant_id"]
        # plant_dict = kwargs["plant_dict"]

        # Read in all wind profiles and select one randomly
        filenames_wind = os.listdir(f'{self.path_input_data}/prosumers/wind/')
        filename_wind = choice(filenames_wind)

        # Copy wind file under plant_id name into prosumer specifications directory
        shutil.copyfile(f"{self.path_input_data}/prosumers/wind/{filename_wind}",
                        f"{self.path_scenario}/prosumer/{account['id_user']}/spec_{plant_id}.json")

        ix = account["list_plants"].index(plant_id)
        plant_config = account["list_plant_specs"][ix]

        return plant_config

    def __create_fixedgen_files(self, **kwargs) -> None:
        """creates the fixed generation files of the respective prosumer

        Args:
            kwargs: set of arguments that contains the account and plant-ID information

        Returns:
            None

        """

        # Read necessary keyword arguments
        account = kwargs["account"]
        plant_id = kwargs["plant_id"]

        # Read random fixedgen generation time series from input data directory
        df_fixedgen = pd.read_csv(f"{self.path_input_data}/prosumers/fixedgen/"
                                  f"{choice(os.listdir(f'{self.path_input_data}/prosumers/fixedgen/'))}"
                                  ).set_index("timestamp")

        # Write fixedgen generation time series to prosumer specifications directory
        ft.write_dataframe(df_fixedgen.reset_index(),
                           f"{self.path_scenario}/prosumer/{account['id_user']}/"
                           f"raw_data_{plant_id}.ft")

        ix = account["list_plants"].index(plant_id)
        plant_config = account["list_plant_specs"][ix]

        return plant_config

    def __create_aggregator(self) -> None:
        """creates the aggregator files if activated

        Comment:
            An aggregator can only be used when the market type is set to ex-ante.

        Args:

        Returns:
            None

        """

        # Check if aggregator is active
        if not self.config["aggregator"]["active"]:  # exit function if aggregator is inactive
            return

        # Check if market type is set to ex-ante. Otherwise, set aggregator to False and save the updated config file
        if not self.config["lem"]["types_clearing_ex_ante"]:
            print("There can be no aggregator when the market is set to ex-post. The aggregator was deactivated and "
                  "the config file updated and saved.")
            self.config["aggregator"]["active"] = False
            # Copy config file to scenario directory
            with open(f"{self.path_scenario}/config.yaml", 'w') as file:
                self.yaml.dump(self.config, file)
            return

        # Load aggregator configuration
        config_aggregator = self.config["aggregator"]
        self.config["aggregator"]["id_market_agent"] = self.config["aggregator"]["id_user"]

        # Configure forecast parameters
        if config_aggregator["fcast"] == "sarma":
            fcast_order = config_aggregator["fcast_sarma_order"]
            num_param = sum(fcast_order) - fcast_order[6] - fcast_order[10]
            config_aggregator["fcast_param"] = [1 / num_param] * num_param
        else:
            config_aggregator["fcast_param"] = 9

        # Add "prosumers"-entry to configuration and save in aggregator specifications directory. The list will contain
        #   the IDs of all prosumers that are managed by the aggregator
        config_aggregator["prosumers"] = []
        with open(f"{self.path_scenario}/aggregator/config_account.json", "w+") as write_file:
            json.dump(config_aggregator, write_file)

        # Deaggregate first to ensure everything is clean
        self.__deaggregate_prosumers()

        # Aggregate the list of prosumers that are managed by the aggregator
        if self.config["aggregator"]["active"] is True:
            self.__aggregate_prosumers(without_plants_only=self.config["aggregator"]["prosumers_wo_plants_only"],
                                       without_bat_only=self.config["aggregator"]["prosumers_wo_battery_only"])

    def __deaggregate_prosumers(self, list_prosumers: list = None) -> None:
        """deaggregates all prosumers that were in the list of prosumers to be managed by the aggregator, changes their
        forecasting method to the one specified in the config file and deactivates the aggregator

        Comment:
            An aggregator can only be used when the market type is set to ex-ante

        Args:

        Returns:
            None

        """

        # Get a list of all prosumers if not provided as input
        if list_prosumers is None:
            list_prosumers = os.listdir(f"{self.path_scenario}/prosumer")

        # Read config file of aggregator account
        try:
            with open(f"{self.path_scenario}/aggregator/config_account.json", "r") as read_file:
                agg_dict = json.load(read_file)
        except FileNotFoundError as err:
            raise err

        # Deaggregate prosumers and set their forecasting model to the model specified in the scenario configuration
        for prosumer in list_prosumers:

            # Delete existing list entry if prosumer is already in list and reconfigure forecasting method
            if prosumer in agg_dict["prosumers"]:
                agg_dict["prosumers"].remove(prosumer)

                # Reconfigure forecast for prosumer household to settings in scenario_config
                self.__reconfig_fcast_hh(prosumer=prosumer)

            # If no prosumers are left in the list set aggregator to False and exit loop
            if len(agg_dict["prosumers"]) == 0:
                agg_dict["active"] = False
                break

        # Save updated aggregator config file in the aggregator directory
        with open(f"{self.path_scenario}/aggregator/config_account.json", "w+") as write_file:
            json.dump(agg_dict, write_file)

        # Update the aggregated loads
        self.__gen_sum_aggregated_loads()

    def __reconfig_fcast_hh(self, prosumer: str) -> None:
        """reconfigures the forecasting method for the plant type "household"

        Args:
            prosumer: string that contains the prosumer's ID for which the forecasting method needs to be changed

        Returns:
            None

        """

        # Get the prosumer's plant configuration and reset the forecasting method to settings in scenario_config
        with open(f"{self.path_scenario}/prosumer/{prosumer}/config_plants.json", "r") as read_file:
            plant_dict = json.load(read_file)

        # Configure forecast settings again using the configuration in the config file
        hh_plant = next(plant for plant in plant_dict if plant_dict[plant]["type"] == "hh")
        if self.config["prosumer"]["hh_fcast"] == "sarma":
            fcast_order = self.config["prosumer"]["hh_fcast_sarma_order"]
            num_param = sum(fcast_order) - fcast_order[6] - fcast_order[10]
            fcast_param = [1 / num_param] * num_param
            plant_dict[hh_plant]["fcast"] = self.config["prosumer"]["hh_fcast"]
            plant_dict[hh_plant]["fcast_order"] = fcast_order
            plant_dict[hh_plant]["fcast_param"] = fcast_param
        elif self.config["prosumer"]["hh_fcast"] == "smoothed":
            plant_dict[hh_plant]["fcast"] = self.config["prosumer"]["hh_fcast"]
            plant_dict[hh_plant]["fcast_order"] = []
            plant_dict[hh_plant]["fcast_param"] = 9
        else:
            plant_dict[hh_plant]["fcast"] = self.config["prosumer"]["hh_fcast"]
            plant_dict[hh_plant]["fcast_order"] = []
            plant_dict[hh_plant]["fcast_param"] = []

        # Save new plant dict
        with open(f"{self.path_scenario}/prosumer/{prosumer}/config_plants.json",
                  "w+") as write_file:
            json.dump(plant_dict, write_file)

    def __aggregate_prosumers(self, list_prosumers: list = None, without_plants_only: bool = False,
                              without_bat_only: bool = False) -> None:
        """aggregates all prosumers that were in the list of prosumers to be managed by the aggregator, changes their
        forecasting method to "aggregator" and activates the aggregator

        Args:
            list_prosumers: list of prosumers to be managed by the aggregator. If no list is provided all prosumers are
                            added to the list
            without_bat_only: boolean that states if only the prosumers without a battery are to be managed by the
                              aggregator. Is automatically set to False if a list of prosumers was provided

        Returns:
            None

        """

        # Deactivate check for batteries if list of prosumers was provided
        if list_prosumers is not None:
            without_plants_only = False
            without_bat_only = False

        # Import list of prosumers from directory if none was provided
        list_prosumers = os.listdir(f"{self.path_scenario}/prosumer") if list_prosumers is None else list_prosumers

        # Read aggregator account configuration
        with open(f"{self.path_scenario}/aggregator/config_account.json", "r") as read_file:
            agg_dict = json.load(read_file)

        # Loop through list of prosumers and add them to the aggregator if conditions are fulfilled
        for prosumer in list_prosumers:
            with open(f"{self.path_scenario}/prosumer/{prosumer}/config_plants.json", "r") as read_file:
                plant_dict = json.load(read_file)

            # If only prosumers without any plant need to be aggregated check for batteries and set flag
            has_plant = True if without_plants_only and len(plant_dict) > 1 else False

            # If only prosumers without a battery need to be aggregated check for batteries and set flag
            if without_bat_only and not without_plants_only:
                has_bat = next((True for plant in plant_dict if plant_dict[plant]["type"] == "bat"), False)
            elif without_plants_only:
                has_bat = True
            else:
                has_bat = False

            # Aggregate prosumer if both without_plants_only or without_bat_only is set to False
            #   or prosumer has no plants or battery respectively
            if not has_plant or not has_bat:

                # Change forecast type of household to aggregator for prosumers with a household demand
                hh_plant = next((plant for plant in plant_dict if plant_dict[plant]["type"] == "hh"), None)
                if hh_plant:
                    plant_dict[hh_plant]["fcast"] = "aggregator"
                    if prosumer not in agg_dict["prosumers"]:
                        agg_dict["prosumers"].append(prosumer)

            # Save updated plants configuration of the prosumer to prosumer directory
            with open(f"{self.path_scenario}/prosumer/{prosumer}/config_plants.json", "w+") as write_file:
                json.dump(plant_dict, write_file)

        # Activate aggregator if prosumers need to be managed
        if len(agg_dict["prosumers"]) > 0:
            agg_dict["active"] = True

        # Save updated aggregator account configuration to aggregator directory
        with open(f"{self.path_scenario}/aggregator/config_account.json", "w+") as write_file:
            json.dump(agg_dict, write_file)

        # Sum all loads that are managed by the aggregator
        self.__gen_sum_aggregated_loads()

    def __gen_sum_aggregated_loads(self) -> None:
        """sums up all the household loads that are to be managed by the aggregator

        Args:

        Returns:
            None

        """

        # List for all households to be managed by the aggregator
        list_aggregated_plants = []

        # Add every prosumer to the aggregator list, if forecast type is set to "aggregator"
        for prosumer in os.listdir(f'{self.path_scenario}/prosumer/'):
            with open(f"{self.path_scenario}/prosumer/{prosumer}/config_plants.json", "r") as read_file:
                plant_dict = json.load(read_file)
            next((list_aggregated_plants.append([prosumer, plant]) for plant in plant_dict
                  if plant_dict[plant]["type"] == "hh" and plant_dict[plant]["fcast"] == "aggregator"), None)

        # Check if there are prosumers to be managed by the aggregator, if so add the load profile to aggregated loads,
        #   otherwise delete aggregated load files from the aggregator directory if they exist
        if list_aggregated_plants:
            flag_first = True
            for prosumer, plant in list_aggregated_plants:
                # Copy and paste load profile of first prosumer to aggregator directory
                if flag_first:
                    shutil.copyfile(f"{self.path_scenario}/prosumer/{prosumer}/raw_data_{plant}.ft",
                                    f"{self.path_scenario}/aggregator/raw_data_aggregated_loads.ft")
                    df_aggregated_load = ft.read_dataframe(f"{self.path_scenario}/aggregator"
                                                           f"/raw_data_aggregated_loads.ft") \
                        .set_index("timestamp")
                    flag_first = False
                else:  # Add all other load profiles to the first one
                    df_load = ft.read_dataframe(f"{self.path_scenario}/prosumer/{prosumer}/raw_data_{plant}.ft") \
                        .set_index("timestamp")
                    df_aggregated_load += df_load

            # Save aggregated load profile as ft and csv file
            ft.write_dataframe(df_aggregated_load.reset_index(),
                               f"{self.path_scenario}/aggregator/raw_data_aggregated_loads.ft")
            df_aggregated_load.to_csv(f"{self.path_scenario}/aggregator/raw_data_aggregated_loads.csv")
        else:
            try:
                os.remove(f"{self.path_scenario}/aggregator/aggregated_loads.ft")
            except FileNotFoundError:
                pass
            try:
                os.remove(f"{self.path_scenario}/aggregator/aggregated_loads.csv")
            except FileNotFoundError:
                pass

    def __edit_new(self, category: str) -> None:
        """calls the respective function for the market participant specified in category to be created anew

        Args:
            category: string that contains the type of market participant that needs to be created anew

        Returns:
            None

        """

        if category == "lem":
            self.__create_lem()
        elif category == "retailer":
            self.__create_retailer()
        elif category == "aggregator":
            self.__create_aggregator()
        else:
            raise Warning(f"The category {category} is not to be edited using __edit_new(). Please check the "
                          f"associations of the categories in edit_scenario().")

    def __edit_individual(self, category: str, config_old: dict, base_path_old: str) -> None:
        """calls the respective function for the market participant specified in category to edit the files

        Comment:
            The difference between __edit_new() and __edit_individual() is that the former deletes the corresponding
            files first and then builds them anew while the latter changes the existing files. This allows to only
            change a few parameters while leaving the rest, e.g. the household specifications, intact.

        Args:
            category: string that contains the type of market participant that needs to be edited
            config_old: dictionary that contains the config file of the scenario that the new one is based on
            base_path_old: string the points to the base path of the existing scenario

        Returns:
            None

        """

        # Depending on the category make the edits
        if category == "prosumer" or "producer":
            # Copy old files to new directory to edit afterwards (do not copy again, if it already exists)
            self.__copy_folder("prosumer", base_path_old, overwrite=False)

            # Loop through all settings and change the ones that are different
            for setting in self.config[category]:
                if self.config[category][setting] != config_old[category][setting]:
                    self.__change_prosumer_setting(participant_type=category, setting=setting,
                                                   val=self.config[category][setting],
                                                   val_old=config_old[category][setting])
        else:
            raise Warning(f"The category {category} is not to be edited using __edit_individual(). Please check the "
                          f"associations of the categories in edit_scenario().")

    def __copy_folder(self, category: str, from_path: str, overwrite: bool = True) -> None:
        """copies a folder and its subfolders of the specified category of market participant into the current directory

        Args:
            category: string that contains the type of market participant whose folder is copied
            from_path: string that contains the path to the scenario that the folder is copied from
            overwrite: boolean that specifies if the folder is to be overwritten if it already exists

        Returns:
            None

        """

        # Specify from and to folder paths
        from_path = f"{from_path}/{category}"
        to_path = f"{self.path_scenario}/{category}"

        # Copy folder to new directory. Check if folder exists and contains something
        if os.path.exists(to_path):
            if overwrite or not os.listdir(to_path):
                shutil.rmtree(to_path)
                shutil.copytree(from_path, to_path)
            else:
                return
        else:
            shutil.copytree(from_path, to_path)

    def __print_changed_settings(self, category: str, config_old: dict) -> None:
        """prints out the settings' names that were changed and what the old and new values are

        Args:
            category: string that contains the category in the config file
            config_old: dictionary that contains the old config settings

        Returns:
            None

        """

        # Loop through all settings and print out the changed ones
        for setting in config_old[category]:
            if self.config[category][setting] != config_old[category][setting]:
                print(f"{category} - {setting}: {config_old[category][setting]} "
                      f"--> {self.config[category][setting]}")

    def __change_prosumer_setting(self, participant_type: str, setting: str, val, val_old,
                                  list_prosumers: list = None) -> None:
        """edits the provided setting in prosumer for all prosumer provided in list_prosumers

        Comment:
            If new settings are added to the config file they need to be added to the dictionary of actions in this
            function so the program knows how to change this setting. Otherwise, a warning will be raised. Please see
            the comments within this function on how to add a new setting to the dictionary of actions.

        Args:
            participant_type: string that contains the type of participant that is to be changed
            setting: setting that needs to be changed
            val: new value for the setting
            val_old: old value of the setting
            list_prosumers: list that contains all the prosumers for whom the setting needs to be changed.
                            If no list is provided the change will be applied to all prosumers

        Returns:
            None

        """

        # Flag to detect thrown errors
        flag_error = False

        # Get a list of all prosumers that are to be checked if none is provided
        list_prosumers_all = os.listdir(f"{self.path_scenario}/prosumer") if list_prosumers is None else list_prosumers
        # Shorten the list to the type of participant that is to be changed to ensure that there are no conflicting
        #   actions taken for the different types of prosumers
        list_prosumers = []
        for prosumer in list_prosumers_all:
            with open(f"{self.path_scenario}/prosumer/{prosumer}/config_plants.json", "r") as read_file:
                plant_dict = json.load(read_file)
            plant = next((plant for plant in plant_dict if plant_dict[plant]["type"] == "hh"), None)
            if participant_type == "prosumer" and plant is not None:
                list_prosumers.append(prosumer)
            elif participant_type == "producer" and plant is None:
                list_prosumers.append(prosumer)

        # Check what type of plant needs to be edited
        setting_type = setting.split("_", 1)[0]

        # Dictionary that contains all available settings in prosumer/producer and specifies the according actions
        # NOTE: If a setting is changed or added to prosumer in the config file it also needs to be added to this dict
        #       as otherwise the setting will either be changed incorrectly or a warning is raised
        # General structure of dict_actions entries:
        #   key: ([what files to edit], [how to edit the files], [name of variable])
        # What files to edit - available options:
        # NOTE: Several files can be changed as long as they are separated by a comma and have an associated action
        #     None: Do nothing to this value
        #     "forbidden": Value cannot be changed with edit_scenario(). Results in an exit and deletion of the
        #                  edited scenario
        #     "account": Loads the setting in the account config file and overwrites it, after the specified actions
        #                were applied, with the new value
        #     "plants": Loads the setting in the plant config file and overwrites it, after the specified actions
        #               were applied, with the new value
        #     "soc": Reads in the according json file that contains the SoC and overwrites it, after the specified
        #            actions were applied, with the new value
        # How to edit the files - available actions:
        # NOTE: Actions can be chained together to execute several at once using hyphens (e.g. "update-overwrite")
        #     None: Do nothing
        #     "update": Update existing value by multiplying the old with the new value (val = val * new/old)
        #     "overwrite": Replace old value with new value (val = new)
        #     "multiply": Multiply new value with old value (val = val * new)
        #     "ratio": New value is a ratio of the new and old value (val = new/old)
        #     "choice": New value is random value from list (val = choice(list(val)))
        # Name of variable - available naming methodologies:
        # NOTE: This is optional and can be left out
        #     Code: Write some code that will be evaluated to generate the name (needs to be a string in the dict)
        #     String: Write out the name directly as string
        #     Empty: Leaving it empty will set the name to the value of "setting"
        if participant_type == "prosumer":
            dict_actions = {
                "general": {
                    "general_number_of": (["forbidden"], [None]),
                    "general_solver": (["account"], ["overwrite"], "setting.split('_', 1)[1]"),
                },
                "hh": {
                    "hh_has_submeter": (["plants"], ["overwrite"], "setting.split('_', 1)[1]"),
                    "hh_sizing": (["forbidden"], [None]),
                    "hh_sizing_uniform": (["forbidden"], [None]),
                    "hh_sizing_distribution": (["forbidden"], [None]),
                    "hh_sizing_distribution_intervals": (["forbidden"], [None]),
                    "hh_fcast": ([None], [None]),
                    "hh_fcast_sarma_order": ([None], [None]),
                },
                "pv": {
                    "pv_fraction": (["forbidden"], [None]),
                    "pv_sizing_power": (["plants"], ["update"], "setting.split('_', 2)[2]"),
                    "pv_sizing_power_deviation": (["forbidden"], [None]),
                    "pv_controllable": (["plants"], ["overwrite"], "setting.split('_', 1)[1]"),
                    "pv_fcast": (["plants"], ["overwrite"], "setting.split('_', 1)[1]"),
                    "pv_quality": (["plants"], ["overwrite"], "setting.split('_', 1)[1]"),
                },
                "wind": {
                    "wind_fraction": (["forbidden"], [None]),
                    "wind_sizing_power": (["plants"], ["update"], "setting.split('_', 2)[2]"),
                    "wind_sizing_power_deviation": (["forbidden"], [None]),
                    "wind_controllable": (["plants"], ["overwrite"], "setting.split('_', 1)[1]"),
                    "wind_fcast": (["plants"], ["overwrite"], "setting.split('_', 1)[1]"),
                    "wind_quality": (["plants"], ["overwrite"], "setting.split('_', 1)[1]"),
                },
                "bat": {
                    "bat_fraction": (["forbidden"], [None]),
                    "bat_fraction_dependent_on_pv": (["forbidden"], [None]),
                    "bat_sizing_power": (["plants"], ["update"], "setting.split('_', 2)[2]"),
                    "bat_sizing_capacity": (["plants", "soc"], ["update", "update"], "setting.split('_', 2)[2]"),
                    "bat_efficiency": (["plants"], ["overwrite"], "setting.split('_', 1)[1]"),
                    "bat_soc_init": (["soc"], ["update"], "setting.split('_', 1)[1]"),
                    "bat_charge_from_grid": (["plants"], ["overwrite"], "setting.split('_', 1)[1]"),
                    "bat_quality": (["plants"], ["overwrite"], "setting.split('_', 1)[1]"),
                },
                "ev": {
                    "ev_fraction": (["forbidden"], [None]),
                    "ev_efficiency": (["plants"], ["overwrite"], "setting.split('_', 1)[1]"),
                    "ev_v2g": (["plants"], ["overwrite"], "setting.split('_', 1)[1]"),
                    "ev_charging_power": (["plants"], ["choice"], "setting.split('_', 1)[1]"),
                    "ev_capacity": (["plants"], ["choice"], "setting.split('_', 1)[1]"),
                    "ev_consumption": (["plants"], ["choice"], "setting.split('_', 1)[1]"),
                    "ev_soc_init": (["soc"], ["update"], "setting.split('_', 1)[1]"),
                    "ev_fcast": (["plants"], ["overwrite"], "setting.split('_', 1)[1]"),
                    "ev_quality": (["plants"], ["overwrite"], "setting.split('_', 1)[1]"),
                },
                "hp": {
                    "hp_fraction": (["forbidden"], [None]),
                    "hp_fcast": (["plants"], ["overwrite"], "setting.split('_', 1)[1]"),
                },
                "fixedgen": {
                    "fixedgen_fraction": (["forbidden"], [None]),
                    "fixedgen_power": (["plants"], ["overwrite"], "setting.split('_', 1)[1]"),
                    "fixedgen_controllable": (["plants"], ["overwrite"], "setting.split('_', 1)[1]"),
                    "fixedgen_quality": (["plants"], ["overwrite"], "setting.split('_', 1)[1]"),
                },
                "mpc": {
                    "mpc_price_fcast": (["account"], ["overwrite"]),
                    "mpc_horizon": (["account"], ["overwrite"]),
                },
                "ma": {
                    "ma_strategy": (["account"], ["choice"]),
                    "ma_horizon": (["account"], ["choice"]),
                    "ma_preference_quality": (["account"], ["choice"]),
                    "ma_premium_preference_quality": (["account"], ["choice"]),
                },
                "meter": {
                    "meter_prob_late": (["account"], ["overwrite"]),
                    "meter_prob_late_95": (["account"], ["overwrite"]),
                    "meter_prob_missing": (["account"], ["overwrite"]),
                },
            }
        elif participant_type == "producer":
            dict_actions = {
                "pv": {
                    "pv_active": (["forbidden"], [None]),
                    "pv_power": (["plants"], ["overwrite"], "setting.split('_', 1)[1]"),
                    "pv_controllable": (["plants"], ["overwrite"], "setting.split('_', 1)[1]"),
                    "pv_fcast": (["plants"], ["overwrite"], "setting.split('_', 1)[1]"),
                    "pv_quality": (["plants"], ["overwrite"], "setting.split('_', 1)[1]"),
                },
                "wind": {
                    "wind_active": (["forbidden"], [None]),
                    "wind_power": (["plants"], ["overwrite"], "setting.split('_', 1)[1]"),
                    "wind_controllable": (["plants"], ["overwrite"], "setting.split('_', 1)[1]"),
                    "wind_fcast": (["plants"], ["overwrite"], "setting.split('_', 1)[1]"),
                    "wind_quality": (["plants"], ["overwrite"], "setting.split('_', 1)[1]"),
                },
                "fixedgen": {
                    "fixedgen_active": (["forbidden"], [None]),
                    "fixedgen_power": (["plants"], ["overwrite"], "setting.split('_', 1)[1]"),
                    "fixedgen_controllable": (["plants"], ["overwrite"], "setting.split('_', 1)[1]"),
                    "fixedgen_quality": (["plants"], ["overwrite"], "setting.split('_', 1)[1]"),
                },
            }
        else:
            raise Warning(f"Participant type {participant_type} does not exist.")

        # Get the name of the config/file name that is to be changed
        try:
            setting_name = eval(dict_actions[setting_type][setting][2])
        except NameError:
            setting_name = dict_actions[setting_type][setting][2]
        except IndexError:
            setting_name = setting
        except KeyError:
            raise KeyError(f"The setting {setting} does not exist in the dictionary containing the corresponding "
                           f"actions. Please update the dictionary 'dict_actions' accordingly.")

        # Loop through all files that need to be changed
        for idx in range(len(dict_actions[setting_type][setting][0])):
            # Loop through all prosumers and edit the desired setting
            for prosumer in list_prosumers:
                # Get the corresponding value_type and act accordingly
                value_type = dict_actions[setting_type][setting][0][idx]
                # Break out of loop and raise warning
                if value_type == "forbidden":
                    flag_error = True
                    break
                # Break out of loop as nothing needs to be done
                elif value_type is None:
                    break
                # Load the setting from the prosumer's account config
                elif value_type == "account":
                    with open(f"{self.path_scenario}/prosumer/{prosumer}/config_account.json", "r") as read_file:
                        user_dict = json.load(read_file)
                    val_new = user_dict[setting_name]
                # Load the setting from the prosumer's plant config. Skip prosumers that do not have this type of plant
                elif value_type == "plants":
                    with open(f"{self.path_scenario}/prosumer/{prosumer}/config_plants.json", "r") as read_file:
                        plant_dict = json.load(read_file)
                    plant = next((plant for plant in plant_dict if plant_dict[plant]["type"] == setting_type), None)
                    if plant:
                        val_new = plant_dict[plant][setting_name]
                    else:
                        continue
                # Load the setting from the prosumer's SoC file. Skip prosumers that do not have this type of plant
                elif value_type == "soc":
                    with open(f"{self.path_scenario}/prosumer/{prosumer}/config_plants.json", "r") as read_file:
                        plant_dict = json.load(read_file)
                    plant = next((plant for plant in plant_dict if plant_dict[plant]["type"] == setting_type), None)
                    if plant:
                        with open(f"{self.path_scenario}/prosumer/{prosumer}/soc_{plant}.json", "r") as read_file:
                            val_new = json.load(read_file)
                    else:
                        continue
                else:
                    raise Warning(f"Chosen action '{value_type}' in __change_prosumer_setting not available. "
                                  f"Please choose another action or adjust the function accordingly.")

                # Read out the specified actions and run through all of them
                actions = dict_actions[setting_type][setting][1][idx].split("-")
                for action in actions:
                    if action is None:
                        pass
                    elif action == "update":
                        val_new *= val / val_old
                    elif action == "overwrite":
                        val_new = val
                    elif action == "multiply":
                        val_new *= val
                    elif action == "ratio":
                        val_new = val / val_old
                    elif action == "choice":
                        val_new = choice(val)
                    else:
                        raise Warning(f"Chosen action '{action}' in __change_prosumer_setting not available. "
                                      "Please choose another action or adjust the function accordingly.")

                # Update with the new value and overwrite the corresponding file if necessary
                if value_type == "account":
                    user_dict[setting_name] = val_new
                    with open(f"{self.path_scenario}/prosumer/{prosumer}/config_account.json", "w+") as write_file:
                        json.dump(user_dict, write_file)
                elif value_type == "plants":
                    plant_dict[plant][setting_name] = val_new
                    with open(f"{self.path_scenario}/prosumer/{prosumer}/config_plants.json", "w+") as write_file:
                        json.dump(plant_dict, write_file)
                elif value_type == "soc":
                    with open(f"{self.path_scenario}/prosumer/{prosumer}/soc_{plant}.json", "w+") as write_file:
                        json.dump(val_new, write_file)

            # Exit all for-loops if error was detected
            if flag_error:
                break

        # If error was thrown the folder of the new scenario is deleted and a warning message given
        if flag_error:
            try:
                shutil.rmtree(self.path_scenario)
            except OSError as e:
                print(f"Error: {self.path_scenario} : {e.strerror}")
            finally:
                raise Warning(f"The setting '{setting}' has too great of an influence on the entire simulation "
                              f"and thus cannot be changed.\n"
                              f"Please create a new scenario using 'new_scenario()' instead.")

    @staticmethod
    def __del_file_contents(path) -> None:
        """deletes all files and folders within the specified path

        Args:
            path: string that contains the path of the directory that needs to be deleted

        Returns:
            None

        """

        for root, directories, files in os.walk(path):
            for file in files:
                os.unlink(os.path.join(root, file))
            for directory in directories:
                shutil.rmtree(os.path.join(root, directory))

    @staticmethod
    def __gen_rand_bool_list(length: int, share_1s: float) -> list:
        """generates a randomly ordered boolean list of specified length and share of ones

        Args:
            length: integer that specifies the number of elements in the list
            share_1s: float that specifies the share of ones in the list with a value between 0 and 1

        Returns:
            list_bool: list with boolean values

        """

        list_bool = [0] * length
        list_bool[:round(length * share_1s)] = [1] * round(length * share_1s)
        shuffle(list_bool)

        return list_bool

    @staticmethod
    def __gen_dep_bool_list(list_bool: list, share_1s: float) -> list:
        """generates an ordered boolean list with a specified share of ones that depends on another boolean list

        Comment:
            The new list depends on the provided list in that ones can only be created in the positions where the
            provided list has ones. The absolute number of ones therefore depends on the number of ones in the original
            list. Example: list_bool has length 10 and 8 ones. share_1s = 0.5. The dependent list will have 4 ones.
            If list_bool has length 10 and 4 ones, the dependent list will have 2 ones if share_1s = 0.5.

        Args:
            list_bool: list of boolean values
            share_1s: float that specifies the share of ones in the list in relation to the share of ones in list_bool
                      with a value between 0 and 1.

        Returns:
            list_dep: list of boolean values

        """

        list_dep = list_bool.copy()
        n_1s = round(sum(list_dep) * share_1s)

        # Reduce the number of ones until n_1s is reached. A while-loop was used as it is the fastest method for n<1000
        while sum(list_dep) > n_1s:
            idx = randint(0, len(list_dep) - 1)
            list_dep[idx] = 0

        return list_dep

    @staticmethod
    def __gen_distr_list(num: int, distr: list) -> list:
        """generates a sorted list according to the provided distribution with its total equal to num

        Comment:
            If the sum of the list does not match up with num, the list will be adjusted according to the provided
            probability to ensure that the sum of the list and num match up.

        Args:
            num: integer specifying the sum of the new list (represents the number of participants)
            distr: list that contains the distribution pattern for the new list

        Returns:
            list_distr: list according to the provided distribution pattern and the sum equal to num

        """

        # Generate list according to distribution pattern
        list_distr = [round(x / sum(distr) * num) for x in distr]

        # Adjust list_distr if sum does not match num
        while sum(list_distr) != num:
            idx = choices(range(len(distr)), distr)[0]
            if sum(list_distr) > num and list_distr[idx] > 0:  # subtract one element by one
                list_distr[idx] -= 1
            elif sum(list_distr) < num:  # add one element by one
                list_distr[idx] += 1

        return list_distr

    @staticmethod
    def __gen_rand_id(length: int) -> str:
        """generates a random combination of ascii characters and digits of specified length

        Args:
            length: integer specifying the length of the string

        Returns:
            string with length equal to input argument length

        """

        characters = string.ascii_lowercase + string.digits * 3

        return ''.join(choice(characters) for _ in range(length))
