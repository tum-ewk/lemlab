import os
import shutil
import json

# in this example, we create an edited scenario from the original
# by making a copy and deactivating all electric vehicles

if __name__ == "__main__":

    sim_name_old = "test_sim"
    sim_name_new = "test_sim_no_ev"

    # copy/paste original scenario and give it a new name
    shutil.copytree(src=f"../scenarios/{sim_name_old}/",
                    dst=f"../scenarios/{sim_name_new}/")

    # loop through all prosumers in the new scenario
    for prosumer in os.listdir(f"../scenarios/{sim_name_new}/prosumer"):

        # load plant config file
        with open(f"../scenarios/{sim_name_new}/prosumer/{prosumer}/config_plants.json") as read_file:
            config = json.load(read_file)

        # check for EVs
        for plant in config:
            # if ev found, deactivate it
            if config[plant]["type"] == "ev":
                config[plant]["activated"] = False

        # save edited plant config file
        with open(f"../scenarios/{sim_name_new}/prosumer/{prosumer}/config_plants.json", 'w+') as write_file:
            json.dump(config, write_file)

    # the new scenario can now be executed normally
