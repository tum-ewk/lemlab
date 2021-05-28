import lemlab
from ruamel.yaml import YAML

if __name__ == "__main__":

    sim_name = "test_rts"

    # create new config file from which to edit scenario
    with open(f"../scenarios/{sim_name}/config.yaml") as config_file:
        config = YAML().load(config_file)
    config["aggregator"]["active"] = True
    with open(f"../scenarios/{sim_name}/config_edited.yaml", 'w+') as file:
        YAML().dump(config, file)

    # generate new scenario from edited config
    scenario = lemlab.Scenario()
    scenario.edit_scenario(path_new_config=f"../scenarios/{sim_name}/config_edited.yaml",
                           name_new_scenario="test_rts_with_agg")
