from lemlab import Scenario

# in this code example, we create a lemlab scenario from the demo configuration file

if __name__ == "__main__":
    sim_name = "test_sim"

    scenario = Scenario()
    scenario.new_scenario(path_specification="sim_0_config.yaml",
                          scenario_name=f"{sim_name}")
