import lemlab


if __name__ == "__main__":
    sim_name = "test_sim"

    scenario = lemlab.Scenario()
    scenario.new_scenario(path_specification="sim_0_config.yaml",
                          scenario_name=f"{sim_name}")
