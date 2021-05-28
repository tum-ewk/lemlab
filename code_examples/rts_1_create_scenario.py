import lemlab


if __name__ == "__main__":
    sim_name = "test_rts"

    scenario = lemlab.Scenario()
    scenario.new_scenario(path_specification="rts_0_config.yaml",
                          scenario_name=f"{sim_name}")
